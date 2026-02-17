#!/usr/bin/env python3
"""Async validator orchestration framework for naming pipeline.

V1 scope:
- create/run validation jobs per candidate + check type
- persist job lifecycle states: pending -> running -> success/fail
- retry with backoff
- persist validation results and run summary

This phase focuses on framework + deterministic checks. External adapters can
be plugged in later.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

import name_generator as ng
import naming_db as ndb


@dataclass
class CandidateRow:
    candidate_id: int
    name_display: str
    state: str


@dataclass
class ValidationJobSpec:
    job_id: int
    run_id: int
    candidate_id: int
    candidate_name: str
    candidate_prev_state: str
    check_type: str


@dataclass
class ProgressState:
    total_jobs: int
    started_at_monotonic: float
    completed_jobs: int = 0
    success_jobs: int = 0
    failed_jobs: int = 0
    last_report_monotonic: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Async validator orchestration for naming pipeline.')
    parser.add_argument('--db', default='docs/branding/naming_pipeline.db', help='SQLite DB path.')
    parser.add_argument('--candidate-limit', type=int, default=100, help='Max candidates to validate in this run.')
    parser.add_argument('--concurrency', type=int, default=12, help='Max concurrent jobs.')
    parser.add_argument('--max-retries', type=int, default=2, help='Retry attempts per job.')
    parser.add_argument('--retry-backoff-ms', type=int, default=300, help='Base retry backoff (ms).')
    parser.add_argument('--timeout-s', type=float, default=8.0, help='Per-check timeout seconds.')
    parser.add_argument(
        '--checks',
        default='adversarial,psych,descriptive',
        help='Comma-separated check types to execute.',
    )
    parser.add_argument(
        '--state-filter',
        default='new,checked',
        help='Comma-separated candidate states eligible for validation.',
    )
    parser.add_argument('--scope', choices=['dach', 'eu', 'global'], default='global')
    parser.add_argument('--gate', choices=['strict', 'balanced'], default='balanced')

    parser.add_argument('--adversarial-fail-threshold', type=int, default=82)
    parser.add_argument('--adversarial-warn-threshold', type=int, default=68)
    parser.add_argument('--min-trust-proxy', type=int, default=50)
    parser.add_argument('--warn-trust-proxy', type=int, default=62)
    parser.add_argument('--max-spelling-risk', type=int, default=28)
    parser.add_argument('--warn-spelling-risk', type=int, default=16)
    parser.add_argument('--descriptive-fail-threshold', type=int, default=72)
    parser.add_argument('--descriptive-warn-threshold', type=int, default=52)
    parser.add_argument('--web-top', type=int, default=8)
    parser.add_argument('--web-near-fail-threshold', type=int, default=2)
    parser.add_argument('--store-countries', default='de,ch,us')
    parser.add_argument('--social-unavailable-fail-threshold', type=int, default=3)
    parser.add_argument('--strict-required-domains', action='store_true')
    parser.add_argument('--progress', dest='progress', action='store_true', default=True)
    parser.add_argument('--no-progress', dest='progress', action='store_false')
    parser.add_argument(
        '--progress-every',
        type=int,
        default=20,
        help='Emit progress after every N completed jobs.',
    )
    parser.add_argument(
        '--progress-interval-s',
        type=float,
        default=10.0,
        help='Emit progress when this many seconds elapsed since last report.',
    )
    return parser.parse_args()


def parse_csv_set(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(',') if part.strip()]


def load_candidates(conn: sqlite3.Connection, states: list[str], limit: int) -> list[CandidateRow]:
    if not states:
        return []
    placeholders = ','.join('?' for _ in states)
    rows = conn.execute(
        f"""
        SELECT id, name_display, state
        FROM candidates
        WHERE state IN ({placeholders})
        ORDER BY id ASC
        LIMIT ?
        """,
        (*states, limit),
    ).fetchall()
    return [CandidateRow(candidate_id=int(row[0]), name_display=str(row[1]), state=str(row[2])) for row in rows]


def check_adversarial(name: str, args: argparse.Namespace) -> dict:
    normalized = ng.normalize_alpha(name)
    risk, hits = ng.adversarial_similarity_signal(normalized)
    if risk >= args.adversarial_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -20.0,
            'reason': 'adversarial_confusion_risk',
            'evidence': {'risk': risk, 'hits': hits},
        }
    if risk >= args.adversarial_warn_threshold:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -8.0,
            'reason': 'adversarial_similarity_warning',
            'evidence': {'risk': risk, 'hits': hits},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'risk': risk, 'hits': hits},
    }


def check_psych(name: str, args: argparse.Namespace) -> dict:
    normalized = ng.normalize_alpha(name)
    spelling_risk = ng.psych_spelling_risk(normalized)
    trust_proxy = ng.psych_trust_proxy_score(normalized)

    if trust_proxy < args.min_trust_proxy or spelling_risk > args.max_spelling_risk:
        return {
            'status': 'fail',
            'hard_fail': False,
            'score_delta': -10.0,
            'reason': 'psych_quality_fail',
            'evidence': {'spelling_risk': spelling_risk, 'trust_proxy': trust_proxy},
        }
    if trust_proxy < args.warn_trust_proxy or spelling_risk > args.warn_spelling_risk:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -4.0,
            'reason': 'psych_quality_warn',
            'evidence': {'spelling_risk': spelling_risk, 'trust_proxy': trust_proxy},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'spelling_risk': spelling_risk, 'trust_proxy': trust_proxy},
    }


def check_descriptive(name: str, args: argparse.Namespace) -> dict:
    normalized = ng.normalize_alpha(name)
    risk = ng.descriptive_risk(normalized)
    if risk >= args.descriptive_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -15.0,
            'reason': 'descriptive_risk_fail',
            'evidence': {'descriptive_risk': risk},
        }
    if risk >= args.descriptive_warn_threshold:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -6.0,
            'reason': 'descriptive_risk_warn',
            'evidence': {'descriptive_risk': risk},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'descriptive_risk': risk},
    }


def normalized_or_fail(name: str) -> str:
    normalized = ng.normalize_alpha(name)
    if not normalized:
        raise ValueError(f'Invalid candidate name for check: {name!r}')
    return normalized


def check_domain(name: str, args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    com = ng.rdap_available(normalized, 'com')
    de = ng.rdap_available(normalized, 'de')
    ch = ng.rdap_available(normalized, 'ch')
    required = ng.required_tlds(args.scope)
    availability = {'com': com, 'de': de, 'ch': ch}

    missing = [tld for tld in required if availability.get(tld) == 'no']
    unknown = [tld for tld in required if availability.get(tld) == 'unknown']
    if missing:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -18.0,
            'reason': f'domain_unavailable_{"-".join(missing)}',
            'evidence': {'required': required, 'availability': availability},
        }
    if unknown:
        if args.strict_required_domains:
            return {
                'status': 'fail',
                'hard_fail': True,
                'score_delta': -10.0,
                'reason': f'domain_unknown_{"-".join(unknown)}',
                'evidence': {'required': required, 'availability': availability},
            }
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -4.0,
            'reason': f'domain_unknown_{"-".join(unknown)}',
            'evidence': {'required': required, 'availability': availability},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'required': required, 'availability': availability},
    }


def check_web(name: str, args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    exact_hits, near_hits, result_count, sample_domains, ok, source = ng.web_collision_signal(normalized, args.web_top)
    if not ok or result_count < 0:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -2.0,
            'reason': 'web_check_unknown',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    if exact_hits > 0:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -20.0,
            'reason': 'web_exact_collision',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    if near_hits >= args.web_near_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': False,
            'score_delta': -10.0,
            'reason': 'web_near_collision',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    if near_hits > 0:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -4.0,
            'reason': 'web_near_warning',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {
            'exact_hits': exact_hits,
            'near_hits': near_hits,
            'result_count': result_count,
            'sample_domains': sample_domains,
            'source': source,
        },
    }


def check_app_store(name: str, args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    countries = [c.strip().lower() for c in args.store_countries.split(',') if c.strip()]
    exact: list[str] = []
    unknown: list[str] = []
    counts: dict[str, int] = {}
    for country in countries:
        count, is_exact, ok = ng.app_store_signal(normalized, country)
        counts[country] = count
        if is_exact:
            exact.append(country)
        if not ok:
            unknown.append(country)
    if exact:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -18.0,
            'reason': f'app_store_exact_collision_{"-".join(exact)}',
            'evidence': {'countries': countries, 'counts': counts, 'exact': exact, 'unknown': unknown},
        }
    if unknown:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -3.0,
            'reason': f'app_store_unknown_{"-".join(unknown)}',
            'evidence': {'countries': countries, 'counts': counts, 'exact': exact, 'unknown': unknown},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'countries': countries, 'counts': counts, 'exact': exact, 'unknown': unknown},
    }


def check_package(name: str, _args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    pypi = ng.package_exists_on_pypi(normalized)
    npm = ng.package_exists_on_npm(normalized)
    collisions = [label for label, value in (('pypi', pypi), ('npm', npm)) if value == 'yes']
    unknown = [label for label, value in (('pypi', pypi), ('npm', npm)) if value == 'unknown']

    if collisions:
        return {
            'status': 'fail',
            'hard_fail': False,
            'score_delta': -10.0,
            'reason': f'package_collision_{"-".join(collisions)}',
            'evidence': {'pypi': pypi, 'npm': npm},
        }
    if unknown:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -2.0,
            'reason': f'package_unknown_{"-".join(unknown)}',
            'evidence': {'pypi': pypi, 'npm': npm},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'pypi': pypi, 'npm': npm},
    }


def check_social(name: str, args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    github, linkedin, x_handle, instagram, unavailable_count, unknown_count = ng.social_handle_signal(normalized)
    if unavailable_count >= args.social_unavailable_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': False,
            'score_delta': -8.0,
            'reason': 'social_handle_crowded',
            'evidence': {
                'github': github,
                'linkedin': linkedin,
                'x': x_handle,
                'instagram': instagram,
                'unavailable_count': unavailable_count,
                'unknown_count': unknown_count,
            },
        }
    if unknown_count > 0:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -2.0,
            'reason': 'social_check_unknown',
            'evidence': {
                'github': github,
                'linkedin': linkedin,
                'x': x_handle,
                'instagram': instagram,
                'unavailable_count': unavailable_count,
                'unknown_count': unknown_count,
            },
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {
            'github': github,
            'linkedin': linkedin,
            'x': x_handle,
            'instagram': instagram,
            'unavailable_count': unavailable_count,
            'unknown_count': unknown_count,
        },
    }


CHECK_RUNNERS: dict[str, Callable[[str, argparse.Namespace], dict]] = {
    'adversarial': check_adversarial,
    'psych': check_psych,
    'descriptive': check_descriptive,
    'domain': check_domain,
    'web': check_web,
    'app_store': check_app_store,
    'package': check_package,
    'social': check_social,
}


async def run_single_job(
    *,
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    spec: ValidationJobSpec,
    runner: Callable[[str, argparse.Namespace], dict],
    db_lock: asyncio.Lock,
    semaphore: asyncio.Semaphore,
    on_complete: Callable[[str], Awaitable[None]] | None = None,
) -> None:
    async with semaphore:
        attempt = 0
        started_at = ndb.now_iso()
        while True:
            attempt += 1
            async with db_lock:
                ndb.update_validation_job(
                    conn,
                    job_id=spec.job_id,
                    status='running',
                    attempt_count=attempt,
                    started_at=started_at,
                    finished_at=None,
                    last_error='',
                )
                conn.commit()

            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(runner, spec.candidate_name, args),
                    timeout=args.timeout_s,
                )
                status = str(result['status'])
                hard_fail = bool(result['hard_fail'])
                score_delta = float(result['score_delta'])
                reason = str(result['reason'])
                evidence = dict(result['evidence'])

                async with db_lock:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=spec.candidate_id,
                        run_id=spec.run_id,
                        check_type=spec.check_type,
                        status=status,
                        score_delta=score_delta,
                        hard_fail=hard_fail,
                        reason=reason,
                        evidence=evidence,
                    )
                    ndb.update_validation_job(
                        conn,
                        job_id=spec.job_id,
                        status='success',
                        attempt_count=attempt,
                        started_at=started_at,
                        finished_at=ndb.now_iso(),
                        last_error='',
                    )
                    conn.commit()
                if on_complete is not None:
                    await on_complete('success')
                return
            except Exception as exc:  # noqa: BLE001
                err = f'{type(exc).__name__}: {exc}'
                should_retry = attempt <= args.max_retries
                if should_retry:
                    async with db_lock:
                        ndb.update_validation_job(
                            conn,
                            job_id=spec.job_id,
                            status='pending',
                            attempt_count=attempt,
                            started_at=started_at,
                            finished_at=None,
                            last_error=err,
                        )
                        conn.commit()
                    await asyncio.sleep((args.retry_backoff_ms / 1000.0) * attempt)
                    continue

                async with db_lock:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=spec.candidate_id,
                        run_id=spec.run_id,
                        check_type=spec.check_type,
                        status='error',
                        score_delta=-5.0,
                        hard_fail=False,
                        reason='validator_execution_error',
                        evidence={'error': err},
                    )
                    ndb.update_validation_job(
                        conn,
                        job_id=spec.job_id,
                        status='fail',
                        attempt_count=attempt,
                        started_at=started_at,
                        finished_at=ndb.now_iso(),
                        last_error=err,
                    )
                    conn.commit()
                if on_complete is not None:
                    await on_complete('fail')
                return


def summarize_run(conn: sqlite3.Connection, run_id: int) -> dict:
    rows = conn.execute(
        """
        SELECT status, COUNT(*)
        FROM validation_jobs
        WHERE run_id = ?
        GROUP BY status
        ORDER BY status
        """,
        (run_id,),
    ).fetchall()
    summary = {'total_jobs': 0, 'status_counts': {}}
    for status, count in rows:
        summary['status_counts'][str(status)] = int(count)
        summary['total_jobs'] += int(count)
    return summary


def mark_candidates_checked(conn: sqlite3.Connection, rows: list[CandidateRow], actor: str) -> None:
    ts = ndb.now_iso()
    for row in rows:
        if row.state == 'checked':
            continue
        conn.execute(
            """
            UPDATE candidates
            SET state = ?, state_updated_at = ?
            WHERE id = ?
            """,
            ('checked', ts, row.candidate_id),
        )
        conn.execute(
            """
            INSERT INTO state_transitions(candidate_id, from_state, to_state, actor, note, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (row.candidate_id, row.state, 'checked', actor, 'async validation completed', ts),
        )


async def orchestrate(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    checks = parse_csv_set(args.checks)
    unknown = [check for check in checks if check not in CHECK_RUNNERS]
    if unknown:
        print(f'Unknown checks: {", ".join(unknown)}')
        return 1

    states = parse_csv_set(args.state_filter)

    with sqlite3.connect(db_path) as conn:
        ndb.ensure_schema(conn)
        candidates = load_candidates(conn, states, args.candidate_limit)
        if not candidates:
            print('No candidates found for selected state filter.')
            return 0

        run_id = ndb.create_run(
            conn,
            source_path=str(db_path),
            scope=args.scope,
            gate_mode=args.gate,
            variation_profile='validator_async',
            status='running',
            config={
                'checks': checks,
                'candidate_limit': args.candidate_limit,
                'concurrency': args.concurrency,
                'max_retries': args.max_retries,
                'state_filter': states,
            },
            summary={},
        )
        conn.commit()

        jobs: list[ValidationJobSpec] = []
        for row in candidates:
            for check_type in checks:
                job_id = ndb.create_validation_job(
                    conn,
                    run_id=run_id,
                    candidate_id=row.candidate_id,
                    check_type=check_type,
                    status='pending',
                )
                jobs.append(
                    ValidationJobSpec(
                        job_id=job_id,
                        run_id=run_id,
                        candidate_id=row.candidate_id,
                        candidate_name=row.name_display,
                        candidate_prev_state=row.state,
                        check_type=check_type,
                    )
                )
        conn.commit()

        db_lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(max(1, args.concurrency))
        progress_lock = asyncio.Lock()
        progress_state = ProgressState(
            total_jobs=len(jobs),
            started_at_monotonic=time.monotonic(),
            last_report_monotonic=time.monotonic(),
        )

        def format_progress_line(*, force: bool = False) -> str:
            elapsed = max(0.001, time.monotonic() - progress_state.started_at_monotonic)
            rate = progress_state.completed_jobs / elapsed
            remaining = max(0, progress_state.total_jobs - progress_state.completed_jobs)
            eta_seconds = remaining / max(rate, 0.001)
            percent = (progress_state.completed_jobs / max(1, progress_state.total_jobs)) * 100.0
            label = 'progress_final' if force else 'progress'
            return (
                f'async_validation_{label} completed={progress_state.completed_jobs}/{progress_state.total_jobs} '
                f'({percent:.1f}%) success={progress_state.success_jobs} fail={progress_state.failed_jobs} '
                f'rate={rate:.2f}jobs/s eta={eta_seconds:.1f}s'
            )

        async def on_job_complete(outcome: str) -> None:
            if not args.progress:
                return
            async with progress_lock:
                progress_state.completed_jobs += 1
                if outcome == 'success':
                    progress_state.success_jobs += 1
                else:
                    progress_state.failed_jobs += 1

                now = time.monotonic()
                due_by_count = progress_state.completed_jobs % max(1, args.progress_every) == 0
                due_by_time = (now - progress_state.last_report_monotonic) >= max(0.1, args.progress_interval_s)
                first_job = progress_state.completed_jobs == 1
                last_job = progress_state.completed_jobs >= progress_state.total_jobs
                if first_job or last_job or due_by_count or due_by_time:
                    print(format_progress_line(force=last_job), flush=True)
                    progress_state.last_report_monotonic = now

        if args.progress:
            print(
                f'async_validation_start run_id={run_id} candidates={len(candidates)} '
                f'jobs={len(jobs)} checks={",".join(checks)}',
                flush=True,
            )

        tasks = []
        for job in jobs:
            runner = CHECK_RUNNERS[job.check_type]
            tasks.append(
                asyncio.create_task(
                    run_single_job(
                        conn=conn,
                        args=args,
                        spec=job,
                        runner=runner,
                        db_lock=db_lock,
                        semaphore=semaphore,
                        on_complete=on_job_complete,
                    )
                )
            )
        await asyncio.gather(*tasks)

        mark_candidates_checked(conn, candidates, actor='naming_validate_async')
        summary = summarize_run(conn, run_id)
        conn.execute(
            """
            UPDATE naming_runs
            SET status = ?, summary_json = ?
            WHERE id = ?
            """,
            ('completed', json.dumps(summary, ensure_ascii=False), run_id),
        )
        conn.commit()

    print(
        f'async_validation_complete run_id={run_id} candidates={len(candidates)} '
        f'jobs={len(jobs)} db={db_path}'
    )
    print(f'run_summary={json.dumps(summary, ensure_ascii=False)}')
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(orchestrate(args))


if __name__ == '__main__':
    raise SystemExit(main())
