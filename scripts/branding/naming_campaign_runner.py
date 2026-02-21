#!/usr/bin/env python3
"""Long-running naming campaign runner with optional active LLM ideation.

Execution order per run:
1) (optional) active LLM ideation stage -> artifact for --llm-input
2) v3 generator run
3) async validator run
4) contract assertion + novelty tracking + reporting
"""

from __future__ import annotations

import atexit
import argparse
import csv
import datetime as dt
import json
import math
import os
import random
import shlex
import shutil
import socket
import statistics
import subprocess
import time
from collections import deque
from pathlib import Path
from typing import Any

import naming_ideation_stage as nide


TRUTHY_VALUES = {'1', 'true', 'yes', 'y'}
DEFAULT_GENERATOR_FAMILIES = ['coined', 'stem', 'suggestive', 'morphology', 'seed', 'expression', 'source_pool', 'blend']


def parse_csv_list(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(',') if part.strip()]


def clamp_share(value: float) -> float:
    return max(0.0, min(1.0, value))


def is_truthy(raw: str) -> bool:
    return raw.strip().lower() in TRUTHY_VALUES


def run_cmd(cmd: list[str], *, cwd: Path, log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('w', encoding='utf-8') as handle:
        handle.write(f'$ {" ".join(shlex.quote(part) for part in cmd)}\n\n')
        handle.flush()
        proc = subprocess.run(cmd, cwd=str(cwd), stdout=handle, stderr=subprocess.STDOUT, check=False)
    return int(proc.returncode)


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_lock_payload(lock_path: Path) -> dict[str, object]:
    try:
        payload = json.loads(lock_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def acquire_campaign_lock(*, out_dir: Path, shard_id: int, shard_count: int) -> tuple[Path | None, str]:
    lock_path = out_dir / f'.campaign_lock_shard_{shard_id}.json'
    payload = {
        'pid': os.getpid(),
        'host': socket.gethostname(),
        'created_at': dt.datetime.now().isoformat(timespec='seconds'),
        'shard_id': int(shard_id),
        'shard_count': int(shard_count),
    }
    for _ in range(2):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            existing = read_lock_payload(lock_path)
            existing_pid = int(existing.get('pid') or 0)
            existing_host = str(existing.get('host') or 'unknown')
            existing_created = str(existing.get('created_at') or 'unknown')
            if existing_pid > 0 and pid_is_alive(existing_pid):
                return None, (
                    f'active_pid={existing_pid} host={existing_host} '
                    f'created_at={existing_created}'
                )
            try:
                lock_path.unlink()
            except FileNotFoundError:
                continue
            except OSError as exc:
                return None, f'stale_lock_remove_failed={exc}'
            continue
        except OSError as exc:
            return None, f'lock_open_failed={exc}'
        else:
            with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                json.dump(payload, handle, ensure_ascii=False)
                handle.write('\n')
            return lock_path, ''
    return None, 'lock_acquire_failed_after_retries'


def release_campaign_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        return


def extract_run_summary(validator_log: Path) -> dict[str, object]:
    if not validator_log.exists():
        return {}
    payload: dict[str, object] = {}
    for raw in validator_log.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line.startswith('run_summary='):
            continue
        blob = line[len('run_summary=') :]
        try:
            value = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            payload = value
    return payload


def load_shortlist_names(csv_path: Path) -> list[str]:
    if not csv_path.exists():
        return []
    out: list[str] = []
    with csv_path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            name = str(row.get('name') or '').strip()
            if not name:
                continue
            if is_truthy(str(row.get('shortlist_selected') or '')):
                out.append(name)
    return out


def append_progress_row(path: Path, row: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    headers = [
        'run',
        'arm',
        'llm_active',
        'llm_provider',
        'llm_model',
        'llm_candidate_count',
        'llm_cost_usd',
        'llm_stage_status',
        'shard_id',
        'shard_count',
        'shard_combo_count',
        'timestamp',
        'scope',
        'gate',
        'source_influence_share',
        'quota_profile',
        'quota_profile_effective',
        'shortlist_count',
        'new_shortlist_count',
        'hard_fail_ratio',
        'cumulative_unique_shortlist',
        'validator_total_jobs',
        'validator_status_counts',
        'validator_tier_result_counts',
        'status',
        'duration_s',
    ]
    with path.open('a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        if write_header:
            writer.writeheader()
        writer.writerow({key: row.get(key, '') for key in headers})


def validate_quota_profile(*, active_families: list[str], quota_profile: str) -> tuple[bool, str]:
    quotas = nide.parse_family_quotas(quota_profile)
    if not quotas:
        return False, 'invalid quota profile (empty parse)'
    missing = [family for family in active_families if family not in quotas]
    extra = [family for family in quotas.keys() if family not in active_families]
    if missing or extra:
        return False, f'family/quota mismatch missing={missing} extra={extra}'
    return True, 'ok'


def load_cached_candidates(*, cache_dir: Path, cache_key: str, ttl_days: int) -> list[str] | None:
    path = cache_dir / f'{cache_key}.json'
    if not path.exists():
        return None
    ttl_seconds = max(0, int(ttl_days)) * 86400
    if ttl_seconds > 0:
        age = time.time() - path.stat().st_mtime
        if age > ttl_seconds:
            return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    source = payload.get('candidates')
    if not isinstance(source, list):
        return None
    out: list[str] = []
    for item in source:
        if isinstance(item, str):
            name = nide.normalize_alpha_name(item)
            if 5 <= len(name) <= 12:
                out.append(name)
    return sorted(set(out))


def store_cached_candidates(*, cache_dir: Path, cache_key: str, candidates: list[str], meta: dict[str, Any]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f'{cache_key}.json'
    payload = {
        'cached_at': dt.datetime.now().isoformat(timespec='seconds'),
        'candidates': sorted(set(candidates)),
        'meta': meta,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def run_active_llm_ideation(
    *,
    args: argparse.Namespace,
    runs_dir: Path,
    logs_dir: Path,
    run_id: str,
    run_index: int,
    scope: str,
    seen_shortlist: set[str],
    context_packet: dict[str, Any],
) -> tuple[Path | None, dict[str, Any]]:
    context_hash = nide.constraints_hash({'context': context_packet}) if context_packet else ''
    report: dict[str, Any] = {
        'enabled': bool(args.llm_ideation_enabled),
        'provider': args.llm_provider,
        'model': args.llm_model,
        'status': 'disabled',
        'candidate_count': 0,
        'cost_usd': 0.0,
        'constraints_path': '',
        'artifact_path': '',
        'retries': 0,
        'cache_hits': 0,
        'errors': [],
        'context_enabled': bool(context_packet),
        'context_hash': context_hash,
    }
    if not args.llm_ideation_enabled:
        return None, report

    constraints = nide.compute_dynamic_constraints(
        runs_dir=runs_dir,
        seen_shortlist=seen_shortlist,
        window_runs=max(1, args.dynamic_window_runs),
        fail_threshold=max(0.0, min(1.0, args.dynamic_fail_threshold)),
        entropy_threshold=max(0.0, float(args.dynamic_prefix_entropy_threshold)),
        max_token_ban=max(1, args.dynamic_max_token_ban),
        max_prefix_ban=max(1, args.dynamic_max_prefix_ban),
    )
    constraints_path = runs_dir / f'{run_id}_dynamic_constraints.json'
    constraints_path.write_text(json.dumps(constraints, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    report['constraints_path'] = str(constraints_path)

    target_total = max(1, int(args.llm_rounds)) * max(1, int(args.llm_candidates_per_round))
    names: list[str] = []
    names_seen: set[str] = set()
    stage_started = time.monotonic()
    total_cost = 0.0
    last_call_cost = 0.0
    llm_log = logs_dir / f'{run_id}_llm.log'
    cache_dir = Path(args.llm_cache_dir).expanduser() if args.llm_cache_dir else Path()

    def append_log(line: str) -> None:
        llm_log.parent.mkdir(parents=True, exist_ok=True)
        with llm_log.open('a', encoding='utf-8') as handle:
            handle.write(line.rstrip() + '\n')

    if args.llm_provider == 'fixture':
        fixture_names = nide.load_fixture_candidates(args.llm_fixture_input)
        names = fixture_names[:target_total]
        report['status'] = 'ok_fixture'
    elif args.llm_provider == 'pal':
        fixture_names = nide.load_fixture_candidates(args.llm_fixture_input)
        if fixture_names:
            names = fixture_names[:target_total]
            report['status'] = 'ok_pal_fixture'
        else:
            report['status'] = 'pal_unavailable_without_fixture'
            report['errors'].append('pal mode requires fixture input in CLI runner')
    else:
        api_key = os.environ.get(args.llm_api_key_env, '').strip()
        if not api_key:
            report['status'] = 'missing_api_key'
            report['errors'].append(f'missing env {args.llm_api_key_env}')
        else:
            models = nide.list_openrouter_models(
                api_key=api_key,
                timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
            )
            if models is not None and args.llm_model not in models:
                report['status'] = 'model_not_in_catalog'
                report['errors'].append(f'model={args.llm_model}')
            else:
                report['status'] = 'running'
                for round_idx in range(max(1, int(args.llm_rounds))):
                    elapsed_ms = int((time.monotonic() - stage_started) * 1000)
                    if elapsed_ms >= max(1000, int(args.llm_stage_timeout_ms)):
                        report['status'] = 'stage_timeout'
                        break
                    if args.llm_max_usd_per_run > 0:
                        projected = total_cost + max(last_call_cost, 0.0)
                        if projected > float(args.llm_max_usd_per_run):
                            report['status'] = 'budget_stop'
                            break

                    prompt, mode = nide.build_prompt(
                        scope=scope,
                        round_index=((run_index - 1) * max(1, int(args.llm_rounds))) + round_idx,
                        target_count=max(1, int(args.llm_candidates_per_round)),
                        constraints=constraints,
                        context_packet=context_packet,
                    )
                    mode_key = ':'.join(mode)
                    cache_key_blob = json.dumps(
                        {
                            'model': args.llm_model,
                            'prompt': prompt,
                            'schema_version': 'llm_candidates_v1',
                            'constraints_hash': nide.constraints_hash(constraints),
                            'mode_key': mode_key,
                        },
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                    cache_key = nide.constraints_hash({'blob': cache_key_blob})

                    round_names: list[str] = []
                    cached = None
                    if args.llm_cache_dir:
                        cached = load_cached_candidates(
                            cache_dir=cache_dir,
                            cache_key=cache_key,
                            ttl_days=max(0, int(args.llm_cache_ttl_days)),
                        )
                    if cached is not None:
                        round_names = cached
                        report['cache_hits'] = int(report['cache_hits']) + 1
                        append_log(f'round={round_idx + 1} mode={mode_key} source=cache names={len(round_names)}')
                    else:
                        retries = max(0, int(args.llm_max_retries))
                        usage: dict[str, Any] = {}
                        err = 'unknown'
                        for attempt in range(retries + 1):
                            call_names, usage, err = nide.call_openrouter_candidates(
                                api_key=api_key,
                                model=args.llm_model,
                                prompt=prompt,
                                timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
                                strict_json=bool(args.llm_strict_json),
                            )
                            if call_names:
                                round_names = call_names
                                call_cost = nide.estimate_usage_cost_usd(
                                    usage=usage,
                                    in_price_per_1k=max(0.0, float(args.llm_pricing_input_per_1k)),
                                    out_price_per_1k=max(0.0, float(args.llm_pricing_output_per_1k)),
                                )
                                total_cost += call_cost
                                last_call_cost = call_cost
                                append_log(
                                    f'round={round_idx + 1} mode={mode_key} source=openrouter '
                                    f'names={len(round_names)} usage={usage} cost_usd={call_cost:.6f}'
                                )
                                if args.llm_cache_dir and round_names:
                                    store_cached_candidates(
                                        cache_dir=cache_dir,
                                        cache_key=cache_key,
                                        candidates=round_names,
                                        meta={'usage': usage, 'mode': mode},
                                    )
                                break
                            retriable = err in {'timeout', 'network_error', 'http_429', 'http_500', 'http_502', 'http_503'}
                            if retriable and attempt < retries:
                                report['retries'] = int(report['retries']) + 1
                                sleep_s = (0.35 * (attempt + 1)) + random.uniform(0.0, 0.25)
                                append_log(f'round={round_idx + 1} retry={attempt + 1} err={err} sleep_s={sleep_s:.2f}')
                                time.sleep(sleep_s)
                                continue
                            report['errors'].append(f'round={round_idx + 1}:{err}')
                            append_log(f'round={round_idx + 1} err={err}')
                            break

                    for name in round_names:
                        normalized = nide.normalize_alpha_name(name)
                        if not (5 <= len(normalized) <= 12):
                            continue
                        if normalized in names_seen:
                            continue
                        names_seen.add(normalized)
                        names.append(normalized)
                    if len(names) >= target_total:
                        break

                if report['status'] == 'running':
                    if names:
                        report['status'] = 'ok'
                    elif report['errors']:
                        report['status'] = 'empty_with_errors'
                    else:
                        report['status'] = 'empty'

    names = names[:target_total]
    report['candidate_count'] = len(names)
    report['cost_usd'] = round(total_cost, 6)
    if not names:
        return None, report

    artifact_path = runs_dir / f'{run_id}_llm_candidates.json'
    artifact_payload = {
        'candidates': [{'name': item} for item in names],
        'metadata': {
            'provider': args.llm_provider,
            'model': args.llm_model,
            'run_id': run_id,
            'constraints_path': str(constraints_path),
            'status': report['status'],
            'cost_usd': report['cost_usd'],
            'context_hash': context_hash,
        },
    }
    artifact_path.write_text(json.dumps(artifact_payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    report['artifact_path'] = str(artifact_path)
    return artifact_path, report


def _rank_values(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    idx = 0
    while idx < len(indexed):
        j = idx
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[idx][1]:
            j += 1
        avg_rank = (idx + j + 2) / 2.0
        for k in range(idx, j + 1):
            ranks[indexed[k][0]] = avg_rank
        idx = j + 1
    return ranks


def mann_whitney_u(x: list[float], y: list[float]) -> dict[str, float]:
    if not x or not y:
        return {'u': 0.0, 'p_two_sided': 1.0, 'z': 0.0}
    combined = x + y
    ranks = _rank_values(combined)
    n1 = len(x)
    n2 = len(y)
    r1 = sum(ranks[:n1])
    u1 = r1 - (n1 * (n1 + 1) / 2.0)
    u2 = (n1 * n2) - u1
    u = min(u1, u2)

    counts: dict[float, int] = {}
    for value in combined:
        counts[value] = counts.get(value, 0) + 1
    tie_sum = sum((count**3 - count) for count in counts.values() if count > 1)
    n = n1 + n2
    base = n1 * n2 / 12.0
    tie_corr = 0.0
    if n > 1:
        tie_corr = tie_sum / (n * (n - 1))
    sigma_sq = base * ((n + 1) - tie_corr)
    sigma = math.sqrt(max(1e-12, sigma_sq))
    mu = n1 * n2 / 2.0
    cc = 0.5 if u1 > mu else -0.5
    z = (u1 - mu - cc) / sigma
    p = math.erfc(abs(z) / math.sqrt(2.0))
    return {'u': float(u), 'p_two_sided': float(p), 'z': float(z)}


def bootstrap_median_diff_ci(
    *,
    a: list[float],
    b: list[float],
    iters: int = 2000,
    seed: int = 0,
    alpha: float = 0.05,
) -> tuple[float, float]:
    if not a or not b:
        return 0.0, 0.0
    rng = random.Random(seed)
    diffs: list[float] = []
    for _ in range(max(200, iters)):
        sa = [a[rng.randrange(len(a))] for _ in range(len(a))]
        sb = [b[rng.randrange(len(b))] for _ in range(len(b))]
        diffs.append(float(statistics.median(sb) - statistics.median(sa)))
    diffs.sort()
    lo_idx = int((alpha / 2.0) * (len(diffs) - 1))
    hi_idx = int((1.0 - alpha / 2.0) * (len(diffs) - 1))
    return diffs[lo_idx], diffs[hi_idx]


def write_ab_report(*, out_dir: Path, metrics: list[dict[str, float]], seed: int) -> tuple[Path, Path] | None:
    arm_a = [row['new_shortlist_count'] for row in metrics if row.get('arm') == 'A']
    arm_b = [row['new_shortlist_count'] for row in metrics if row.get('arm') == 'B']
    if not arm_a or not arm_b:
        return None
    mw = mann_whitney_u([float(v) for v in arm_a], [float(v) for v in arm_b])
    ci_lo, ci_hi = bootstrap_median_diff_ci(
        a=[float(v) for v in arm_a],
        b=[float(v) for v in arm_b],
        seed=seed,
    )
    med_a = float(statistics.median(arm_a))
    med_b = float(statistics.median(arm_b))
    rel = ((med_b - med_a) / med_a * 100.0) if med_a > 0 else 0.0
    payload = {
        'sample_sizes': {'A': len(arm_a), 'B': len(arm_b)},
        'medians': {'A_new_shortlist': med_a, 'B_new_shortlist': med_b},
        'median_relative_change_pct': rel,
        'mann_whitney': mw,
        'bootstrap_ci_95_median_diff': {'low': ci_lo, 'high': ci_hi},
    }
    json_path = out_dir / 'ab_report.json'
    md_path = out_dir / 'ab_report.md'
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    md = (
        '# Naming Campaign A/B Report\n\n'
        f'- Samples: A={len(arm_a)} B={len(arm_b)}\n'
        f'- Median new shortlist count: A={med_a:.3f}, B={med_b:.3f}\n'
        f'- Relative median change: {rel:.2f}%\n'
        f'- Mann-Whitney U: U={mw["u"]:.3f}, z={mw["z"]:.3f}, p={mw["p_two_sided"]:.6f}\n'
        f'- Bootstrap 95% CI (median diff B-A): [{ci_lo:.3f}, {ci_hi:.3f}]\n'
    )
    md_path.write_text(md, encoding='utf-8')
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run long naming campaign sweeps with progress reporting.')
    parser.add_argument('--hours', type=float, default=8.0, help='Wall-clock runtime budget in hours.')
    parser.add_argument('--max-runs', type=int, default=48, help='Maximum number of runs.')
    parser.add_argument('--sleep-s', type=int, default=120, help='Sleep seconds between runs.')
    parser.add_argument('--max-errors', type=int, default=3, help='Abort after this many failed runs.')
    parser.add_argument('--mini-test', dest='mini_test', action='store_true', default=True)
    parser.add_argument('--no-mini-test', dest='mini_test', action='store_false')
    parser.add_argument('--pool-size', type=int, default=560, help='Generator pool size.')
    parser.add_argument('--check-limit', type=int, default=150, help='Generator external-check finalist limit.')
    parser.add_argument('--shortlist-size', type=int, default=60, help='Generator shortlist size.')
    parser.add_argument(
        '--generator-store-countries',
        default='de,ch,us,gb,fr,it',
        help='Comma-separated App Store countries passed to generator --store-countries.',
    )
    parser.add_argument(
        '--generator-families',
        default=','.join(DEFAULT_GENERATOR_FAMILIES),
        help='Comma-separated generator families.',
    )
    parser.add_argument(
        '--generator-seeds',
        default='clarity,balance,tenant,settlement,trust,ratio',
        help='Comma-separated seeds passed to generator --seeds.',
    )
    parser.add_argument(
        '--generator-no-external-checks',
        dest='generator_no_external_checks',
        action='store_true',
        default=False,
        help='Disable generator external checks (fast local sweep mode).',
    )
    parser.add_argument(
        '--generator-degraded-network-mode',
        dest='generator_degraded_network_mode',
        action='store_true',
        default=False,
        help='Pass --degraded-network-mode to generator (unknown external states become soft signals).',
    )
    parser.add_argument(
        '--generator-quality-first',
        dest='generator_quality_first',
        action='store_true',
        default=True,
        help='Enable generator quality-first gates (default on).',
    )
    parser.add_argument('--no-generator-quality-first', dest='generator_quality_first', action='store_false')
    parser.add_argument(
        '--validator-checks',
        default='adversarial,psych,descriptive,tm_cheap,domain,web,app_store,package,social',
        help='Comma-separated validator checks.',
    )
    parser.add_argument(
        '--validator-tier',
        choices=['all', 'cheap', 'expensive'],
        default='all',
        help='Validator tier setting.',
    )
    parser.add_argument(
        '--validator-candidate-limit',
        type=int,
        default=120,
        help='Validator candidate limit.',
    )
    parser.add_argument(
        '--validator-concurrency',
        type=int,
        default=10,
        help='Validator concurrency.',
    )
    parser.add_argument(
        '--validator-expensive-finalist-limit',
        type=int,
        default=30,
        help='Validator expensive finalist limit when tier includes expensive checks.',
    )
    parser.add_argument(
        '--stop-window',
        type=int,
        default=10,
        help='Early stop window size (last N runs) for novelty check.',
    )
    parser.add_argument(
        '--stop-min-new',
        type=int,
        default=5,
        help='Early stop if sum(new shortlist names) over stop window is below this threshold.',
    )
    parser.add_argument(
        '--source-influence-shares',
        default='0.15,0.25,0.35,0.50',
        help='Comma-separated source influence shares for sweep.',
    )
    parser.add_argument(
        '--scopes',
        default='global,eu,dach',
        help='Comma-separated scope sweep values.',
    )
    parser.add_argument(
        '--gates',
        default='balanced,strict',
        help='Comma-separated gate sweep values.',
    )
    parser.add_argument(
        '--quota-profiles',
        default=(
            'coined:180,stem:140,suggestive:120,morphology:200,seed:120,expression:80,source_pool:220,blend:220'
            '|coined:220,stem:170,suggestive:160,morphology:120,seed:120,expression:95,source_pool:140,blend:140'
            '|coined:140,stem:120,suggestive:120,morphology:260,seed:120,expression:80,source_pool:260,blend:220'
        ),
        help='Pipe-separated family quota profiles used per run.',
    )
    parser.add_argument(
        '--adapt-family-quotas',
        dest='adapt_family_quotas',
        action='store_true',
        default=True,
        help='Adapt family quotas from recent run fail/shortlist rates.',
    )
    parser.add_argument('--no-adapt-family-quotas', dest='adapt_family_quotas', action='store_false')
    parser.add_argument(
        '--enforce-family-quota-parity',
        dest='enforce_family_quota_parity',
        action='store_true',
        default=True,
        help='Abort if quota profile keys do not match active generator families.',
    )
    parser.add_argument('--no-enforce-family-quota-parity', dest='enforce_family_quota_parity', action='store_false')
    parser.add_argument(
        '--llm-ideation-enabled',
        dest='llm_ideation_enabled',
        action='store_true',
        default=False,
        help='Enable active LLM ideation stage that writes artifact for --llm-input.',
    )
    parser.add_argument('--no-llm-ideation-enabled', dest='llm_ideation_enabled', action='store_false')
    parser.add_argument(
        '--llm-provider',
        choices=['openrouter_http', 'pal', 'fixture'],
        default='openrouter_http',
        help='LLM provider mode for ideation stage.',
    )
    parser.add_argument(
        '--llm-model',
        default='mistralai/mistral-small-creative',
        help='LLM model identifier.',
    )
    parser.add_argument(
        '--llm-api-key-env',
        default='OPENROUTER_API_KEY',
        help='Environment variable name containing API key for openrouter_http mode.',
    )
    parser.add_argument('--llm-rounds', type=int, default=2, help='LLM ideation rounds per run.')
    parser.add_argument('--llm-candidates-per-round', type=int, default=20, help='Candidates requested per LLM round.')
    parser.add_argument('--llm-max-call-latency-ms', type=int, default=8000, help='Per-call timeout in milliseconds.')
    parser.add_argument('--llm-stage-timeout-ms', type=int, default=30000, help='Total ideation stage timeout in milliseconds.')
    parser.add_argument('--llm-max-retries', type=int, default=3, help='Max retries for retriable LLM call errors.')
    parser.add_argument('--llm-max-usd-per-run', type=float, default=0.0, help='Budget cap for LLM calls (0 disables cap).')
    parser.add_argument('--llm-pricing-input-per-1k', type=float, default=0.0, help='Estimated input token price USD per 1k.')
    parser.add_argument('--llm-pricing-output-per-1k', type=float, default=0.0, help='Estimated output token price USD per 1k.')
    parser.add_argument('--llm-cache-dir', default='', help='Optional cache directory for LLM round responses.')
    parser.add_argument('--llm-cache-ttl-days', type=int, default=7, help='Cache TTL in days.')
    parser.add_argument('--llm-fixture-input', default='', help='Fixture input file used by fixture/pal modes.')
    parser.add_argument(
        '--llm-context-file',
        default='',
        help='Optional JSON context packet injected into LLM prompt.',
    )
    parser.add_argument('--llm-strict-json', dest='llm_strict_json', action='store_true', default=True)
    parser.add_argument('--no-llm-strict-json', dest='llm_strict_json', action='store_false')
    parser.add_argument('--dynamic-window-runs', type=int, default=5, help='Window size for dynamic constraints.')
    parser.add_argument('--dynamic-fail-threshold', type=float, default=0.20, help='Fail-reason share threshold for bans.')
    parser.add_argument('--dynamic-prefix-entropy-threshold', type=float, default=2.5, help='Entropy threshold for prefix bans.')
    parser.add_argument('--dynamic-max-token-ban', type=int, default=50, help='Maximum banned token list size.')
    parser.add_argument('--dynamic-max-prefix-ban', type=int, default=30, help='Maximum banned prefix list size.')
    parser.add_argument('--ab-mode', dest='ab_mode', action='store_true', default=False, help='Enable A/B run assignment.')
    parser.add_argument('--ab-seed', type=int, default=722, help='Random seed used for A/B block randomization.')
    parser.add_argument('--out-dir', default='', help='Campaign output root (default test_outputs/branding/naming_campaign_<timestamp>).')
    parser.add_argument('--db', default='', help='SQLite DB path (default <out-dir>/naming_campaign.db).')
    parser.add_argument(
        '--include-names-txt',
        dest='include_names_txt',
        action='store_true',
        default=True,
        help='Ingest names.txt if present in repository root.',
    )
    parser.add_argument('--no-include-names-txt', dest='include_names_txt', action='store_false')
    parser.add_argument('--shard-id', type=int, default=0, help='0-based shard id for parallel campaign workers.')
    parser.add_argument('--shard-count', type=int, default=1, help='Total number of shard workers.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    stamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')

    out_dir = Path(args.out_dir).expanduser() if args.out_dir else root / 'test_outputs' / 'branding' / f'naming_campaign_{stamp}'
    db_path = Path(args.db).expanduser() if args.db else out_dir / 'naming_campaign.db'

    logs_dir = out_dir / 'logs'
    runs_dir = out_dir / 'runs'
    progress_csv = out_dir / 'campaign_progress.csv'
    seen_names_path = out_dir / 'seen_shortlist_names.txt'
    campaign_summary_path = out_dir / 'campaign_summary.json'

    out_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    shares = [clamp_share(float(part)) for part in parse_csv_list(args.source_influence_shares)]
    scopes = parse_csv_list(args.scopes)
    gates = parse_csv_list(args.gates)
    quota_profiles = [part.strip() for part in args.quota_profiles.split('|') if part.strip()]
    active_families = parse_csv_list(args.generator_families) or list(DEFAULT_GENERATOR_FAMILIES)
    if not shares or not scopes or not gates or not quota_profiles:
        print('Invalid sweep configuration: shares/scopes/gates/quota-profiles must be non-empty.')
        return 1
    if args.shard_count < 1:
        print('Invalid shard configuration: --shard-count must be >= 1.')
        return 1
    if args.shard_id < 0 or args.shard_id >= args.shard_count:
        print('Invalid shard configuration: --shard-id must be in range [0, --shard-count).')
        return 1
    llm_context_packet: dict[str, Any] = {}
    if args.llm_context_file.strip():
        try:
            llm_context_packet = nide.load_context_packet(args.llm_context_file.strip())
        except ValueError as exc:
            print(f'Invalid LLM context file: {exc}')
            return 1
    if args.enforce_family_quota_parity:
        for profile in quota_profiles:
            ok, msg = validate_quota_profile(active_families=active_families, quota_profile=profile)
            if not ok:
                print(f'Invalid quota profile "{profile}": {msg}')
                return 1

    sweep_combos: list[tuple[float, str, str, str]] = []
    for quota_profile in quota_profiles:
        for gate in gates:
            for scope in scopes:
                for share in shares:
                    sweep_combos.append((share, scope, gate, quota_profile))
    shard_combos = sweep_combos[args.shard_id :: args.shard_count]
    if not shard_combos:
        print(
            'Invalid shard configuration: no sweep combinations assigned '
            f'to shard_id={args.shard_id} with shard_count={args.shard_count}.'
        )
        return 1

    print(f'campaign_start out_dir={out_dir}')
    print(
        f'campaign_config hours={args.hours} max_runs={args.max_runs} sleep_s={args.sleep_s} '
        f'shares={shares} scopes={scopes} gates={gates} quota_profiles={len(quota_profiles)} '
        f'shard={args.shard_id + 1}/{args.shard_count} shard_combo_count={len(shard_combos)} '
        f'check_limit={args.check_limit} validator_tier={args.validator_tier} '
        f'validator_candidate_limit={args.validator_candidate_limit} '
        f'llm_enabled={args.llm_ideation_enabled} llm_provider={args.llm_provider} llm_model={args.llm_model} '
        f'llm_context_enabled={bool(llm_context_packet)}'
    )

    lock_path, lock_error = acquire_campaign_lock(
        out_dir=out_dir,
        shard_id=int(args.shard_id),
        shard_count=int(args.shard_count),
    )
    if lock_path is None:
        print(
            f'campaign_lock_blocked out_dir={out_dir} '
            f'shard={args.shard_id + 1}/{args.shard_count} reason={lock_error}'
        )
        return 2
    atexit.register(release_campaign_lock, lock_path)
    print(f'campaign_lock_acquired path={lock_path}')

    if args.mini_test:
        if args.shard_count > 1 and args.shard_id != 0:
            print('mini_test_skipped reason=shard_nonzero')
        else:
            mini_log = logs_dir / 'mini_test_smoke.log'
            code = run_cmd(
                [str(root / 'scripts' / 'branding' / 'test_naming_pipeline_v3.sh'), 'smoke'],
                cwd=root,
                log_path=mini_log,
            )
            if code != 0:
                print(f'mini_test_failed exit={code} log={mini_log}')
                return code
            print(f'mini_test_passed log={mini_log}')

    syntax_log = logs_dir / 'preflight_syntax.log'
    syntax_cmd = [
        'python3',
        '-m',
        'py_compile',
        str(root / 'scripts' / 'branding' / 'name_generator.py'),
        str(root / 'scripts' / 'branding' / 'naming_db.py'),
        str(root / 'scripts' / 'branding' / 'name_ideation_ingest.py'),
        str(root / 'scripts' / 'branding' / 'name_input_ingest.py'),
        str(root / 'scripts' / 'branding' / 'naming_validate_async.py'),
        str(root / 'scripts' / 'branding' / 'naming_ideation_stage.py'),
        str(root / 'scripts' / 'branding' / 'naming_campaign_runner.py'),
    ]
    if run_cmd(syntax_cmd, cwd=root, log_path=syntax_log) != 0:
        print(f'preflight_syntax_failed log={syntax_log}')
        return 1

    if shutil.which('ruff'):
        ruff_log = logs_dir / 'preflight_ruff.log'
        ruff_cmd = [
            'ruff',
            'check',
            str(root / 'scripts' / 'branding' / 'name_generator.py'),
            str(root / 'scripts' / 'branding' / 'naming_db.py'),
            str(root / 'scripts' / 'branding' / 'name_ideation_ingest.py'),
            str(root / 'scripts' / 'branding' / 'name_input_ingest.py'),
            str(root / 'scripts' / 'branding' / 'naming_validate_async.py'),
            str(root / 'scripts' / 'branding' / 'naming_ideation_stage.py'),
            str(root / 'scripts' / 'branding' / 'naming_campaign_runner.py'),
        ]
        if run_cmd(ruff_cmd, cwd=root, log_path=ruff_log) != 0:
            print(f'preflight_ruff_failed log={ruff_log}')
            return 1

    if db_path.exists():
        db_path.unlink()

    ingest_curated_log = logs_dir / 'ingest_curated.log'
    ingest_curated_cmd = [
        'python3',
        str(root / 'scripts' / 'branding' / 'name_input_ingest.py'),
        '--db',
        str(db_path),
        '--inputs',
        str(root / 'docs' / 'branding' / 'source_inputs_v2.csv'),
        '--source-label',
        'curated_lexicon_v2',
        '--scope',
        'global',
        '--gate',
        'balanced',
        '--derive-morphology',
        '--morph-confidence-scale',
        '0.72',
        '--also-candidates',
    ]
    if run_cmd(ingest_curated_cmd, cwd=root, log_path=ingest_curated_log) != 0:
        print(f'ingest_curated_failed log={ingest_curated_log}')
        return 1

    names_txt = root / 'names.txt'
    if args.include_names_txt and names_txt.exists():
        ingest_names_log = logs_dir / 'ingest_names_txt.log'
        ingest_names_cmd = [
            'python3',
            str(root / 'scripts' / 'branding' / 'name_input_ingest.py'),
            '--db',
            str(db_path),
            '--inputs',
            str(names_txt),
            '--source-label',
            'swahili_names',
            '--default-confidence',
            '0.72',
        ]
        code = run_cmd(ingest_names_cmd, cwd=root, log_path=ingest_names_log)
        if code == 0:
            print(f'ingest_names_txt_done source={names_txt}')
        else:
            print(f'ingest_names_txt_failed exit={code} log={ingest_names_log}')

    seen_shortlist: set[str] = set()
    if seen_names_path.exists():
        seen_shortlist = {line.strip() for line in seen_names_path.read_text(encoding='utf-8').splitlines() if line.strip()}
    novelty_window: deque[int] = deque(maxlen=max(1, args.stop_window))

    started = time.monotonic()
    deadline = started + max(0.1, args.hours) * 3600.0
    run_count = 0
    error_count = 0
    last_status = 'completed'
    ab_metrics: list[dict[str, float]] = []
    ab_arms = nide.build_ab_arms(max_runs=max(1, args.max_runs), seed=int(args.ab_seed), block_size=4) if args.ab_mode else []
    ab_report_paths: tuple[Path, Path] | None = None

    while run_count < max(1, args.max_runs) and time.monotonic() < deadline:
        run_count += 1
        run_started = time.monotonic()
        run_stamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        run_id = f'run_{run_count:03d}_{run_stamp}'

        share, scope, gate, quota_profile = shard_combos[(run_count - 1) % len(shard_combos)]
        quota_profile_effective = quota_profile
        quota_adjust_meta = {'adjusted': False}
        if args.adapt_family_quotas:
            quota_profile_effective, quota_adjust_meta = nide.adapt_family_quotas(
                runs_dir=runs_dir,
                base_quota_profile=quota_profile,
                active_families=active_families,
                window_runs=max(1, args.dynamic_window_runs),
            )
            if quota_adjust_meta.get('adjusted'):
                print(f'quota_adjusted run={run_id} changes={quota_adjust_meta.get("changes", {})}')
        if args.enforce_family_quota_parity:
            ok, msg = validate_quota_profile(active_families=active_families, quota_profile=quota_profile_effective)
            if not ok:
                print(f'run_failed idx={run_count} stage=quota_validation msg={msg}')
                error_count += 1
                if error_count >= max(1, args.max_errors):
                    break
                continue

        run_csv = runs_dir / f'{run_id}.csv'
        run_json = runs_dir / f'{run_id}.json'
        run_log = runs_dir / f'{run_id}.jsonl'
        gen_log = logs_dir / f'{run_id}_generator.log'
        validator_log = logs_dir / f'{run_id}_validator.log'
        assert_log = logs_dir / f'{run_id}_assert.log'

        arm = ab_arms[run_count - 1] if args.ab_mode and run_count - 1 < len(ab_arms) else 'single'
        llm_active_for_run = bool(args.llm_ideation_enabled and (arm != 'A'))
        print(
            f'run_start idx={run_count} id={run_id} arm={arm} '
            f'shard={args.shard_id + 1}/{args.shard_count} '
            f'share={share:.2f} scope={scope} gate={gate}'
        )

        llm_artifact: Path | None = None
        llm_report: dict[str, Any] = {
            'provider': args.llm_provider,
            'model': args.llm_model,
            'status': 'skipped',
            'candidate_count': 0,
            'cost_usd': 0.0,
        }
        if llm_active_for_run:
            llm_artifact, llm_report = run_active_llm_ideation(
                args=args,
                runs_dir=runs_dir,
                logs_dir=logs_dir,
                run_id=run_id,
                run_index=run_count,
                scope=scope,
                seen_shortlist=seen_shortlist,
                context_packet=llm_context_packet,
            )
            print(
                f'llm_ideation_complete run={run_id} status={llm_report.get("status")} '
                f'candidates={llm_report.get("candidate_count")} cost_usd={llm_report.get("cost_usd")}'
            )

        check_limit = max(1, int(args.check_limit))
        pool_size = max(1, int(args.pool_size))
        shortlist_size = max(1, int(args.shortlist_size))

        generator_cmd = [
            'python3',
            str(root / 'scripts' / 'branding' / 'name_generator.py'),
            '--pipeline-version=v3',
            '--enable-v3',
            '--use-engine-interfaces',
            '--use-tiered-validation',
            f'--scope={scope}',
            f'--gate={gate}',
            '--variation-profile=expanded',
            f'--generator-families={",".join(active_families)}',
            f'--family-quotas={quota_profile_effective}',
            f'--source-pool-db={db_path}',
            '--source-pool-limit=900',
            '--source-min-confidence=0.55',
            f'--store-countries={args.generator_store_countries}',
            f'--source-influence-share={share:.2f}',
            f'--pool-size={pool_size}',
            f'--check-limit={check_limit}',
            f'--shortlist-size={shortlist_size}',
            '--shortlist-max-bucket=2',
            '--shortlist-max-prefix3=2',
            '--shortlist-max-phonetic=1',
            '--persist-db',
            f'--db={db_path}',
            f'--output={run_csv}',
            f'--json-output={run_json}',
            f'--run-log={run_log}',
        ]
        if args.generator_seeds.strip():
            generator_cmd.append(f'--seeds={args.generator_seeds.strip()}')
        if args.generator_quality_first:
            generator_cmd.append('--quality-first')
        if llm_artifact is not None:
            generator_cmd.append(f'--llm-input={llm_artifact}')
        if args.generator_no_external_checks:
            generator_cmd.extend(
                [
                    '--degraded-network-mode',
                    '--no-domain-check',
                    '--no-store-check',
                    '--no-web-check',
                    '--no-package-check',
                    '--no-social-check',
                    '--no-progress',
                ]
            )
        elif args.generator_degraded_network_mode:
            generator_cmd.append('--degraded-network-mode')
        code = run_cmd(generator_cmd, cwd=root, log_path=gen_log)
        if code != 0:
            error_count += 1
            last_status = 'generator_failed'
            append_progress_row(
                progress_csv,
                {
                    'run': run_count,
                    'arm': arm,
                    'llm_active': int(llm_active_for_run),
                    'llm_provider': llm_report.get('provider', ''),
                    'llm_model': llm_report.get('model', ''),
                    'llm_candidate_count': llm_report.get('candidate_count', 0),
                    'llm_cost_usd': llm_report.get('cost_usd', 0.0),
                    'llm_stage_status': llm_report.get('status', 'skipped'),
                    'shard_id': args.shard_id,
                    'shard_count': args.shard_count,
                    'shard_combo_count': len(shard_combos),
                    'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
                    'scope': scope,
                    'gate': gate,
                    'source_influence_share': f'{share:.2f}',
                    'quota_profile': quota_profile,
                    'quota_profile_effective': quota_profile_effective,
                    'status': last_status,
                    'duration_s': int(time.monotonic() - run_started),
                },
            )
            print(f'run_failed idx={run_count} stage=generator exit={code} log={gen_log}')
            if error_count >= max(1, args.max_errors):
                break
            if time.monotonic() < deadline:
                time.sleep(max(0, args.sleep_s))
            continue

        hard_fail_ratio = nide.compute_hard_fail_ratio(run_csv)

        validator_cmd = [
            'python3',
            str(root / 'scripts' / 'branding' / 'naming_validate_async.py'),
            '--db',
            str(db_path),
            '--pipeline-version=v3',
            '--enable-v3',
            '--state-filter=new,checked',
            f'--scope={scope}',
            f'--gate={gate}',
            f'--expensive-finalist-limit={max(1, int(args.validator_expensive_finalist_limit))}',
            '--finalist-recommendations=strong,consider',
            f'--checks={args.validator_checks}',
            f'--validation-tier={args.validator_tier}',
            f'--candidate-limit={max(1, int(args.validator_candidate_limit))}',
            f'--concurrency={max(1, int(args.validator_concurrency))}',
        ]
        code = run_cmd(validator_cmd, cwd=root, log_path=validator_log)
        if code != 0:
            error_count += 1
            last_status = 'validator_failed'
            append_progress_row(
                progress_csv,
                {
                    'run': run_count,
                    'arm': arm,
                    'llm_active': int(llm_active_for_run),
                    'llm_provider': llm_report.get('provider', ''),
                    'llm_model': llm_report.get('model', ''),
                    'llm_candidate_count': llm_report.get('candidate_count', 0),
                    'llm_cost_usd': llm_report.get('cost_usd', 0.0),
                    'llm_stage_status': llm_report.get('status', 'skipped'),
                    'shard_id': args.shard_id,
                    'shard_count': args.shard_count,
                    'shard_combo_count': len(shard_combos),
                    'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
                    'scope': scope,
                    'gate': gate,
                    'source_influence_share': f'{share:.2f}',
                    'quota_profile': quota_profile,
                    'quota_profile_effective': quota_profile_effective,
                    'hard_fail_ratio': round(hard_fail_ratio, 6),
                    'status': last_status,
                    'duration_s': int(time.monotonic() - run_started),
                },
            )
            print(f'run_failed idx={run_count} stage=validator exit={code} log={validator_log}')
            if error_count >= max(1, args.max_errors):
                break
            if time.monotonic() < deadline:
                time.sleep(max(0, args.sleep_s))
            continue

        assert_cmd = [
            'python3',
            str(root / 'scripts' / 'branding' / 'naming_db.py'),
            '--db',
            str(db_path),
            'assert-contract',
            '--min-candidates=10',
            '--require-shortlist',
        ]
        code = run_cmd(assert_cmd, cwd=root, log_path=assert_log)
        if code != 0:
            error_count += 1
            last_status = 'assert_failed'
            print(f'run_failed idx={run_count} stage=assert_contract exit={code} log={assert_log}')
            if error_count >= max(1, args.max_errors):
                break
            if time.monotonic() < deadline:
                time.sleep(max(0, args.sleep_s))
            continue

        shortlist = load_shortlist_names(run_csv)
        new_names = [name for name in shortlist if name not in seen_shortlist]
        seen_shortlist.update(new_names)
        novelty_window.append(len(new_names))
        if new_names:
            with seen_names_path.open('a', encoding='utf-8') as handle:
                for name in new_names:
                    handle.write(f'{name}\n')

        run_summary = extract_run_summary(validator_log)
        status_counts = run_summary.get('status_counts', {})
        tier_counts = run_summary.get('tier_result_counts', {})
        total_jobs = run_summary.get('total_jobs', 0)

        duration_s = int(time.monotonic() - run_started)
        append_progress_row(
            progress_csv,
            {
                'run': run_count,
                'arm': arm,
                'llm_active': int(llm_active_for_run),
                'llm_provider': llm_report.get('provider', ''),
                'llm_model': llm_report.get('model', ''),
                'llm_candidate_count': llm_report.get('candidate_count', 0),
                'llm_cost_usd': llm_report.get('cost_usd', 0.0),
                'llm_stage_status': llm_report.get('status', 'skipped'),
                'shard_id': args.shard_id,
                'shard_count': args.shard_count,
                'shard_combo_count': len(shard_combos),
                'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
                'scope': scope,
                'gate': gate,
                'source_influence_share': f'{share:.2f}',
                'quota_profile': quota_profile,
                'quota_profile_effective': quota_profile_effective,
                'shortlist_count': len(shortlist),
                'new_shortlist_count': len(new_names),
                'hard_fail_ratio': round(hard_fail_ratio, 6),
                'cumulative_unique_shortlist': len(seen_shortlist),
                'validator_total_jobs': total_jobs,
                'validator_status_counts': json.dumps(status_counts, ensure_ascii=False),
                'validator_tier_result_counts': json.dumps(tier_counts, ensure_ascii=False),
                'status': 'ok',
                'duration_s': duration_s,
            },
        )

        if args.ab_mode and arm in {'A', 'B'}:
            ab_metrics.append(
                {
                    'arm': arm,
                    'new_shortlist_count': float(len(new_names)),
                    'hard_fail_ratio': float(hard_fail_ratio),
                }
            )

        remaining_s = max(0, int(deadline - time.monotonic()))
        print(
            f'run_done idx={run_count} arm={arm} duration_s={duration_s} shortlist={len(shortlist)} '
            f'new={len(new_names)} unique_total={len(seen_shortlist)} '
            f'hard_fail_ratio={hard_fail_ratio:.4f} validator_total_jobs={total_jobs} '
            f'remaining_s={remaining_s} shard={args.shard_id + 1}/{args.shard_count}'
        )
        last_status = 'ok'

        if (
            len(novelty_window) >= max(1, args.stop_window)
            and sum(novelty_window) < max(0, args.stop_min_new)
        ):
            print(
                f'early_stop triggered: novelty_window={list(novelty_window)} '
                f'sum={sum(novelty_window)} < stop_min_new={args.stop_min_new}'
            )
            last_status = 'early_stop_low_novelty'
            break

        if time.monotonic() < deadline and run_count < max(1, args.max_runs):
            time.sleep(max(0, args.sleep_s))

    if args.ab_mode:
        ab_report_paths = write_ab_report(out_dir=out_dir, metrics=ab_metrics, seed=int(args.ab_seed))
        if ab_report_paths:
            print(f'ab_report_written json={ab_report_paths[0]} md={ab_report_paths[1]}')

    summary = {
        'finished_at': dt.datetime.now().isoformat(timespec='seconds'),
        'out_dir': str(out_dir),
        'db': str(db_path),
        'hours_budget': float(args.hours),
        'max_runs': int(args.max_runs),
        'shard_id': int(args.shard_id),
        'shard_count': int(args.shard_count),
        'shard_combo_count': int(len(shard_combos)),
        'runs_executed': int(run_count),
        'errors': int(error_count),
        'status': last_status,
        'unique_shortlist_names': int(len(seen_shortlist)),
        'progress_csv': str(progress_csv),
        'seen_shortlist_names_path': str(seen_names_path),
        'ab_mode': bool(args.ab_mode),
        'ab_report_json': str(ab_report_paths[0]) if ab_report_paths else '',
        'ab_report_md': str(ab_report_paths[1]) if ab_report_paths else '',
    }
    campaign_summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(f'campaign_complete summary={campaign_summary_path}')
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if error_count < max(1, args.max_errors) else 1


if __name__ == '__main__':
    raise SystemExit(main())
