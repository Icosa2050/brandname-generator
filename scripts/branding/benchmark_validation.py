#!/usr/bin/env python3
"""Benchmark harness for naming validator parallelism tuning.

Runs multiple validator executions over synthetic candidate sets and reports:
- wall clock duration
- jobs/sec
- lock wait metrics from stage_event complete payload
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import naming_db as ndb


def parse_csv_ints(raw: str) -> list[int]:
    out: list[int] = []
    for part in str(raw or '').split(','):
        token = part.strip()
        if not token:
            continue
        try:
            value = int(token)
        except ValueError:
            continue
        if value > 0:
            out.append(value)
    return out


def make_name(rng: random.Random) -> str:
    vowels = 'aeiou'
    consonants = 'bcdfghjklmnprstvwz'
    length = rng.randint(6, 8)
    parts: list[str] = []
    for idx in range(length):
        if idx % 2 == 0:
            parts.append(rng.choice(consonants))
        else:
            parts.append(rng.choice(vowels))
    return ''.join(parts)


def generate_names(*, count: int, seed: int) -> list[str]:
    rng = random.Random(seed)
    names: list[str] = []
    seen: set[str] = set()
    while len(names) < max(1, count):
        candidate = make_name(rng)
        if candidate in seen:
            continue
        seen.add(candidate)
        names.append(candidate)
    return names


def split_counts(total: int, shards: int) -> list[int]:
    base = total // max(1, shards)
    rem = total % max(1, shards)
    return [base + (1 if idx < rem else 0) for idx in range(max(1, shards))]


def seed_db(db_path: Path, names: list[str]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with ndb.open_connection(db_path) as conn:
        ndb.ensure_schema(conn)
        for name in names:
            ndb.upsert_candidate(
                conn,
                name_display=name,
                total_score=60.0,
                risk_score=20.0,
                recommendation='consider',
                quality_score=65.0,
                engine_id='benchmark_seed',
                parent_ids='',
                status='new',
                rejection_reason='',
            )
        conn.commit()


def parse_complete_stage(stdout_text: str) -> dict[str, object]:
    payload: dict[str, object] = {}
    for raw in stdout_text.splitlines():
        line = raw.strip()
        if not line.startswith('stage_event='):
            continue
        blob = line[len('stage_event=') :]
        try:
            value = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get('stage') == 'complete':
            payload = value
    return payload


@dataclass
class BenchmarkRow:
    round_index: int
    candidate_count: int
    concurrency: int
    shard_count: int
    wall_time_ms: int
    total_jobs: int
    jobs_per_sec: float
    lock_wait_ms: int
    lock_contended_count: int
    status: str


def run_combo(
    *,
    root: Path,
    work_dir: Path,
    candidate_count: int,
    concurrency: int,
    shard_count: int,
    checks: str,
    round_index: int,
) -> BenchmarkRow:
    run_dir = work_dir / f'cand_{candidate_count}_conc_{concurrency}_shards_{shard_count}_round_{round_index}'
    run_dir.mkdir(parents=True, exist_ok=True)
    all_names = generate_names(count=candidate_count, seed=(candidate_count * 1000 + concurrency * 100 + shard_count * 10 + round_index))
    shard_sizes = split_counts(candidate_count, shard_count)

    shard_chunks: list[list[str]] = []
    cursor = 0
    for size in shard_sizes:
        shard_chunks.append(all_names[cursor : cursor + size])
        cursor += size

    cmds: list[list[str]] = []
    for idx, names in enumerate(shard_chunks):
        db_path = run_dir / f'shard_{idx}.db'
        seed_db(db_path, names)
        cmds.append(
            [
                'python3',
                str(root / 'scripts' / 'branding' / 'naming_validate_async.py'),
                f'--db={db_path}',
                f'--checks={checks}',
                '--validation-tier=cheap',
                f'--candidate-limit={max(1, len(names))}',
                f'--concurrency={max(1, concurrency)}',
                '--min-concurrency=1',
                f'--max-concurrency={max(2, concurrency)}',
                '--max-retries=0',
                '--state-filter=new',
                '--scope=global',
                '--gate=balanced',
                '--no-progress',
            ]
        )

    started = time.monotonic()
    procs = [
        subprocess.Popen(
            cmd,
            cwd=str(root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for cmd in cmds
    ]

    shard_status = 'ok'
    total_jobs = 0
    lock_wait_ms = 0
    lock_contended_count = 0
    for proc in procs:
        stdout_text, _ = proc.communicate()
        if proc.returncode != 0:
            shard_status = 'failed'
        complete_payload = parse_complete_stage(stdout_text)
        total_jobs += int(complete_payload.get('executed_job_count') or 0)
        lock_wait_ms += int(complete_payload.get('lock_total_wait_ms') or 0)
        lock_contended_count += int(complete_payload.get('lock_contended_count') or 0)

    wall_time_ms = int((time.monotonic() - started) * 1000)
    jobs_per_sec = (total_jobs / max(0.001, wall_time_ms / 1000.0)) if total_jobs > 0 else 0.0
    return BenchmarkRow(
        round_index=round_index,
        candidate_count=candidate_count,
        concurrency=concurrency,
        shard_count=shard_count,
        wall_time_ms=wall_time_ms,
        total_jobs=total_jobs,
        jobs_per_sec=round(jobs_per_sec, 4),
        lock_wait_ms=lock_wait_ms,
        lock_contended_count=lock_contended_count,
        status=shard_status,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Benchmark naming validator parallelism.')
    parser.add_argument('--db', default='', help='Optional base DB path. Benchmark workspace is created beside it.')
    parser.add_argument('--candidate-counts', default='50,100,200')
    parser.add_argument('--concurrency-levels', default='1,4,8,16')
    parser.add_argument('--shard-counts', default='1,2,4')
    parser.add_argument('--checks', default='adversarial,psych,descriptive')
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--quick', action='store_true', help='Run minimal CI-friendly benchmark matrix.')
    parser.add_argument(
        '--csv-output',
        default='test_outputs/branding/benchmark_validation.csv',
        help='Output CSV path.',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    csv_path = Path(args.csv_output).expanduser()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if args.db.strip():
        base = Path(args.db).expanduser().parent
        work_dir = base / 'benchmark_validation'
    else:
        work_dir = root / 'test_outputs' / 'branding' / 'benchmark_validation'
    work_dir.mkdir(parents=True, exist_ok=True)

    if args.quick:
        candidate_counts = [50]
        concurrency_levels = [1, 8]
        shard_counts = [1]
        rounds = 1
    else:
        candidate_counts = parse_csv_ints(args.candidate_counts)
        concurrency_levels = parse_csv_ints(args.concurrency_levels)
        shard_counts = parse_csv_ints(args.shard_counts)
        rounds = max(1, int(args.rounds))

    if not candidate_counts or not concurrency_levels or not shard_counts:
        print('Invalid benchmark matrix: candidate/concurrency/shard lists must be non-empty positive integers.')
        return 1

    rows: list[BenchmarkRow] = []
    for candidate_count in candidate_counts:
        for concurrency in concurrency_levels:
            for shard_count in shard_counts:
                for round_index in range(1, rounds + 1):
                    row = run_combo(
                        root=root,
                        work_dir=work_dir,
                        candidate_count=candidate_count,
                        concurrency=concurrency,
                        shard_count=shard_count,
                        checks=args.checks,
                        round_index=round_index,
                    )
                    rows.append(row)
                    print(
                        f'benchmark_result candidates={row.candidate_count} concurrency={row.concurrency} '
                        f'shards={row.shard_count} round={row.round_index} wall_ms={row.wall_time_ms} '
                        f'jobs={row.total_jobs} jobs_per_sec={row.jobs_per_sec:.3f} '
                        f'lock_wait_ms={row.lock_wait_ms} contended={row.lock_contended_count} status={row.status}'
                    )

    with csv_path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                'round',
                'candidates',
                'concurrency',
                'shards',
                'wall_time_ms',
                'jobs',
                'jobs_per_sec',
                'lock_wait_ms',
                'lock_contended_count',
                'status',
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    'round': row.round_index,
                    'candidates': row.candidate_count,
                    'concurrency': row.concurrency,
                    'shards': row.shard_count,
                    'wall_time_ms': row.wall_time_ms,
                    'jobs': row.total_jobs,
                    'jobs_per_sec': f'{row.jobs_per_sec:.4f}',
                    'lock_wait_ms': row.lock_wait_ms,
                    'lock_contended_count': row.lock_contended_count,
                    'status': row.status,
                }
            )

    print(f'benchmark_csv={csv_path}')
    print('Top rows by jobs/sec:')
    top_rows = sorted(rows, key=lambda item: item.jobs_per_sec, reverse=True)[:5]
    for row in top_rows:
        print(
            f'  candidates={row.candidate_count} concurrency={row.concurrency} shards={row.shard_count} '
            f'jobs_per_sec={row.jobs_per_sec:.3f} wall_ms={row.wall_time_ms} status={row.status}'
        )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
