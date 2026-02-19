#!/usr/bin/env python3
"""Long-running naming campaign runner with parameter sweep + novelty tracking.

This script is intended for first-time long runs:
1) optional mini smoke test
2) repeated v3 generator + async validator runs with varied parameters
3) run-by-run progress + novelty reporting and optional early stop
"""

from __future__ import annotations

import atexit
import argparse
import csv
import datetime as dt
import json
import os
import shlex
import shutil
import socket
import subprocess
import time
from collections import deque
from pathlib import Path


TRUTHY_VALUES = {'1', 'true', 'yes', 'y'}


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
        'shard_id',
        'shard_count',
        'shard_combo_count',
        'timestamp',
        'scope',
        'gate',
        'source_influence_share',
        'quota_profile',
        'shortlist_count',
        'new_shortlist_count',
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
        '--out-dir',
        default='',
        help='Campaign output root (default test_outputs/branding/naming_campaign_<timestamp>).',
    )
    parser.add_argument(
        '--db',
        default='',
        help='SQLite DB path (default <out-dir>/naming_campaign.db).',
    )
    parser.add_argument(
        '--include-names-txt',
        dest='include_names_txt',
        action='store_true',
        default=True,
        help='Ingest names.txt if present in repository root.',
    )
    parser.add_argument('--no-include-names-txt', dest='include_names_txt', action='store_false')
    parser.add_argument(
        '--shard-id',
        type=int,
        default=0,
        help='0-based shard id for parallel campaign workers.',
    )
    parser.add_argument(
        '--shard-count',
        type=int,
        default=1,
        help='Total number of shard workers.',
    )
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
    if not shares or not scopes or not gates or not quota_profiles:
        print('Invalid sweep configuration: shares/scopes/gates/quota-profiles must be non-empty.')
        return 1
    if args.shard_count < 1:
        print('Invalid shard configuration: --shard-count must be >= 1.')
        return 1
    if args.shard_id < 0 or args.shard_id >= args.shard_count:
        print('Invalid shard configuration: --shard-id must be in range [0, --shard-count).')
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
        f'validator_candidate_limit={args.validator_candidate_limit}'
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

    while run_count < max(1, args.max_runs) and time.monotonic() < deadline:
        run_count += 1
        run_started = time.monotonic()
        run_stamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        run_id = f'run_{run_count:03d}_{run_stamp}'

        share, scope, gate, quota_profile = shard_combos[(run_count - 1) % len(shard_combos)]

        run_csv = runs_dir / f'{run_id}.csv'
        run_json = runs_dir / f'{run_id}.json'
        run_log = runs_dir / f'{run_id}.jsonl'
        gen_log = logs_dir / f'{run_id}_generator.log'
        validator_log = logs_dir / f'{run_id}_validator.log'
        assert_log = logs_dir / f'{run_id}_assert.log'

        print(
            f'run_start idx={run_count} id={run_id} '
            f'shard={args.shard_id + 1}/{args.shard_count} '
            f'share={share:.2f} scope={scope} gate={gate}'
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
            '--generator-families=coined,stem,suggestive,morphology,seed,expression,source_pool,blend',
            f'--family-quotas={quota_profile}',
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
                    'shard_id': args.shard_id,
                    'shard_count': args.shard_count,
                    'shard_combo_count': len(shard_combos),
                    'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
                    'scope': scope,
                    'gate': gate,
                    'source_influence_share': f'{share:.2f}',
                    'quota_profile': quota_profile,
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
                    'shard_id': args.shard_id,
                    'shard_count': args.shard_count,
                    'shard_combo_count': len(shard_combos),
                    'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
                    'scope': scope,
                    'gate': gate,
                    'source_influence_share': f'{share:.2f}',
                    'quota_profile': quota_profile,
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
                'shard_id': args.shard_id,
                'shard_count': args.shard_count,
                'shard_combo_count': len(shard_combos),
                'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
                'scope': scope,
                'gate': gate,
                'source_influence_share': f'{share:.2f}',
                'quota_profile': quota_profile,
                'shortlist_count': len(shortlist),
                'new_shortlist_count': len(new_names),
                'cumulative_unique_shortlist': len(seen_shortlist),
                'validator_total_jobs': total_jobs,
                'validator_status_counts': json.dumps(status_counts, ensure_ascii=False),
                'validator_tier_result_counts': json.dumps(tier_counts, ensure_ascii=False),
                'status': 'ok',
                'duration_s': duration_s,
            },
        )

        remaining_s = max(0, int(deadline - time.monotonic()))
        print(
            f'run_done idx={run_count} duration_s={duration_s} shortlist={len(shortlist)} '
            f'new={len(new_names)} unique_total={len(seen_shortlist)} '
            f'validator_total_jobs={total_jobs} remaining_s={remaining_s} '
            f'shard={args.shard_id + 1}/{args.shard_count}'
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
    }
    campaign_summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(f'campaign_complete summary={campaign_summary_path}')
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if error_count < max(1, args.max_errors) else 1


if __name__ == '__main__':
    raise SystemExit(main())
