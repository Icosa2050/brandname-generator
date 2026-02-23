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
import select
import shlex
import shutil
import socket
import statistics
import subprocess
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import naming_db as ndb
import naming_ideation_stage as nide
import path_config as bpaths

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback
    tomllib = None  # type: ignore[assignment]


TRUTHY_VALUES = {'1', 'true', 'yes', 'y'}
DEFAULT_GENERATOR_FAMILIES = ['coined', 'stem', 'suggestive', 'morphology', 'seed', 'expression', 'source_pool', 'blend']
SweepCombo = tuple[float, str, str, str]


def parse_csv_list(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(',') if part.strip()]


def parse_model_list(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in parse_csv_list(str(raw or '').replace('|', ',')):
        model = str(part).strip()
        if not model or model in seen:
            continue
        seen.add(model)
        out.append(model)
    return out


def _append_models_unique(target: list[str], source: list[str]) -> None:
    seen = set(target)
    for model in source:
        key = str(model).strip()
        if not key or key in seen:
            continue
        target.append(key)
        seen.add(key)


def _coerce_models_node(raw: Any) -> list[str]:
    if isinstance(raw, str):
        return parse_model_list(raw)
    if isinstance(raw, list):
        joined = ','.join(str(item).strip() for item in raw if str(item).strip())
        return parse_model_list(joined)
    if isinstance(raw, dict):
        return _coerce_models_node(raw.get('models'))
    return []


def _parse_txt_model_config(raw_text: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for line in raw_text.splitlines():
        trimmed = line.split('#', 1)[0].strip()
        if not trimmed:
            continue
        if '=' in trimmed:
            provider_raw, blob = trimmed.split('=', 1)
            provider = provider_raw.strip().lower() or 'default'
            models = parse_model_list(blob)
        else:
            provider = 'default'
            models = parse_model_list(trimmed)
        if not models:
            continue
        bucket = out.setdefault(provider, [])
        _append_models_unique(bucket, models)
    return out


def _extract_model_config_map(payload: Any) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}

    def add(provider: str, models_node: Any) -> None:
        models = _coerce_models_node(models_node)
        if not models:
            return
        key = str(provider or '').strip().lower() or 'default'
        bucket = out.setdefault(key, [])
        _append_models_unique(bucket, models)

    if isinstance(payload, (str, list)):
        add('default', payload)
        return out
    if not isinstance(payload, dict):
        return out

    providers_node = payload.get('providers')
    if isinstance(providers_node, dict):
        for provider, models_node in providers_node.items():
            add(str(provider), models_node)

    if 'models' in payload and not isinstance(payload.get('models'), dict):
        add('default', payload.get('models'))
    if 'default' in payload:
        add('default', payload.get('default'))

    for key, value in payload.items():
        if str(key) in {'providers', 'models', 'default', 'scheduler'}:
            continue
        add(str(key), value)

    return out


def load_llm_model_config(path: str) -> dict[str, list[str]]:
    config_path = Path(str(path or '').strip()).expanduser()
    if not str(path or '').strip():
        return {}
    if not config_path.exists():
        raise ValueError(f'llm_model_config_not_found:{config_path}')
    try:
        raw_text = config_path.read_text(encoding='utf-8')
    except OSError as exc:
        raise ValueError(f'llm_model_config_read_error:{config_path}:{exc}') from exc

    suffix = config_path.suffix.lower()
    payload: Any
    if suffix == '.json':
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ValueError(f'llm_model_config_invalid_json:{config_path}:{exc}') from exc
        return _extract_model_config_map(payload)
    if suffix == '.toml':
        if tomllib is None:
            raise ValueError('llm_model_config_toml_requires_python311')
        try:
            payload = tomllib.loads(raw_text)
        except Exception as exc:
            raise ValueError(f'llm_model_config_invalid_toml:{config_path}:{exc}') from exc
        return _extract_model_config_map(payload)
    return _parse_txt_model_config(raw_text)


def resolve_llm_models_for_provider(
    *,
    args: argparse.Namespace,
    provider: str,
    cli_models_raw: str,
    fallback_model_raw: str,
) -> tuple[list[str], str, str]:
    explicit_models = parse_model_list(cli_models_raw)
    if explicit_models:
        return explicit_models, 'cli_models', ''

    config_path = str(getattr(args, 'llm_model_config', '') or '').strip()
    if config_path:
        try:
            model_config = load_llm_model_config(config_path)
        except ValueError as exc:
            return [], 'model_config', str(exc)
        provider_key = str(provider or '').strip().lower()
        configured = model_config.get(provider_key, [])
        if not configured:
            configured = model_config.get('default', [])
        if configured:
            return configured, 'model_config', ''
        return [], 'model_config', f'no_models_for_provider:{provider_key}'

    fallback_model = str(fallback_model_raw or '').strip()
    if fallback_model:
        return [fallback_model], 'single_model', ''
    return [], 'none', 'no_models_configured'


def resolve_llm_models(*, args: argparse.Namespace, provider: str) -> tuple[list[str], str, str]:
    return resolve_llm_models_for_provider(
        args=args,
        provider=provider,
        cli_models_raw=str(getattr(args, 'llm_models', '') or ''),
        fallback_model_raw=str(getattr(args, 'llm_model', '') or ''),
    )


def build_hybrid_provider_round_schedule(*, total_rounds: int, local_rounds: int, remote_rounds: int) -> list[str]:
    planned_total = max(0, int(total_rounds))
    local_target = max(0, int(local_rounds))
    remote_target = max(0, int(remote_rounds))
    if planned_total <= 0:
        return []
    if local_target + remote_target <= 0:
        return []

    provider_order = ['openai_compat', 'openrouter_http']
    targets = {
        'openai_compat': local_target,
        'openrouter_http': remote_target,
    }
    assigned = {
        'openai_compat': 0,
        'openrouter_http': 0,
    }
    schedule: list[str] = []
    while len(schedule) < planned_total:
        candidates = [provider for provider in provider_order if assigned[provider] < targets[provider]]
        if not candidates:
            break

        def progress_ratio(provider: str) -> float:
            target = max(1, int(targets[provider]))
            return float(assigned[provider]) / float(target)

        chosen = min(candidates, key=lambda provider: (progress_ratio(provider), provider_order.index(provider)))
        schedule.append(chosen)
        assigned[chosen] += 1
    return schedule


def select_round_model(*, models: list[str], round_idx: int, selection: str, rng: random.Random) -> str:
    if not models:
        return ''
    if str(selection or '').strip().lower() == 'random':
        return rng.choice(models)
    return models[round_idx % len(models)]


def clamp_share(value: float) -> float:
    return max(0.0, min(1.0, value))


def combo_history_key(*, share: float, scope: str, gate: str, quota_profile: str) -> str:
    return f'{clamp_share(float(share)):.4f}|{scope.strip()}|{gate.strip()}|{quota_profile.strip()}'


def load_combo_duration_history(progress_csv: Path) -> dict[str, float]:
    if not progress_csv.exists():
        return {}
    samples: dict[str, list[float]] = defaultdict(list)
    with progress_csv.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            status = str(row.get('status') or '').strip().lower()
            if status and status not in {'ok', 'completed'}:
                continue
            scope = str(row.get('scope') or '').strip()
            gate = str(row.get('gate') or '').strip()
            quota_profile = str(row.get('quota_profile') or '').strip()
            if not scope or not gate or not quota_profile:
                continue
            try:
                share = float(row.get('source_influence_share') or 0.0)
                duration_s = float(row.get('duration_s') or 0.0)
            except (TypeError, ValueError):
                continue
            if duration_s <= 0:
                continue
            key = combo_history_key(
                share=share,
                scope=scope,
                gate=gate,
                quota_profile=quota_profile,
            )
            samples[key].append(duration_s)
    out: dict[str, float] = {}
    for key, values in samples.items():
        if not values:
            continue
        out[key] = float(round(sum(values) / len(values), 3))
    return out


def assign_sweep_combos_to_shards(
    *,
    sweep_combos: list[SweepCombo],
    shard_count: int,
    scheduling: str,
    history_seconds_by_combo: dict[str, float] | None = None,
    fallback_duration_s: float = 180.0,
) -> tuple[list[list[SweepCombo]], dict[str, Any]]:
    if shard_count < 1:
        raise ValueError('shard_count must be >= 1')
    if not sweep_combos:
        return ([[] for _ in range(shard_count)], {'mode': 'empty', 'predicted_load_s': [0.0] * shard_count})
    if scheduling != 'weighted':
        assignments = [sweep_combos[idx::shard_count] for idx in range(shard_count)]
        return (
            assignments,
            {
                'mode': 'slice',
                'requested_mode': scheduling,
                'history_matches': 0,
                'history_entries': 0,
                'fallback_duration_s': float(max(1.0, fallback_duration_s)),
                'predicted_load_s': [float(len(chunk)) for chunk in assignments],
            },
        )

    history = {
        key: float(value)
        for key, value in (history_seconds_by_combo or {}).items()
        if isinstance(value, (float, int)) and float(value) > 0
    }
    if not history:
        assignments = [sweep_combos[idx::shard_count] for idx in range(shard_count)]
        return (
            assignments,
            {
                'mode': 'slice_fallback_no_history',
                'requested_mode': scheduling,
                'history_matches': 0,
                'history_entries': 0,
                'fallback_duration_s': float(max(1.0, fallback_duration_s)),
                'predicted_load_s': [float(len(chunk)) for chunk in assignments],
            },
        )

    observed: list[float] = []
    weighted: list[tuple[SweepCombo, str, float]] = []
    for combo in sweep_combos:
        share, scope, gate, quota_profile = combo
        key = combo_history_key(share=share, scope=scope, gate=gate, quota_profile=quota_profile)
        duration_s = history.get(key)
        if duration_s is not None:
            observed.append(duration_s)
        weighted.append((combo, key, float(duration_s or 0.0)))

    if not observed:
        assignments = [sweep_combos[idx::shard_count] for idx in range(shard_count)]
        return (
            assignments,
            {
                'mode': 'slice_fallback_no_matches',
                'requested_mode': scheduling,
                'history_matches': 0,
                'history_entries': int(len(history)),
                'fallback_duration_s': float(max(1.0, fallback_duration_s)),
                'predicted_load_s': [float(len(chunk)) for chunk in assignments],
            },
        )

    fallback = float(max(1.0, statistics.median(observed) if observed else fallback_duration_s))
    normalized: list[tuple[SweepCombo, str, float]] = []
    for combo, key, duration_s in weighted:
        normalized.append((combo, key, duration_s if duration_s > 0 else fallback))
    normalized.sort(key=lambda item: (-item[2], item[1]))

    assignments: list[list[SweepCombo]] = [[] for _ in range(shard_count)]
    loads: list[float] = [0.0 for _ in range(shard_count)]
    for combo, _key, weight_s in normalized:
        shard_idx = min(range(shard_count), key=lambda idx: (loads[idx], len(assignments[idx]), idx))
        assignments[shard_idx].append(combo)
        loads[shard_idx] += float(weight_s)

    return (
        assignments,
        {
            'mode': 'weighted',
            'requested_mode': scheduling,
            'history_matches': int(len(observed)),
            'history_entries': int(len(history)),
            'fallback_duration_s': fallback,
            'predicted_load_s': [round(value, 3) for value in loads],
        },
    )


def is_truthy(raw: str) -> bool:
    return raw.strip().lower() in TRUTHY_VALUES


def should_stream_line(line: str, patterns: list[str]) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    for pattern in patterns:
        if pattern and pattern in stripped:
            return True
    return False


def derive_validator_runtime_settings(
    *,
    requested_concurrency: int,
    requested_min_concurrency: int,
    requested_max_concurrency: int,
    requested_timeout_s: float,
) -> dict[str, float | int]:
    min_concurrency = max(1, int(requested_min_concurrency))
    max_concurrency = max(min_concurrency, int(requested_max_concurrency))
    concurrency = max(min_concurrency, min(max_concurrency, int(requested_concurrency)))
    timeout_s = max(0.5, float(requested_timeout_s))
    return {
        'concurrency': int(concurrency),
        'min_concurrency': int(min_concurrency),
        'max_concurrency': int(max_concurrency),
        'timeout_s': float(timeout_s),
    }


def emit_campaign_event(
    *,
    enabled: bool,
    heartbeat_path: Path | None,
    event: str,
    **fields: object,
) -> None:
    if not enabled:
        return
    payload = {
        'event': event,
        'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
        **fields,
    }
    print(f'campaign_event={json.dumps(payload, ensure_ascii=False)}', flush=True)
    if heartbeat_path is not None:
        heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        with heartbeat_path.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + '\n')


def run_cmd(
    cmd: list[str],
    *,
    cwd: Path,
    log_path: Path,
    live_patterns: list[str] | None = None,
    stage_label: str = '',
    run_id: str = '',
    heartbeat_enabled: bool = False,
    heartbeat_path: Path | None = None,
    heartbeat_interval_s: float = 0.0,
) -> int:
    tokens = [pattern.strip() for pattern in (live_patterns or []) if pattern.strip()]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started_monotonic = time.monotonic()
    last_heartbeat_monotonic = started_monotonic
    with log_path.open('w', encoding='utf-8') as handle:
        handle.write(f'$ {" ".join(shlex.quote(part) for part in cmd)}\n\n')
        handle.flush()
        if not tokens:
            emit_campaign_event(
                enabled=heartbeat_enabled,
                heartbeat_path=heartbeat_path,
                event='stage_start',
                run_id=run_id,
                stage=stage_label,
            )
            proc = subprocess.run(cmd, cwd=str(cwd), stdout=handle, stderr=subprocess.STDOUT, check=False)
            emit_campaign_event(
                enabled=heartbeat_enabled,
                heartbeat_path=heartbeat_path,
                event='stage_complete',
                run_id=run_id,
                stage=stage_label,
                exit_code=int(proc.returncode),
                duration_s=round(max(0.0, time.monotonic() - started_monotonic), 3),
            )
            return int(proc.returncode)

        emit_campaign_event(
            enabled=heartbeat_enabled,
            heartbeat_path=heartbeat_path,
            event='stage_start',
            run_id=run_id,
            stage=stage_label,
        )
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors='replace',
            bufsize=1,
        )
        assert proc.stdout is not None
        fd = proc.stdout.fileno()
        while True:
            ready, _, _ = select.select([fd], [], [], 0.25)
            now_monotonic = time.monotonic()
            if heartbeat_interval_s > 0 and (now_monotonic - last_heartbeat_monotonic) >= heartbeat_interval_s:
                emit_campaign_event(
                    enabled=heartbeat_enabled,
                    heartbeat_path=heartbeat_path,
                    event='stage_heartbeat',
                    run_id=run_id,
                    stage=stage_label,
                    elapsed_s=round(max(0.0, now_monotonic - started_monotonic), 3),
                )
                last_heartbeat_monotonic = now_monotonic
            if ready:
                raw = proc.stdout.readline()
                if raw:
                    handle.write(raw)
                    if should_stream_line(raw, tokens):
                        print(raw.rstrip('\n'), flush=True)
                    continue
            if proc.poll() is not None:
                break

        for raw in proc.stdout:
            handle.write(raw)
            if should_stream_line(raw, tokens):
                print(raw.rstrip('\n'), flush=True)
        proc.stdout.close()
        exit_code = int(proc.wait())
        emit_campaign_event(
            enabled=heartbeat_enabled,
            heartbeat_path=heartbeat_path,
            event='stage_complete',
            run_id=run_id,
            stage=stage_label,
            exit_code=exit_code,
            duration_s=round(max(0.0, time.monotonic() - started_monotonic), 3),
        )
        return exit_code


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


def shard_db_path(base_db: Path, shard_id: int) -> Path:
    suffix = base_db.suffix or '.db'
    return base_db.with_name(f'{base_db.stem}_shard{int(shard_id)}{suffix}')


def merge_shard_databases(
    *,
    target_db: Path,
    shard_db_paths: list[Path],
) -> dict[str, Any]:
    existing_shards = [path for path in shard_db_paths if path.exists() and path.resolve() != target_db.resolve()]
    if not existing_shards:
        return {
            'status': 'skipped_no_shard_dbs',
            'target_db': str(target_db),
            'requested_shards': [str(path) for path in shard_db_paths],
            'existing_shards': [],
            'merged_candidates': 0,
            'merged_validation_results': 0,
            'merged_candidate_scores': 0,
            'merged_shortlist_decisions': 0,
        }

    target_db.parent.mkdir(parents=True, exist_ok=True)
    merged_candidates = 0
    merged_validation_results = 0
    merged_candidate_scores = 0
    merged_shortlist_decisions = 0
    with ndb.open_connection(target_db) as conn:
        ndb.ensure_schema(conn)
        merge_run_id = ndb.create_run(
            conn,
            source_path=str(target_db),
            scope='global',
            gate_mode='balanced',
            variation_profile='merge_shards',
            status='running',
            config={'shards': [str(path) for path in existing_shards]},
            summary={},
        )
        conn.commit()

        for idx, shard_db in enumerate(existing_shards):
            alias = f'shard_{idx}'
            conn.execute(f'ATTACH DATABASE ? AS {alias}', (str(shard_db),))
            conn.execute(
                f"""
                INSERT OR IGNORE INTO candidates(
                  name_display, name_normalized, first_seen_at, last_seen_at,
                  current_score, current_risk, current_recommendation, state, state_updated_at,
                  engine_id, parent_ids, status, rejection_reason, score_quality, score_total
                )
                SELECT
                  name_display, name_normalized, first_seen_at, last_seen_at,
                  current_score, current_risk, current_recommendation, state, state_updated_at,
                  engine_id, parent_ids, status, rejection_reason, score_quality, score_total
                FROM {alias}.candidates
                """
            )
            merged_candidates += int(conn.execute('SELECT changes()').fetchone()[0] or 0)

            conn.execute(
                f"""
                INSERT INTO validation_results(
                  candidate_id, run_id, check_type, status, score_delta, hard_fail,
                  reason, evidence_json, checked_at, cache_expires_at
                )
                SELECT
                  tc.id, ?, vr.check_type, vr.status, vr.score_delta, vr.hard_fail,
                  vr.reason, vr.evidence_json, vr.checked_at, vr.cache_expires_at
                FROM {alias}.validation_results vr
                JOIN {alias}.candidates sc ON sc.id = vr.candidate_id
                JOIN candidates tc ON tc.name_normalized = sc.name_normalized
                """,
                (merge_run_id,),
            )
            merged_validation_results += int(conn.execute('SELECT changes()').fetchone()[0] or 0)

            conn.execute(
                f"""
                INSERT INTO candidate_scores(
                  candidate_id, run_id, quality_score, risk_score, external_penalty,
                  total_score, recommendation, hard_fail, reason, created_at
                )
                SELECT
                  tc.id, ?, cs.quality_score, cs.risk_score, cs.external_penalty,
                  cs.total_score, cs.recommendation, cs.hard_fail, cs.reason, cs.created_at
                FROM {alias}.candidate_scores cs
                JOIN {alias}.candidates sc ON sc.id = cs.candidate_id
                JOIN candidates tc ON tc.name_normalized = sc.name_normalized
                """,
                (merge_run_id,),
            )
            merged_candidate_scores += int(conn.execute('SELECT changes()').fetchone()[0] or 0)

            conn.execute(
                f"""
                INSERT INTO shortlist_decisions(
                  candidate_id, run_id, selected, shortlist_rank, bucket_key, reason, score, created_at
                )
                SELECT
                  tc.id, ?, sd.selected, sd.shortlist_rank, sd.bucket_key, sd.reason, sd.score, sd.created_at
                FROM {alias}.shortlist_decisions sd
                JOIN {alias}.candidates sc ON sc.id = sd.candidate_id
                JOIN candidates tc ON tc.name_normalized = sc.name_normalized
                """,
                (merge_run_id,),
            )
            merged_shortlist_decisions += int(conn.execute('SELECT changes()').fetchone()[0] or 0)

            conn.commit()
            conn.execute(f'DETACH DATABASE {alias}')

        merge_summary = {
            'status': 'merged',
            'target_db': str(target_db),
            'existing_shards': [str(path) for path in existing_shards],
            'merged_candidates': int(merged_candidates),
            'merged_validation_results': int(merged_validation_results),
            'merged_candidate_scores': int(merged_candidate_scores),
            'merged_shortlist_decisions': int(merged_shortlist_decisions),
            'merge_run_id': int(merge_run_id),
        }
        conn.execute(
            """
            UPDATE naming_runs
            SET status = ?, summary_json = ?
            WHERE id = ?
            """,
            ('completed', json.dumps(merge_summary, ensure_ascii=False), merge_run_id),
        )
        conn.commit()

    return merge_summary


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


def extract_generator_history_skip(generator_log: Path) -> dict[str, object]:
    if not generator_log.exists():
        return {
            'skipped_count': 0,
            'skipped_generated_count': 0,
            'skipped_finalist_count': 0,
            'skipped_names_sample': [],
        }
    skipped_count = 0
    skipped_generated_count = 0
    skipped_finalist_count = 0
    legacy_skipped_count = 0
    skipped_sample: list[str] = []
    skipped_generated_sample: list[str] = []
    skipped_finalist_sample: list[str] = []
    saw_phased_event = False
    for raw in generator_log.read_text(encoding='utf-8', errors='replace').splitlines():
        line = raw.strip()
        if not line.startswith('stage_event='):
            continue
        blob = line[len('stage_event=') :]
        try:
            value = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict):
            continue
        if str(value.get('stage') or '').strip() != 'history_skip':
            continue
        try:
            current_count = int(value.get('skipped_count') or 0)
        except (TypeError, ValueError):
            current_count = 0
        sample_raw = value.get('skipped_names_sample')
        sample_values: list[str] = []
        if isinstance(sample_raw, list):
            sample_values = [str(item).strip() for item in sample_raw if str(item).strip()]
        phase = str(value.get('phase') or '').strip().lower()
        if phase == 'generated':
            saw_phased_event = True
            if current_count >= skipped_generated_count:
                skipped_generated_count = max(0, current_count)
                skipped_generated_sample = sample_values[:20]
            continue
        if phase in {'finalist', 'finalists'}:
            saw_phased_event = True
            if current_count >= skipped_finalist_count:
                skipped_finalist_count = max(0, current_count)
                skipped_finalist_sample = sample_values[:20]
            continue
        if current_count >= legacy_skipped_count:
            legacy_skipped_count = max(0, current_count)
            skipped_sample = sample_values[:20]

    if saw_phased_event:
        skipped_count = skipped_generated_count + skipped_finalist_count
        skipped_sample = skipped_generated_sample or skipped_finalist_sample or skipped_sample
    else:
        skipped_count = legacy_skipped_count
    return {
        'skipped_count': int(skipped_count),
        'skipped_generated_count': int(skipped_generated_count),
        'skipped_finalist_count': int(skipped_finalist_count),
        'skipped_names_sample': skipped_sample,
    }


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
        'llm_slo_status',
        'llm_slo_success_rate',
        'llm_slo_timeout_rate',
        'llm_slo_empty_rate',
        'llm_slo_breaches',
        'shard_id',
        'shard_count',
        'shard_combo_count',
        'shard_scheduling',
        'combo_key',
        'history_skip_count',
        'history_skip_generated_count',
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
            if nide.is_valid_candidate_name(name):
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
        'model': '',
        'models_requested': [],
        'models_used': {},
        'model_selection': str(getattr(args, 'llm_model_selection', 'round_robin') or 'round_robin'),
        'model_source': '',
        'prompt_template_path': str(getattr(args, 'llm_prompt_template_file', '') or ''),
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
        'slo': {
            'status': 'skipped',
            'attempted_rounds': 0,
            'successful_rounds': 0,
            'timeout_rounds': 0,
            'empty_rounds': 0,
            'success_rate': 0.0,
            'timeout_rate': 0.0,
            'empty_rate': 0.0,
            'breaches': [],
            'pass': False,
        },
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
    prompt_template = ''
    template_path = str(getattr(args, 'llm_prompt_template_file', '') or '').strip()
    if template_path:
        try:
            prompt_template = nide.load_prompt_template(template_path)
        except ValueError as exc:
            report['status'] = 'prompt_template_error'
            report['errors'].append(str(exc))
            return None, report

    target_total = max(1, int(args.llm_rounds)) * max(1, int(args.llm_candidates_per_round))
    names: list[str] = []
    names_seen: set[str] = set()
    stage_started = time.monotonic()
    total_cost = 0.0
    last_call_cost = 0.0
    attempted_rounds = 0
    successful_rounds = 0
    timeout_rounds = 0
    empty_rounds = 0
    llm_log = logs_dir / f'{run_id}_llm.log'
    cache_dir = Path(args.llm_cache_dir).expanduser() if args.llm_cache_dir else Path()

    def append_log(line: str) -> None:
        llm_log.parent.mkdir(parents=True, exist_ok=True)
        with llm_log.open('a', encoding='utf-8') as handle:
            handle.write(line.rstrip() + '\n')

    if args.llm_provider == 'fixture':
        attempted_rounds = 1
        fixture_names, fixture_usage, fixture_err = nide.load_fixture_candidates_with_usage(args.llm_fixture_input)
        names = fixture_names[:target_total]
        if names:
            successful_rounds = 1
            total_cost = nide.estimate_usage_cost_usd(
                usage=fixture_usage,
                in_price_per_1k=max(0.0, float(args.llm_pricing_input_per_1k)),
                out_price_per_1k=max(0.0, float(args.llm_pricing_output_per_1k)),
            )
            report['status'] = 'ok_fixture'
        else:
            empty_rounds = 1
            report['status'] = 'fixture_empty'
            if fixture_err:
                report['errors'].append(f'fixture:{fixture_err}')
    elif args.llm_provider == 'pal':
        attempted_rounds = 1
        fixture_names, fixture_usage, fixture_err = nide.load_fixture_candidates_with_usage(args.llm_fixture_input)
        if fixture_names:
            names = fixture_names[:target_total]
            successful_rounds = 1
            total_cost = nide.estimate_usage_cost_usd(
                usage=fixture_usage,
                in_price_per_1k=max(0.0, float(args.llm_pricing_input_per_1k)),
                out_price_per_1k=max(0.0, float(args.llm_pricing_output_per_1k)),
            )
            report['status'] = 'ok_pal_fixture'
        else:
            empty_rounds = 1
            report['status'] = 'pal_unavailable_without_fixture'
            if fixture_err:
                report['errors'].append(f'pal_fixture:{fixture_err}')
            report['errors'].append('pal mode requires fixture input in CLI runner')
    elif args.llm_provider in {'openrouter_http', 'openai_compat'}:
        source_label = 'openrouter'
        api_key = ''
        model_catalog: set[str] | None = None

        configured_models, model_source, model_err = resolve_llm_models(args=args, provider=args.llm_provider)
        report['model_source'] = model_source
        report['models_requested'] = configured_models
        report['model'] = '|'.join(configured_models)
        if model_err:
            report['status'] = 'model_config_error'
            report['errors'].append(model_err)

        def call_provider(_prompt: str, _model: str) -> tuple[list[str], dict[str, Any], str]:
            return [], {}, 'provider_unavailable'

        if report['status'] != 'model_config_error' and args.llm_provider == 'openrouter_http':
            api_key = os.environ.get(args.llm_api_key_env, '').strip()
            if not api_key:
                report['status'] = 'missing_api_key'
                report['errors'].append(f'missing env {args.llm_api_key_env}')
            else:
                model_catalog = nide.list_openrouter_models(
                    api_key=api_key,
                    timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
                )

                def call_provider(_prompt: str, _model: str) -> tuple[list[str], dict[str, Any], str]:
                    return nide.call_openrouter_candidates(
                        api_key=api_key,
                        model=_model,
                        prompt=_prompt,
                        timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
                        strict_json=bool(args.llm_strict_json),
                        http_referer=args.llm_openrouter_http_referer,
                        x_title=args.llm_openrouter_x_title,
                    )
        elif report['status'] != 'model_config_error':
            source_label = 'openai_compat'
            api_key = os.environ.get(args.llm_openai_api_key_env, '').strip() or 'ollama'
            openai_request_extras: dict[str, Any] = {}
            ttl_s = max(0, int(getattr(args, 'llm_openai_ttl_s', 0)))
            if ttl_s > 0:
                openai_request_extras['ttl'] = ttl_s
            keep_alive_raw = str(getattr(args, 'llm_openai_keep_alive', '') or '').strip()
            if keep_alive_raw:
                keep_alive_value: Any = keep_alive_raw
                if keep_alive_raw.lstrip('-').isdigit():
                    keep_alive_value = int(keep_alive_raw)
                openai_request_extras['keep_alive'] = keep_alive_value
            if openai_request_extras:
                report['openai_request_extras'] = dict(openai_request_extras)
            model_catalog = nide.list_openai_models(
                api_key=api_key,
                base_url=args.llm_openai_base_url,
                timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
            )

            def call_provider(_prompt: str, _model: str) -> tuple[list[str], dict[str, Any], str]:
                return nide.call_openai_compat_candidates(
                    api_key=api_key,
                    base_url=args.llm_openai_base_url,
                    model=_model,
                    prompt=_prompt,
                    timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
                    strict_json=bool(args.llm_strict_json),
                    request_extras=openai_request_extras,
                )

        if report['status'] not in {'missing_api_key', 'model_config_error'}:
            if not configured_models:
                report['status'] = 'model_not_configured'
            else:
                if model_catalog is not None:
                    unavailable_models = [model for model in configured_models if model not in model_catalog]
                    if unavailable_models:
                        report['errors'].append(f'models_not_in_catalog={",".join(unavailable_models)}')
                        configured_models = [model for model in configured_models if model in model_catalog]
                        report['models_requested'] = configured_models
                        report['model'] = '|'.join(configured_models)
                if not configured_models:
                    report['status'] = 'model_not_in_catalog'
                else:
                    report['status'] = 'running'
                used_counts: dict[str, int] = {}
                model_rng_seed = (int(getattr(args, 'ab_seed', 0)) * 1000003) + int(run_index)
                model_rng = random.Random(model_rng_seed)
                for round_idx in range(max(1, int(args.llm_rounds))):
                    if report['status'] != 'running':
                        break
                    attempted_rounds += 1
                    elapsed_ms = int((time.monotonic() - stage_started) * 1000)
                    if elapsed_ms >= max(1000, int(args.llm_stage_timeout_ms)):
                        report['status'] = 'stage_timeout'
                        timeout_rounds += 1
                        break
                    if args.llm_max_usd_per_run > 0:
                        projected = total_cost + max(last_call_cost, 0.0)
                        if projected > float(args.llm_max_usd_per_run):
                            report['status'] = 'budget_stop'
                            break

                    selected_model = select_round_model(
                        models=configured_models,
                        round_idx=round_idx,
                        selection=str(getattr(args, 'llm_model_selection', 'round_robin')),
                        rng=model_rng,
                    )
                    if not selected_model:
                        report['status'] = 'model_not_configured'
                        break
                    used_counts[selected_model] = int(used_counts.get(selected_model, 0)) + 1

                    prompt, mode = nide.build_prompt(
                        scope=scope,
                        round_index=((run_index - 1) * max(1, int(args.llm_rounds))) + round_idx,
                        target_count=max(1, int(args.llm_candidates_per_round)),
                        constraints=constraints,
                        context_packet=context_packet,
                        prompt_template=prompt_template,
                    )
                    mode_key = ':'.join(mode)
                    cache_key_blob = json.dumps(
                        {
                            'provider': args.llm_provider,
                            'base_url': (
                                nide.normalize_openai_compat_base_url(args.llm_openai_base_url)
                                if args.llm_provider == 'openai_compat'
                                else 'https://openrouter.ai/api/v1'
                            ),
                            'model': selected_model,
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
                        if round_names:
                            successful_rounds += 1
                        else:
                            empty_rounds += 1
                            report['errors'].append(f'round={round_idx + 1}:cache_empty')
                        report['cache_hits'] = int(report['cache_hits']) + 1
                        append_log(
                            f'round={round_idx + 1} mode={mode_key} model={selected_model} '
                            f'source=cache names={len(round_names)}'
                        )
                    else:
                        retries = max(0, int(args.llm_max_retries))
                        usage: dict[str, Any] = {}
                        err = 'unknown'
                        for attempt in range(retries + 1):
                            call_names, usage, err = call_provider(prompt, selected_model)
                            if call_names:
                                round_names = call_names
                                successful_rounds += 1
                                call_cost = nide.estimate_usage_cost_usd(
                                    usage=usage,
                                    in_price_per_1k=max(0.0, float(args.llm_pricing_input_per_1k)),
                                    out_price_per_1k=max(0.0, float(args.llm_pricing_output_per_1k)),
                                )
                                total_cost += call_cost
                                last_call_cost = call_cost
                                append_log(
                                    f'round={round_idx + 1} mode={mode_key} model={selected_model} source={source_label} '
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
                                append_log(
                                    f'round={round_idx + 1} model={selected_model} retry={attempt + 1} '
                                    f'err={err} sleep_s={sleep_s:.2f}'
                                )
                                time.sleep(sleep_s)
                                continue
                            if err == 'timeout':
                                timeout_rounds += 1
                            else:
                                empty_rounds += 1
                            report['errors'].append(f'round={round_idx + 1}:{err}')
                            append_log(f'round={round_idx + 1} model={selected_model} err={err}')
                            break

                    for name in round_names:
                        normalized = nide.normalize_alpha_name(name)
                        if not nide.is_valid_candidate_name(normalized):
                            continue
                        if normalized in names_seen:
                            continue
                        names_seen.add(normalized)
                        names.append(normalized)
                    if len(names) >= target_total:
                        break
                report['models_used'] = used_counts

                if report['status'] == 'running':
                    if names:
                        report['status'] = 'ok'
                    elif report['errors']:
                        report['status'] = 'empty_with_errors'
                    else:
                        report['status'] = 'empty'
    elif args.llm_provider == 'hybrid':
        local_provider = 'openai_compat'
        remote_provider = 'openrouter_http'
        hybrid_local_share = clamp_share(float(getattr(args, 'llm_hybrid_local_share', 0.75)))
        report['model_source'] = 'hybrid'
        report['hybrid_local_share'] = hybrid_local_share
        report['hybrid_providers'] = [local_provider, remote_provider]
        requested_by_provider: dict[str, list[str]] = {}
        source_by_provider: dict[str, str] = {}
        provider_contexts: dict[str, dict[str, Any]] = {}

        openai_request_extras: dict[str, Any] = {}
        ttl_s = max(0, int(getattr(args, 'llm_openai_ttl_s', 0)))
        if ttl_s > 0:
            openai_request_extras['ttl'] = ttl_s
        keep_alive_raw = str(getattr(args, 'llm_openai_keep_alive', '') or '').strip()
        if keep_alive_raw:
            keep_alive_value: Any = keep_alive_raw
            if keep_alive_raw.lstrip('-').isdigit():
                keep_alive_value = int(keep_alive_raw)
            openai_request_extras['keep_alive'] = keep_alive_value
        if openai_request_extras:
            report['openai_request_extras'] = dict(openai_request_extras)

        local_models, local_source, local_err = resolve_llm_models_for_provider(
            args=args,
            provider=local_provider,
            cli_models_raw=str(getattr(args, 'llm_hybrid_local_models', '') or ''),
            fallback_model_raw=str(getattr(args, 'llm_hybrid_local_model', '') or ''),
        )
        requested_by_provider[local_provider] = list(local_models)
        source_by_provider[local_provider] = local_source
        if local_err:
            report['errors'].append(f'{local_provider}:{local_err}')
        else:
            local_api_key = os.environ.get(args.llm_openai_api_key_env, '').strip() or 'ollama'
            local_catalog = nide.list_openai_models(
                api_key=local_api_key,
                base_url=args.llm_openai_base_url,
                timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
            )
            if local_catalog is not None:
                local_unavailable = [model for model in local_models if model not in local_catalog]
                if local_unavailable:
                    report['errors'].append(f'{local_provider}:models_not_in_catalog={",".join(local_unavailable)}')
                    local_models = [model for model in local_models if model in local_catalog]
                    requested_by_provider[local_provider] = list(local_models)
            if local_models:

                def call_local(_prompt: str, _model: str) -> tuple[list[str], dict[str, Any], str]:
                    return nide.call_openai_compat_candidates(
                        api_key=local_api_key,
                        base_url=args.llm_openai_base_url,
                        model=_model,
                        prompt=_prompt,
                        timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
                        strict_json=bool(args.llm_strict_json),
                        request_extras=openai_request_extras,
                    )

                provider_contexts[local_provider] = {
                    'models': list(local_models),
                    'call_provider': call_local,
                    'source_label': 'openai_compat',
                    'base_url': nide.normalize_openai_compat_base_url(args.llm_openai_base_url),
                }
            else:
                report['errors'].append(f'{local_provider}:model_not_configured')

        remote_models, remote_source, remote_err = resolve_llm_models_for_provider(
            args=args,
            provider=remote_provider,
            cli_models_raw=str(getattr(args, 'llm_hybrid_remote_models', '') or ''),
            fallback_model_raw=str(getattr(args, 'llm_hybrid_remote_model', '') or ''),
        )
        requested_by_provider[remote_provider] = list(remote_models)
        source_by_provider[remote_provider] = remote_source
        if remote_err:
            report['errors'].append(f'{remote_provider}:{remote_err}')
        else:
            remote_api_key = os.environ.get(args.llm_api_key_env, '').strip()
            if not remote_api_key:
                report['errors'].append(f'{remote_provider}:missing env {args.llm_api_key_env}')
            else:
                remote_catalog = nide.list_openrouter_models(
                    api_key=remote_api_key,
                    timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
                )
                if remote_catalog is not None:
                    remote_unavailable = [model for model in remote_models if model not in remote_catalog]
                    if remote_unavailable:
                        report['errors'].append(f'{remote_provider}:models_not_in_catalog={",".join(remote_unavailable)}')
                        remote_models = [model for model in remote_models if model in remote_catalog]
                        requested_by_provider[remote_provider] = list(remote_models)
                if remote_models:

                    def call_remote(_prompt: str, _model: str) -> tuple[list[str], dict[str, Any], str]:
                        return nide.call_openrouter_candidates(
                            api_key=remote_api_key,
                            model=_model,
                            prompt=_prompt,
                            timeout_ms=max(1000, int(args.llm_max_call_latency_ms)),
                            strict_json=bool(args.llm_strict_json),
                            http_referer=args.llm_openrouter_http_referer,
                            x_title=args.llm_openrouter_x_title,
                        )

                    provider_contexts[remote_provider] = {
                        'models': list(remote_models),
                        'call_provider': call_remote,
                        'source_label': 'openrouter',
                        'base_url': 'https://openrouter.ai/api/v1',
                    }
                else:
                    report['errors'].append(f'{remote_provider}:model_not_configured')

        report['models_requested_by_provider'] = requested_by_provider
        report['model_source_by_provider'] = source_by_provider
        flattened_requested: list[str] = []
        for provider in [local_provider, remote_provider]:
            models_for_provider = requested_by_provider.get(provider, [])
            for model in models_for_provider:
                flattened_requested.append(f'{provider}:{model}')
        report['models_requested'] = flattened_requested
        report['model'] = '|'.join(flattened_requested)

        total_rounds = max(1, int(args.llm_rounds))
        local_round_target = 0
        remote_round_target = 0
        if local_provider in provider_contexts and remote_provider in provider_contexts:
            local_round_target = int(round(float(total_rounds) * hybrid_local_share))
            local_round_target = max(1, min(total_rounds - 1, local_round_target))
            remote_round_target = total_rounds - local_round_target
        elif local_provider in provider_contexts:
            local_round_target = total_rounds
        elif remote_provider in provider_contexts:
            remote_round_target = total_rounds
        else:
            report['status'] = 'hybrid_unavailable'

        round_schedule = build_hybrid_provider_round_schedule(
            total_rounds=total_rounds,
            local_rounds=local_round_target,
            remote_rounds=remote_round_target,
        )
        report['hybrid_round_targets'] = {
            local_provider: local_round_target,
            remote_provider: remote_round_target,
        }
        report['hybrid_round_schedule'] = list(round_schedule)

        if report['status'] != 'hybrid_unavailable':
            report['status'] = 'running' if round_schedule else 'hybrid_unavailable'

        if report['status'] == 'running':
            used_counts_by_provider: dict[str, dict[str, int]] = {}
            used_counts_flat: dict[str, int] = {}
            provider_round_offsets: dict[str, int] = {
                local_provider: 0,
                remote_provider: 0,
            }
            model_rng_seed = (int(getattr(args, 'ab_seed', 0)) * 1000003) + int(run_index)
            model_rng = random.Random(model_rng_seed)
            for round_idx, provider_name in enumerate(round_schedule):
                attempted_rounds += 1
                elapsed_ms = int((time.monotonic() - stage_started) * 1000)
                if elapsed_ms >= max(1000, int(args.llm_stage_timeout_ms)):
                    report['status'] = 'stage_timeout'
                    timeout_rounds += 1
                    break
                if args.llm_max_usd_per_run > 0:
                    projected = total_cost + max(last_call_cost, 0.0)
                    if projected > float(args.llm_max_usd_per_run):
                        report['status'] = 'budget_stop'
                        break

                context = provider_contexts.get(provider_name)
                if context is None:
                    report['errors'].append(f'round={round_idx + 1}:{provider_name}:provider_unavailable')
                    empty_rounds += 1
                    continue

                provider_models = [str(item) for item in context.get('models', []) if str(item)]
                provider_round_idx = int(provider_round_offsets.get(provider_name, 0))
                selected_model = select_round_model(
                    models=provider_models,
                    round_idx=provider_round_idx,
                    selection=str(getattr(args, 'llm_model_selection', 'round_robin')),
                    rng=model_rng,
                )
                provider_round_offsets[provider_name] = provider_round_idx + 1
                if not selected_model:
                    report['errors'].append(f'round={round_idx + 1}:{provider_name}:model_not_configured')
                    empty_rounds += 1
                    continue

                provider_used = used_counts_by_provider.setdefault(provider_name, {})
                provider_used[selected_model] = int(provider_used.get(selected_model, 0)) + 1
                flat_key = f'{provider_name}:{selected_model}'
                used_counts_flat[flat_key] = int(used_counts_flat.get(flat_key, 0)) + 1

                prompt, mode = nide.build_prompt(
                    scope=scope,
                    round_index=((run_index - 1) * max(1, int(args.llm_rounds))) + round_idx,
                    target_count=max(1, int(args.llm_candidates_per_round)),
                    constraints=constraints,
                    context_packet=context_packet,
                    prompt_template=prompt_template,
                )
                mode_key = ':'.join(mode)
                cache_key_blob = json.dumps(
                    {
                        'provider': provider_name,
                        'base_url': str(context.get('base_url', '') or ''),
                        'model': selected_model,
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
                    if round_names:
                        successful_rounds += 1
                    else:
                        empty_rounds += 1
                        report['errors'].append(f'round={round_idx + 1}:{provider_name}:cache_empty')
                    report['cache_hits'] = int(report['cache_hits']) + 1
                    append_log(
                        f'round={round_idx + 1} provider={provider_name} mode={mode_key} model={selected_model} '
                        f'source=cache names={len(round_names)}'
                    )
                else:
                    retries = max(0, int(args.llm_max_retries))
                    usage: dict[str, Any] = {}
                    err = 'unknown'
                    call_provider_fn = context['call_provider']
                    for attempt in range(retries + 1):
                        call_names, usage, err = call_provider_fn(prompt, selected_model)
                        if call_names:
                            round_names = call_names
                            successful_rounds += 1
                            call_cost = nide.estimate_usage_cost_usd(
                                usage=usage,
                                in_price_per_1k=max(0.0, float(args.llm_pricing_input_per_1k)),
                                out_price_per_1k=max(0.0, float(args.llm_pricing_output_per_1k)),
                            )
                            total_cost += call_cost
                            last_call_cost = call_cost
                            append_log(
                                f'round={round_idx + 1} provider={provider_name} mode={mode_key} model={selected_model} '
                                f'source={context.get("source_label", provider_name)} names={len(round_names)} usage={usage} '
                                f'cost_usd={call_cost:.6f}'
                            )
                            if args.llm_cache_dir and round_names:
                                store_cached_candidates(
                                    cache_dir=cache_dir,
                                    cache_key=cache_key,
                                    candidates=round_names,
                                    meta={'usage': usage, 'mode': mode, 'provider': provider_name},
                                )
                            break
                        retriable = err in {'timeout', 'network_error', 'http_429', 'http_500', 'http_502', 'http_503'}
                        if retriable and attempt < retries:
                            report['retries'] = int(report['retries']) + 1
                            sleep_s = (0.35 * (attempt + 1)) + random.uniform(0.0, 0.25)
                            append_log(
                                f'round={round_idx + 1} provider={provider_name} model={selected_model} '
                                f'retry={attempt + 1} err={err} sleep_s={sleep_s:.2f}'
                            )
                            time.sleep(sleep_s)
                            continue
                        if err == 'timeout':
                            timeout_rounds += 1
                        else:
                            empty_rounds += 1
                        report['errors'].append(f'round={round_idx + 1}:{provider_name}:{err}')
                        append_log(
                            f'round={round_idx + 1} provider={provider_name} model={selected_model} err={err}'
                        )
                        break

                for name in round_names:
                    normalized = nide.normalize_alpha_name(name)
                    if not nide.is_valid_candidate_name(normalized):
                        continue
                    if normalized in names_seen:
                        continue
                    names_seen.add(normalized)
                    names.append(normalized)
                if len(names) >= target_total:
                    break

            report['models_used'] = used_counts_flat
            report['models_used_by_provider'] = {
                provider: counts for provider, counts in used_counts_by_provider.items() if counts
            }
            report['hybrid_rounds_executed_by_provider'] = {
                provider: int(provider_round_offsets.get(provider, 0))
                for provider in [local_provider, remote_provider]
            }
            if report['status'] == 'running':
                if names:
                    report['status'] = 'ok'
                elif report['errors']:
                    report['status'] = 'empty_with_errors'
                else:
                    report['status'] = 'empty'
    else:
        report['status'] = 'unsupported_provider'
        report['errors'].append(f'provider={args.llm_provider}')

    names = names[:target_total]
    report['candidate_count'] = len(names)
    report['cost_usd'] = round(total_cost, 6)
    slo = nide.evaluate_ideation_slo(
        attempted_rounds=attempted_rounds,
        successful_rounds=successful_rounds,
        timeout_rounds=timeout_rounds,
        empty_rounds=empty_rounds,
        min_success_rate=max(0.0, min(1.0, float(args.llm_slo_min_success_rate))),
        max_timeout_rate=max(0.0, min(1.0, float(args.llm_slo_max_timeout_rate))),
        max_empty_rate=max(0.0, min(1.0, float(args.llm_slo_max_empty_rate))),
        min_samples=max(1, int(args.llm_slo_min_samples)),
    )
    report['slo'] = slo
    if slo.get('status') == 'breach':
        report['errors'].append(f'slo_breach:{",".join(slo.get("breaches", []))}')
        if not bool(args.llm_slo_fail_open):
            report['status'] = 'slo_breach'
    if not names:
        return None, report

    artifact_path = runs_dir / f'{run_id}_llm_candidates.json'
    artifact_payload = {
        'candidates': [{'name': item} for item in names],
        'metadata': {
            'provider': args.llm_provider,
            'model': report.get('model', ''),
            'models_requested': report.get('models_requested', []),
            'models_requested_by_provider': report.get('models_requested_by_provider', {}),
            'models_used': report.get('models_used', {}),
            'models_used_by_provider': report.get('models_used_by_provider', {}),
            'model_selection': report.get('model_selection', ''),
            'model_source': report.get('model_source', ''),
            'model_source_by_provider': report.get('model_source_by_provider', {}),
            'hybrid_round_targets': report.get('hybrid_round_targets', {}),
            'hybrid_round_schedule': report.get('hybrid_round_schedule', []),
            'prompt_template_path': report.get('prompt_template_path', ''),
            'run_id': run_id,
            'constraints_path': str(constraints_path),
            'status': report['status'],
            'cost_usd': report['cost_usd'],
            'context_hash': context_hash,
            'slo_status': slo.get('status'),
            'slo_breaches': slo.get('breaches', []),
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
        '--generator-only-llm-candidates',
        dest='generator_only_llm_candidates',
        action='store_true',
        default=False,
        help='Run generator in --only-candidates mode so only LLM artifact names are screened.',
    )
    parser.add_argument('--no-generator-only-llm-candidates', dest='generator_only_llm_candidates', action='store_false')
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
        '--validator-min-concurrency',
        type=int,
        default=2,
        help='Lower bound for adaptive validator concurrency.',
    )
    parser.add_argument(
        '--validator-max-concurrency',
        type=int,
        default=24,
        help='Upper bound for adaptive validator concurrency.',
    )
    parser.add_argument(
        '--validator-timeout-s',
        type=float,
        default=8.0,
        help='Per-check timeout seconds passed to async validator.',
    )
    parser.add_argument(
        '--validator-state-filter',
        default='new',
        help='Candidate states for async validator (comma-separated). Use "new,checked" to revalidate old names.',
    )
    parser.add_argument(
        '--validator-memory-db',
        default='test_outputs/branding/naming_exclusion_memory.db',
        help='Optional persistent exclusion-memory DB path passed to naming_validate_async.',
    )
    parser.add_argument(
        '--validator-memory-ttl-days',
        type=int,
        default=180,
        help='TTL in days for exclusion memory entries written by validator.',
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
        '--heartbeat-events',
        dest='heartbeat_events',
        action='store_true',
        default=True,
        help='Emit campaign_event lines and append them to heartbeat JSONL.',
    )
    parser.add_argument('--no-heartbeat-events', dest='heartbeat_events', action='store_false')
    parser.add_argument(
        '--heartbeat-jsonl',
        default='',
        help='Optional heartbeat JSONL path (defaults to <out-dir>/runs/campaign_heartbeat.jsonl).',
    )
    parser.add_argument(
        '--heartbeat-interval-s',
        type=float,
        default=10.0,
        help='Interval for stage_heartbeat events while long-running child stages execute.',
    )
    parser.add_argument(
        '--live-progress',
        dest='live_progress',
        action='store_true',
        default=True,
        help='Stream selected generator/validator child progress lines to stdout while keeping full logs on disk.',
    )
    parser.add_argument('--no-live-progress', dest='live_progress', action='store_false')
    parser.add_argument(
        '--live-progress-patterns',
        default='stage_event=,async_validation_,run_summary=,cheap_gate_dropped',
        help='Comma-separated substrings used to select child process lines for live streaming.',
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
        choices=['openrouter_http', 'openai_compat', 'hybrid', 'pal', 'fixture'],
        default='openrouter_http',
        help='LLM provider mode for ideation stage.',
    )
    parser.add_argument(
        '--llm-model',
        default=os.environ.get('LLM_MODEL', ''),
        help='Single LLM model identifier fallback (used when --llm-models and --llm-model-config are not set).',
    )
    parser.add_argument(
        '--llm-models',
        default='',
        help='Comma-separated model identifiers; rotates per round for openrouter_http/openai_compat providers.',
    )
    parser.add_argument(
        '--llm-model-config',
        default=os.environ.get('LLM_MODEL_CONFIG', ''),
        help='Path to provider-aware model config file (.json/.toml/.txt).',
    )
    parser.add_argument(
        '--llm-model-selection',
        choices=['round_robin', 'random'],
        default='round_robin',
        help='Model selection strategy when multiple models are configured.',
    )
    parser.add_argument(
        '--llm-api-key-env',
        default='OPENROUTER_API_KEY',
        help='Environment variable name containing API key for openrouter_http mode.',
    )
    parser.add_argument(
        '--llm-openai-api-key-env',
        default='OPENAI_API_KEY',
        help='Environment variable name containing API key for openai_compat mode (optional for local runtimes).',
    )
    parser.add_argument(
        '--llm-openai-base-url',
        default=os.environ.get('OPENAI_COMPAT_BASE_URL', 'http://localhost:11434/v1'),
        help='Base URL for openai_compat mode (for example http://localhost:11434/v1).',
    )
    parser.add_argument(
        '--llm-openai-ttl-s',
        type=int,
        default=0,
        help='Optional request TTL seconds for openai_compat runtimes that support model residency (for example LM Studio).',
    )
    parser.add_argument(
        '--llm-openai-keep-alive',
        default='',
        help='Optional keep_alive value for openai_compat runtimes that accept it (for example Ollama native adapters).',
    )
    parser.add_argument(
        '--llm-hybrid-local-share',
        type=float,
        default=float(os.environ.get('LLM_HYBRID_LOCAL_SHARE', '0.75')),
        help='Local share for hybrid provider rounds (0-1); remainder uses openrouter_http.',
    )
    parser.add_argument(
        '--llm-hybrid-local-model',
        default=os.environ.get('LLM_HYBRID_LOCAL_MODEL', ''),
        help='Single-model fallback for hybrid local provider (openai_compat).',
    )
    parser.add_argument(
        '--llm-hybrid-local-models',
        default=os.environ.get('LLM_HYBRID_LOCAL_MODELS', ''),
        help='Comma-separated models for hybrid local provider (openai_compat).',
    )
    parser.add_argument(
        '--llm-hybrid-remote-model',
        default=os.environ.get('LLM_HYBRID_REMOTE_MODEL', ''),
        help='Single-model fallback for hybrid remote provider (openrouter_http).',
    )
    parser.add_argument(
        '--llm-hybrid-remote-models',
        default=os.environ.get('LLM_HYBRID_REMOTE_MODELS', ''),
        help='Comma-separated models for hybrid remote provider (openrouter_http).',
    )
    parser.add_argument(
        '--llm-openrouter-http-referer',
        default=os.environ.get('OPENROUTER_HTTP_REFERER', ''),
        help='Optional HTTP-Referer header for OpenRouter attribution.',
    )
    parser.add_argument(
        '--llm-openrouter-x-title',
        default=os.environ.get('OPENROUTER_X_TITLE', 'Kostula Naming Pipeline'),
        help='Optional X-Title header for OpenRouter attribution.',
    )
    parser.add_argument('--llm-rounds', type=int, default=2, help='LLM ideation rounds per run.')
    parser.add_argument('--llm-candidates-per-round', type=int, default=20, help='Candidates requested per LLM round.')
    parser.add_argument(
        '--llm-prompt-template-file',
        default=os.environ.get('LLM_PROMPT_TEMPLATE_FILE', ''),
        help='Optional text template file for ideation prompt placeholders ({scope},{round_index},{target_count},{phonetic},{morphology},{semantic},{banned_tokens},{banned_prefixes},{context_block}).',
    )
    parser.add_argument('--llm-max-call-latency-ms', type=int, default=8000, help='Per-call timeout in milliseconds.')
    parser.add_argument('--llm-stage-timeout-ms', type=int, default=30000, help='Total ideation stage timeout in milliseconds.')
    parser.add_argument('--llm-max-retries', type=int, default=3, help='Max retries for retriable LLM call errors.')
    parser.add_argument('--llm-max-usd-per-run', type=float, default=0.0, help='Budget cap for LLM calls (0 disables cap).')
    parser.add_argument('--llm-pricing-input-per-1k', type=float, default=0.0, help='Estimated input token price USD per 1k.')
    parser.add_argument('--llm-pricing-output-per-1k', type=float, default=0.0, help='Estimated output token price USD per 1k.')
    parser.add_argument(
        '--llm-slo-min-success-rate',
        type=float,
        default=0.60,
        help='Minimum successful-round ratio expected for ideation stage (0-1).',
    )
    parser.add_argument(
        '--llm-slo-max-timeout-rate',
        type=float,
        default=0.35,
        help='Maximum timeout-round ratio tolerated for ideation stage (0-1).',
    )
    parser.add_argument(
        '--llm-slo-max-empty-rate',
        type=float,
        default=0.40,
        help='Maximum empty/error-round ratio tolerated for ideation stage (0-1).',
    )
    parser.add_argument(
        '--llm-slo-min-samples',
        type=int,
        default=1,
        help='Minimum round samples required before SLO pass/breach judgment.',
    )
    parser.add_argument(
        '--llm-slo-fail-open',
        dest='llm_slo_fail_open',
        action='store_true',
        default=True,
        help='Keep run deterministic fail-open when ideation SLO is breached.',
    )
    parser.add_argument('--no-llm-slo-fail-open', dest='llm_slo_fail_open', action='store_false')
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
        '--reset-db',
        dest='reset_db',
        action='store_true',
        default=False,
        help='Delete existing campaign DB before ingest/bootstrap.',
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
        '--shard-db-isolation',
        dest='shard_db_isolation',
        action='store_true',
        default=True,
        help='Use per-shard DB files when shard-count > 1 to avoid cross-shard write contention.',
    )
    parser.add_argument('--no-shard-db-isolation', dest='shard_db_isolation', action='store_false')
    parser.add_argument(
        '--merge-shards',
        dest='merge_shards',
        action='store_true',
        default=True,
        help='Merge shard DBs into main DB at campaign end (applies to shard 0 only).',
    )
    parser.add_argument('--no-merge-shards', dest='merge_shards', action='store_false')
    parser.add_argument('--shard-id', type=int, default=0, help='0-based shard id for parallel campaign workers.')
    parser.add_argument('--shard-count', type=int, default=1, help='Total number of shard workers.')
    parser.add_argument(
        '--shard-scheduling',
        choices=['slice', 'weighted'],
        default='slice',
        help='Shard assignment strategy for sweep combos (weighted uses historical duration estimates).',
    )
    parser.add_argument(
        '--shard-history-progress-csv',
        default='',
        help='Optional campaign_progress.csv used for weighted shard scheduling history.',
    )
    parser.add_argument(
        '--shard-weight-fallback-s',
        type=float,
        default=180.0,
        help='Fallback duration estimate in seconds for weighted scheduling combos with no history.',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    stamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')

    out_dir = Path(args.out_dir).expanduser() if args.out_dir else root / 'test_outputs' / 'branding' / f'naming_campaign_{stamp}'
    db_path_main = Path(args.db).expanduser() if args.db else out_dir / 'naming_campaign.db'

    logs_dir = out_dir / 'logs'
    runs_dir = out_dir / 'runs'
    progress_csv = out_dir / 'campaign_progress.csv'
    seen_names_path = out_dir / 'seen_shortlist_names.txt'
    campaign_summary_path = out_dir / 'campaign_summary.json'
    heartbeat_path = Path(args.heartbeat_jsonl).expanduser() if args.heartbeat_jsonl else runs_dir / 'campaign_heartbeat.jsonl'

    out_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    shares = [clamp_share(float(part)) for part in parse_csv_list(args.source_influence_shares)]
    scopes = parse_csv_list(args.scopes)
    gates = parse_csv_list(args.gates)
    live_patterns = parse_csv_list(args.live_progress_patterns) if args.live_progress else []
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
    shard_db_isolation_enabled = bool(args.shard_db_isolation and args.shard_count > 1)
    db_path_worker = (
        shard_db_path(db_path_main, args.shard_id)
        if shard_db_isolation_enabled
        else db_path_main
    )
    merge_shards_enabled = bool(
        args.merge_shards
        and shard_db_isolation_enabled
        and args.shard_id == 0
        and args.shard_count > 1
    )
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
    if args.generator_only_llm_candidates and not args.llm_ideation_enabled:
        print('Invalid config: --generator-only-llm-candidates requires --llm-ideation-enabled.')
        return 1

    sweep_combos: list[SweepCombo] = []
    for quota_profile in quota_profiles:
        for gate in gates:
            for scope in scopes:
                for share in shares:
                    sweep_combos.append((share, scope, gate, quota_profile))
    shard_history_csv: Path | None = None
    if str(args.shard_history_progress_csv).strip():
        shard_history_csv = Path(str(args.shard_history_progress_csv).strip()).expanduser()
    elif progress_csv.exists():
        shard_history_csv = progress_csv
    shard_history_seconds = (
        load_combo_duration_history(shard_history_csv)
        if shard_history_csv is not None
        else {}
    )
    shard_assignments, shard_schedule_meta = assign_sweep_combos_to_shards(
        sweep_combos=sweep_combos,
        shard_count=int(args.shard_count),
        scheduling=str(args.shard_scheduling),
        history_seconds_by_combo=shard_history_seconds,
        fallback_duration_s=float(args.shard_weight_fallback_s),
    )
    shard_combos = shard_assignments[args.shard_id]
    if not shard_combos:
        print(
            'Invalid shard configuration: no sweep combinations assigned '
            f'to shard_id={args.shard_id} with shard_count={args.shard_count}.'
        )
        return 1

    print(f'campaign_start out_dir={out_dir}')
    emit_campaign_event(
        enabled=bool(args.heartbeat_events),
        heartbeat_path=heartbeat_path,
        event='campaign_start',
        out_dir=str(out_dir),
        shard_id=int(args.shard_id),
        shard_count=int(args.shard_count),
        shard_scheduling=str(shard_schedule_meta.get('mode') or args.shard_scheduling),
        shard_predicted_load_s=shard_schedule_meta.get('predicted_load_s', []),
    )
    print(
        f'campaign_config hours={args.hours} max_runs={args.max_runs} sleep_s={args.sleep_s} '
        f'shares={shares} scopes={scopes} gates={gates} quota_profiles={len(quota_profiles)} '
        f'shard={args.shard_id + 1}/{args.shard_count} shard_combo_count={len(shard_combos)} '
        f'shard_scheduling={shard_schedule_meta.get("mode")} '
        f'shard_history_path={shard_history_csv or ""} '
        f'shard_history_matches={shard_schedule_meta.get("history_matches", 0)} '
        f'shard_predicted_load_s={shard_schedule_meta.get("predicted_load_s", [])} '
        f'shard_db_isolation={shard_db_isolation_enabled} db_main={db_path_main} db_worker={db_path_worker} '
        f'merge_shards={merge_shards_enabled} '
        f'check_limit={args.check_limit} validator_tier={args.validator_tier} '
        f'validator_candidate_limit={args.validator_candidate_limit} validator_state_filter={args.validator_state_filter} '
        f'validator_concurrency={int(args.validator_concurrency)} '
        f'validator_min_concurrency={int(args.validator_min_concurrency)} '
        f'validator_max_concurrency={int(args.validator_max_concurrency)} '
        f'validator_timeout_s={float(args.validator_timeout_s):.2f} '
        f'validator_memory_enabled={bool(str(args.validator_memory_db).strip())} '
        f'validator_memory_ttl_days={int(args.validator_memory_ttl_days)} '
        f'reset_db={args.reset_db} '
        f'llm_enabled={args.llm_ideation_enabled} llm_provider={args.llm_provider} '
        f'llm_model={args.llm_model} llm_models={args.llm_models} '
        f'llm_hybrid_local_share={float(args.llm_hybrid_local_share):.2f} '
        f'llm_hybrid_local_model={args.llm_hybrid_local_model} llm_hybrid_local_models={args.llm_hybrid_local_models} '
        f'llm_hybrid_remote_model={args.llm_hybrid_remote_model} llm_hybrid_remote_models={args.llm_hybrid_remote_models} '
        f'llm_model_config={args.llm_model_config} llm_model_selection={args.llm_model_selection} '
        f'llm_prompt_template_file={args.llm_prompt_template_file} '
        f'llm_base_url={nide.normalize_openai_compat_base_url(args.llm_openai_base_url) if args.llm_provider in {"openai_compat", "hybrid"} else ""} '
        f'llm_slo_min_success_rate={float(args.llm_slo_min_success_rate):.2f} '
        f'llm_slo_max_timeout_rate={float(args.llm_slo_max_timeout_rate):.2f} '
        f'llm_slo_max_empty_rate={float(args.llm_slo_max_empty_rate):.2f} '
        f'llm_slo_fail_open={bool(args.llm_slo_fail_open)} '
        f'generator_only_llm={args.generator_only_llm_candidates} '
        f'heartbeat_events={args.heartbeat_events} heartbeat_path={heartbeat_path} '
        f'heartbeat_interval_s={args.heartbeat_interval_s} '
        f'live_progress={args.live_progress} live_patterns={live_patterns} '
        f'llm_context_enabled={bool(llm_context_packet)} '
        f'llm_attribution_headers={bool(str(args.llm_openrouter_http_referer).strip() or str(args.llm_openrouter_x_title).strip())}'
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
        emit_campaign_event(
            enabled=bool(args.heartbeat_events),
            heartbeat_path=heartbeat_path,
            event='campaign_lock_blocked',
            out_dir=str(out_dir),
            shard_id=int(args.shard_id),
            shard_count=int(args.shard_count),
            reason=lock_error,
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

    if args.reset_db and shard_db_isolation_enabled and args.shard_id == 0 and db_path_main.exists():
        db_path_main.unlink()
        print(f'db_reset_main path={db_path_main}')

    if db_path_worker.exists():
        if args.reset_db:
            db_path_worker.unlink()
            print(f'db_reset path={db_path_worker}')
        else:
            print(f'db_reuse path={db_path_worker}')

    ingest_curated_log = logs_dir / 'ingest_curated.log'
    ingest_curated_cmd = [
        'python3',
        str(root / 'scripts' / 'branding' / 'name_input_ingest.py'),
        '--db',
        str(db_path_worker),
        '--inputs',
        str(bpaths.SOURCE_INPUTS_V2),
        '--source-label',
        'curated_lexicon_v2',
        '--scope',
        'global',
        '--gate',
        'balanced',
        '--derive-morphology',
        '--morph-confidence-scale',
        '0.72',
    ]
    if not args.generator_only_llm_candidates:
        ingest_curated_cmd.append('--also-candidates')
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
            str(db_path_worker),
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
    history_skip_total_count = 0
    history_skip_generated_total_count = 0
    history_skip_runs_count = 0
    ab_metrics: list[dict[str, float]] = []
    ab_arms = nide.build_ab_arms(max_runs=max(1, args.max_runs), seed=int(args.ab_seed), block_size=4) if args.ab_mode else []
    ab_report_paths: tuple[Path, Path] | None = None

    while run_count < max(1, args.max_runs) and time.monotonic() < deadline:
        run_count += 1
        run_started = time.monotonic()
        run_stamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        run_id = f'run_{run_count:03d}_{run_stamp}'
        emit_campaign_event(
            enabled=bool(args.heartbeat_events),
            heartbeat_path=heartbeat_path,
            event='run_start',
            run=run_count,
            run_id=run_id,
            shard_id=int(args.shard_id),
            shard_count=int(args.shard_count),
        )

        share, scope, gate, quota_profile = shard_combos[(run_count - 1) % len(shard_combos)]
        combo_key = combo_history_key(
            share=share,
            scope=scope,
            gate=gate,
            quota_profile=quota_profile,
        )
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
            f'share={share:.2f} scope={scope} gate={gate} '
            f'combo_key={combo_key}'
        )

        llm_artifact: Path | None = None
        history_skip_count = 0
        history_skip_generated_count = 0
        llm_report: dict[str, Any] = {
            'provider': args.llm_provider,
            'model': args.llm_model,
            'status': 'skipped',
            'candidate_count': 0,
            'cost_usd': 0.0,
            'slo': {
                'status': 'skipped',
                'success_rate': 0.0,
                'timeout_rate': 0.0,
                'empty_rate': 0.0,
                'breaches': [],
            },
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
        if args.generator_only_llm_candidates and llm_artifact is None:
            error_count += 1
            last_status = 'llm_candidates_missing'
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
                    'llm_slo_status': (llm_report.get('slo') or {}).get('status', 'skipped'),
                    'llm_slo_success_rate': (llm_report.get('slo') or {}).get('success_rate', 0.0),
                    'llm_slo_timeout_rate': (llm_report.get('slo') or {}).get('timeout_rate', 0.0),
                    'llm_slo_empty_rate': (llm_report.get('slo') or {}).get('empty_rate', 0.0),
                    'llm_slo_breaches': json.dumps((llm_report.get('slo') or {}).get('breaches', []), ensure_ascii=False),
                    'shard_id': args.shard_id,
                    'shard_count': args.shard_count,
                    'shard_combo_count': len(shard_combos),
                    'shard_scheduling': str(shard_schedule_meta.get('mode') or args.shard_scheduling),
                    'combo_key': combo_key,
                    'history_skip_count': history_skip_count,
                    'history_skip_generated_count': history_skip_generated_count,
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
            print(f'run_failed idx={run_count} stage=llm_artifact status={llm_report.get("status")}')
            if error_count >= max(1, args.max_errors):
                break
            if time.monotonic() < deadline:
                time.sleep(max(0, args.sleep_s))
            continue

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
            f'--source-pool-db={db_path_worker}',
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
            f'--db={db_path_worker}',
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
        if args.generator_only_llm_candidates:
            generator_cmd.append('--only-candidates')
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
        code = run_cmd(
            generator_cmd,
            cwd=root,
            log_path=gen_log,
            live_patterns=live_patterns,
            stage_label='generator',
            run_id=run_id,
            heartbeat_enabled=bool(args.heartbeat_events),
            heartbeat_path=heartbeat_path,
            heartbeat_interval_s=max(0.0, float(args.heartbeat_interval_s)),
        )
        history_skip_meta = extract_generator_history_skip(gen_log)
        history_skip_count = max(0, int(history_skip_meta.get('skipped_count') or 0))
        history_skip_generated_count = max(0, int(history_skip_meta.get('skipped_generated_count') or 0))
        history_skip_total_count += history_skip_count
        history_skip_generated_total_count += history_skip_generated_count
        if history_skip_count > 0:
            history_skip_runs_count += 1
            sample = history_skip_meta.get('skipped_names_sample', [])
            print(
                f'history_skip_count run={run_id} skipped={history_skip_count} '
                f'generated={history_skip_generated_count} sample={sample}',
                flush=True,
            )
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
                    'llm_slo_status': (llm_report.get('slo') or {}).get('status', 'skipped'),
                    'llm_slo_success_rate': (llm_report.get('slo') or {}).get('success_rate', 0.0),
                    'llm_slo_timeout_rate': (llm_report.get('slo') or {}).get('timeout_rate', 0.0),
                    'llm_slo_empty_rate': (llm_report.get('slo') or {}).get('empty_rate', 0.0),
                    'llm_slo_breaches': json.dumps((llm_report.get('slo') or {}).get('breaches', []), ensure_ascii=False),
                    'shard_id': args.shard_id,
                    'shard_count': args.shard_count,
                    'shard_combo_count': len(shard_combos),
                    'shard_scheduling': str(shard_schedule_meta.get('mode') or args.shard_scheduling),
                    'combo_key': combo_key,
                    'history_skip_count': history_skip_count,
                    'history_skip_generated_count': history_skip_generated_count,
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

        validator_runtime = derive_validator_runtime_settings(
            requested_concurrency=int(args.validator_concurrency),
            requested_min_concurrency=int(args.validator_min_concurrency),
            requested_max_concurrency=int(args.validator_max_concurrency),
            requested_timeout_s=float(args.validator_timeout_s),
        )
        validator_cmd = [
            'python3',
            str(root / 'scripts' / 'branding' / 'naming_validate_async.py'),
            '--db',
            str(db_path_worker),
            '--pipeline-version=v3',
            '--enable-v3',
            f'--state-filter={args.validator_state_filter}',
            f'--scope={scope}',
            f'--gate={gate}',
            f'--expensive-finalist-limit={max(1, int(args.validator_expensive_finalist_limit))}',
            '--finalist-recommendations=strong,consider',
            f'--checks={args.validator_checks}',
            f'--validation-tier={args.validator_tier}',
            f'--candidate-limit={max(1, int(args.validator_candidate_limit))}',
            f'--concurrency={int(validator_runtime["concurrency"])}',
            f'--min-concurrency={int(validator_runtime["min_concurrency"])}',
            f'--max-concurrency={int(validator_runtime["max_concurrency"])}',
            f'--timeout-s={float(validator_runtime["timeout_s"]):.2f}',
            f'--memory-ttl-days={max(1, int(args.validator_memory_ttl_days))}',
        ]
        if str(args.validator_memory_db).strip():
            validator_cmd.append(f'--memory-db={str(args.validator_memory_db).strip()}')
        code = run_cmd(
            validator_cmd,
            cwd=root,
            log_path=validator_log,
            live_patterns=live_patterns,
            stage_label='validator',
            run_id=run_id,
            heartbeat_enabled=bool(args.heartbeat_events),
            heartbeat_path=heartbeat_path,
            heartbeat_interval_s=max(0.0, float(args.heartbeat_interval_s)),
        )
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
                    'llm_slo_status': (llm_report.get('slo') or {}).get('status', 'skipped'),
                    'llm_slo_success_rate': (llm_report.get('slo') or {}).get('success_rate', 0.0),
                    'llm_slo_timeout_rate': (llm_report.get('slo') or {}).get('timeout_rate', 0.0),
                    'llm_slo_empty_rate': (llm_report.get('slo') or {}).get('empty_rate', 0.0),
                    'llm_slo_breaches': json.dumps((llm_report.get('slo') or {}).get('breaches', []), ensure_ascii=False),
                    'shard_id': args.shard_id,
                    'shard_count': args.shard_count,
                    'shard_combo_count': len(shard_combos),
                    'shard_scheduling': str(shard_schedule_meta.get('mode') or args.shard_scheduling),
                    'combo_key': combo_key,
                    'history_skip_count': history_skip_count,
                    'history_skip_generated_count': history_skip_generated_count,
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
            str(db_path_worker),
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
                'llm_slo_status': (llm_report.get('slo') or {}).get('status', 'skipped'),
                'llm_slo_success_rate': (llm_report.get('slo') or {}).get('success_rate', 0.0),
                'llm_slo_timeout_rate': (llm_report.get('slo') or {}).get('timeout_rate', 0.0),
                'llm_slo_empty_rate': (llm_report.get('slo') or {}).get('empty_rate', 0.0),
                'llm_slo_breaches': json.dumps((llm_report.get('slo') or {}).get('breaches', []), ensure_ascii=False),
                'shard_id': args.shard_id,
                'shard_count': args.shard_count,
                'shard_combo_count': len(shard_combos),
                'shard_scheduling': str(shard_schedule_meta.get('mode') or args.shard_scheduling),
                'combo_key': combo_key,
                'history_skip_count': history_skip_count,
                'history_skip_generated_count': history_skip_generated_count,
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
            f'history_skip={history_skip_count} history_skip_generated={history_skip_generated_count} '
            f'remaining_s={remaining_s} shard={args.shard_id + 1}/{args.shard_count}'
        )
        emit_campaign_event(
            enabled=bool(args.heartbeat_events),
            heartbeat_path=heartbeat_path,
            event='run_complete',
            run=run_count,
            run_id=run_id,
            status='ok',
            duration_s=duration_s,
            shortlist_count=len(shortlist),
            new_shortlist_count=len(new_names),
            validator_total_jobs=int(total_jobs),
            history_skip_count=int(history_skip_count),
            history_skip_generated_count=int(history_skip_generated_count),
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

    merge_summary: dict[str, Any] = {
        'status': 'skipped',
        'reason': 'disabled_or_not_primary_shard',
    }
    if merge_shards_enabled:
        shard_paths = [shard_db_path(db_path_main, shard_idx) for shard_idx in range(args.shard_count)]
        merge_summary = merge_shard_databases(
            target_db=db_path_main,
            shard_db_paths=shard_paths,
        )
        print(
            f'merge_shards_complete status={merge_summary.get("status")} '
            f'merged_candidates={merge_summary.get("merged_candidates", 0)} '
            f'merged_validation_results={merge_summary.get("merged_validation_results", 0)} '
            f'merged_candidate_scores={merge_summary.get("merged_candidate_scores", 0)}'
        )

    summary = {
        'finished_at': dt.datetime.now().isoformat(timespec='seconds'),
        'out_dir': str(out_dir),
        'db_main': str(db_path_main),
        'db_worker': str(db_path_worker),
        'shard_db_isolation': bool(shard_db_isolation_enabled),
        'merge_shards_enabled': bool(merge_shards_enabled),
        'merge_summary': merge_summary,
        'hours_budget': float(args.hours),
        'max_runs': int(args.max_runs),
        'shard_id': int(args.shard_id),
        'shard_count': int(args.shard_count),
        'shard_combo_count': int(len(shard_combos)),
        'shard_scheduling': str(shard_schedule_meta.get('mode') or args.shard_scheduling),
        'shard_scheduling_requested': str(args.shard_scheduling),
        'shard_scheduling_history_csv': str(shard_history_csv) if shard_history_csv is not None else '',
        'shard_history_entries': int(shard_schedule_meta.get('history_entries', 0)),
        'shard_history_matches': int(shard_schedule_meta.get('history_matches', 0)),
        'shard_predicted_load_s': shard_schedule_meta.get('predicted_load_s', []),
        'shard_assigned_combos': [
            {
                'share': round(float(combo[0]), 4),
                'scope': str(combo[1]),
                'gate': str(combo[2]),
                'quota_profile': str(combo[3]),
                'combo_key': combo_history_key(
                    share=float(combo[0]),
                    scope=str(combo[1]),
                    gate=str(combo[2]),
                    quota_profile=str(combo[3]),
                ),
            }
            for combo in shard_combos
        ],
        'runs_executed': int(run_count),
        'errors': int(error_count),
        'status': last_status,
        'history_skip_total_count': int(history_skip_total_count),
        'history_skip_generated_total_count': int(history_skip_generated_total_count),
        'history_skip_runs_count': int(history_skip_runs_count),
        'unique_shortlist_names': int(len(seen_shortlist)),
        'progress_csv': str(progress_csv),
        'seen_shortlist_names_path': str(seen_names_path),
        'ab_mode': bool(args.ab_mode),
        'ab_report_json': str(ab_report_paths[0]) if ab_report_paths else '',
        'ab_report_md': str(ab_report_paths[1]) if ab_report_paths else '',
        'heartbeat_jsonl': str(heartbeat_path),
    }
    campaign_summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    emit_campaign_event(
        enabled=bool(args.heartbeat_events),
        heartbeat_path=heartbeat_path,
        event='campaign_complete',
        status=last_status,
        runs_executed=int(run_count),
        errors=int(error_count),
        history_skip_total_count=int(history_skip_total_count),
        history_skip_generated_total_count=int(history_skip_generated_total_count),
        history_skip_runs_count=int(history_skip_runs_count),
        summary_path=str(campaign_summary_path),
    )
    print(f'campaign_complete summary={campaign_summary_path}')
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if error_count < max(1, args.max_errors) else 1


if __name__ == '__main__':
    raise SystemExit(main())
