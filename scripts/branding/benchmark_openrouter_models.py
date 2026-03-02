#!/usr/bin/env python3
"""Benchmark OpenRouter models for naming campaign quality/speed trade-offs."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / 'scripts/branding/naming_campaign_runner.py'
DEFAULT_PROMPT_TEMPLATE = (
    REPO_ROOT / 'resources/branding/llm/llm_prompt.constrained_pronounceable_de_en_v3.txt'
)

MODEL_BUNDLE_A = [
    'mistralai/mistral-small-creative',
    'qwen/qwen3-next-80b-a3b-instruct',
    'anthropic/claude-sonnet-4.6',
]
MODEL_BUNDLE_B = [
    'qwen/qwen3-next-80b-a3b-instruct',
    'anthropic/claude-sonnet-4.6',
]
MODEL_BUNDLE_C = [
    'mistralai/mistral-small-creative',
    'qwen/qwen3-next-80b-a3b-instruct',
    'openai/gpt-5.2',
]
MODEL_BUNDLE_FLASH = [
    'mistralai/mistral-small-creative',
    'qwen/qwen3.5-flash-02-23',
    'anthropic/claude-sonnet-4.6',
]


@dataclass
class BenchmarkResult:
    model: str
    out_dir: str
    return_code: int
    duration_s: float
    llm_stage_status: str
    llm_candidate_count: int
    shortlist_count: int
    new_shortlist_count: int
    llm_slo_status: str
    llm_slo_success_rate: float
    llm_slo_timeout_rate: float
    llm_slo_breaches: str
    status: str
    sample_names: list[str]
    log_path: str


def parse_model_list(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in str(raw or '').split(','):
        model = part.strip()
        if not model or model in seen:
            continue
        out.append(model)
        seen.add(model)
    return out


def model_slug(model: str) -> str:
    return str(model).replace('/', '__').replace(':', '__')


def parse_tail_names(path: Path) -> list[str]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return []
    candidates = payload.get('candidates')
    if not isinstance(candidates, list):
        return []
    out: list[str] = []
    for item in candidates:
        if isinstance(item, dict):
            name = str(item.get('name') or '').strip()
        else:
            name = str(item).strip()
        if name:
            out.append(name)
        if len(out) >= 8:
            break
    return out


def read_progress_metrics(out_dir: Path) -> dict[str, Any]:
    progress_path = out_dir / 'campaign_progress.csv'
    fallback = {
        'llm_stage_status': 'no_progress',
        'llm_candidate_count': 0,
        'shortlist_count': 0,
        'new_shortlist_count': 0,
        'llm_slo_status': 'na',
        'llm_slo_success_rate': 0.0,
        'llm_slo_timeout_rate': 0.0,
        'llm_slo_breaches': '',
        'status': 'no_progress',
    }
    if not progress_path.exists():
        return fallback
    try:
        with progress_path.open('r', encoding='utf-8', newline='') as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return fallback
    if not rows:
        return fallback
    row = rows[-1]

    def _to_int(raw: Any) -> int:
        try:
            return int(float(raw or 0))
        except (TypeError, ValueError):
            return 0

    def _to_float(raw: Any) -> float:
        try:
            return float(raw or 0.0)
        except (TypeError, ValueError):
            return 0.0

    return {
        'llm_stage_status': str(row.get('llm_stage_status') or ''),
        'llm_candidate_count': _to_int(row.get('llm_candidate_count')),
        'shortlist_count': _to_int(row.get('shortlist_count')),
        'new_shortlist_count': _to_int(row.get('new_shortlist_count')),
        'llm_slo_status': str(row.get('llm_slo_status') or ''),
        'llm_slo_success_rate': _to_float(row.get('llm_slo_success_rate')),
        'llm_slo_timeout_rate': _to_float(row.get('llm_slo_timeout_rate')),
        'llm_slo_breaches': str(row.get('llm_slo_breaches') or ''),
        'status': str(row.get('status') or ''),
    }


def read_campaign_summary(out_dir: Path) -> dict[str, Any]:
    summary_path = out_dir / 'campaign_summary.json'
    fallback = {'status': '', 'errors': 0}
    if not summary_path.exists():
        return fallback
    try:
        payload = json.loads(summary_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return fallback
    try:
        errors = int(payload.get('errors') or 0)
    except (TypeError, ValueError):
        errors = 0
    return {
        'status': str(payload.get('status') or ''),
        'errors': errors,
    }


def pick_bundle_models(bundle: str) -> list[str]:
    key = str(bundle or '').strip().lower()
    if key == 'a':
        return list(MODEL_BUNDLE_A)
    if key == 'b':
        return list(MODEL_BUNDLE_B)
    if key == 'c':
        return list(MODEL_BUNDLE_C)
    if key == 'flash':
        return list(MODEL_BUNDLE_FLASH)
    raise ValueError(f'unknown bundle: {bundle}')


def render_cmd(cmd: list[str]) -> str:
    return ' '.join(shlex.quote(part) for part in cmd)


def build_runner_cmd(args: argparse.Namespace, *, model: str, out_dir: Path) -> list[str]:
    cmd = [
        'python3',
        str(RUNNER_PATH),
        '--max-runs',
        str(args.max_runs),
        '--sleep-s',
        '0',
        '--no-mini-test',
        '--pool-size',
        str(args.pool_size),
        '--check-limit',
        str(args.check_limit),
        '--validator-tier',
        str(args.validator_tier),
        '--validator-candidate-limit',
        str(args.validator_candidate_limit),
        '--validator-expensive-finalist-limit',
        str(args.validator_expensive_finalist_limit),
        '--validator-timeout-s',
        str(args.validator_timeout_s),
        '--validator-concurrency',
        str(args.validator_concurrency),
        '--llm-ideation-enabled',
        '--llm-provider',
        'openrouter_http',
        '--llm-model',
        model,
        '--llm-rounds',
        str(args.llm_rounds),
        '--llm-candidates-per-round',
        str(args.llm_candidates_per_round),
        '--llm-max-call-latency-ms',
        str(args.llm_max_call_latency_ms),
        '--llm-stage-timeout-ms',
        str(args.llm_stage_timeout_ms),
        '--llm-max-retries',
        str(args.llm_max_retries),
        '--llm-max-usd-per-run',
        str(args.llm_max_usd_per_run),
        '--generator-only-llm-candidates',
        '--generator-no-external-checks',
        '--out-dir',
        str(out_dir),
        '--no-live-progress',
    ]
    if str(args.prompt_template).strip():
        cmd.extend(['--llm-prompt-template-file', str(args.prompt_template)])
    if args.extra_args:
        cmd.extend(args.extra_args)
    return cmd


def rank_key(result: BenchmarkResult) -> tuple[int, int, int, float]:
    status_rank = 1 if result.llm_stage_status == 'ok' else 0
    return (
        status_rank,
        int(result.new_shortlist_count),
        int(result.llm_candidate_count),
        -float(result.duration_s),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Benchmark OpenRouter models for naming campaign output quality/speed.')
    parser.add_argument(
        '--bundle',
        choices=['a', 'b', 'c', 'flash'],
        default='a',
        help='Preset model bundle (default: a).',
    )
    parser.add_argument(
        '--models',
        default='',
        help='Comma-separated explicit model list. Overrides --bundle when provided.',
    )
    parser.add_argument(
        '--out-dir',
        default=str(REPO_ROOT / 'test_outputs/branding/openrouter_model_benchmark'),
        help='Benchmark output directory.',
    )
    parser.add_argument('--max-runs', type=int, default=1)
    parser.add_argument('--pool-size', type=int, default=260)
    parser.add_argument('--check-limit', type=int, default=64)
    parser.add_argument('--validator-tier', default='cheap')
    parser.add_argument('--validator-candidate-limit', type=int, default=36)
    parser.add_argument('--validator-expensive-finalist-limit', type=int, default=12)
    parser.add_argument('--validator-timeout-s', type=float, default=8.0)
    parser.add_argument('--validator-concurrency', type=int, default=8)
    parser.add_argument('--llm-rounds', type=int, default=3)
    parser.add_argument('--llm-candidates-per-round', type=int, default=14)
    parser.add_argument('--llm-max-call-latency-ms', type=int, default=45_000)
    parser.add_argument('--llm-stage-timeout-ms', type=int, default=210_000)
    parser.add_argument('--llm-max-retries', type=int, default=1)
    parser.add_argument('--llm-max-usd-per-run', type=float, default=2.5)
    parser.add_argument(
        '--prompt-template',
        default=str(DEFAULT_PROMPT_TEMPLATE),
        help='LLM prompt template file path.',
    )
    parser.add_argument('--keep-existing', action='store_true', help='Do not delete per-model out dirs before running.')
    parser.add_argument('--dry-run', action='store_true', help='Print commands without running.')
    parser.add_argument(
        '--extra-arg',
        dest='extra_args',
        action='append',
        default=[],
        help='Extra arg forwarded to naming_campaign_runner.py (repeatable).',
    )
    parser.add_argument(
        '--demote-timeout-rate',
        type=float,
        default=0.35,
        help='Demote model from recommendation when timeout rate exceeds this value.',
    )
    return parser.parse_args()


def run_one(args: argparse.Namespace, *, model: str, root_out: Path) -> BenchmarkResult:
    model_out_dir = root_out / model_slug(model)
    log_path = model_out_dir / 'run.log'
    if model_out_dir.exists() and not args.keep_existing:
        for child in model_out_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
    model_out_dir.mkdir(parents=True, exist_ok=True)

    cmd = build_runner_cmd(args, model=model, out_dir=model_out_dir)
    if args.dry_run:
        print(f'dry_run model={model} cmd={render_cmd(cmd)}')
        return BenchmarkResult(
            model=model,
            out_dir=str(model_out_dir),
            return_code=0,
            duration_s=0.0,
            llm_stage_status='dry_run',
            llm_candidate_count=0,
            shortlist_count=0,
            new_shortlist_count=0,
            llm_slo_status='na',
            llm_slo_success_rate=0.0,
            llm_slo_timeout_rate=0.0,
            llm_slo_breaches='',
            status='dry_run',
            sample_names=[],
            log_path=str(log_path),
        )

    started = time.monotonic()
    with log_path.open('w', encoding='utf-8') as log_handle:
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=log_handle, stderr=subprocess.STDOUT, check=False)
    duration_s = round(time.monotonic() - started, 1)

    metrics = read_progress_metrics(model_out_dir)
    summary = read_campaign_summary(model_out_dir)
    llm_stage_status = str(metrics.get('llm_stage_status') or '')
    status = str(metrics.get('status') or '')
    summary_status = str(summary.get('status') or '')
    if llm_stage_status == 'no_progress' and summary_status:
        llm_stage_status = summary_status
    if (not status or status == 'no_progress') and summary_status:
        status = summary_status

    llm_candidate_files = sorted((model_out_dir / 'runs').glob('run_*_llm_candidates.json'))
    sample_names = parse_tail_names(llm_candidate_files[-1]) if llm_candidate_files else []
    return BenchmarkResult(
        model=model,
        out_dir=str(model_out_dir),
        return_code=int(proc.returncode),
        duration_s=float(duration_s),
        llm_stage_status=llm_stage_status,
        llm_candidate_count=int(metrics.get('llm_candidate_count') or 0),
        shortlist_count=int(metrics.get('shortlist_count') or 0),
        new_shortlist_count=int(metrics.get('new_shortlist_count') or 0),
        llm_slo_status=str(metrics.get('llm_slo_status') or ''),
        llm_slo_success_rate=float(metrics.get('llm_slo_success_rate') or 0.0),
        llm_slo_timeout_rate=float(metrics.get('llm_slo_timeout_rate') or 0.0),
        llm_slo_breaches=str(metrics.get('llm_slo_breaches') or ''),
        status=status,
        sample_names=sample_names,
        log_path=str(log_path),
    )


def write_outputs(root_out: Path, results: list[BenchmarkResult]) -> tuple[Path, Path]:
    root_out.mkdir(parents=True, exist_ok=True)
    tsv_path = root_out / 'results.tsv'
    json_path = root_out / 'results.json'

    headers = [
        'model',
        'return_code',
        'duration_s',
        'llm_stage_status',
        'llm_candidate_count',
        'shortlist_count',
        'new_shortlist_count',
        'llm_slo_status',
        'llm_slo_success_rate',
        'llm_slo_timeout_rate',
        'llm_slo_breaches',
        'status',
        'sample_names',
        'out_dir',
        'log_path',
    ]
    with tsv_path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, delimiter='\t')
        writer.writeheader()
        for result in results:
            row = asdict(result)
            row['sample_names'] = ','.join(result.sample_names)
            writer.writerow(row)

    payload = {'results': [asdict(item) for item in results]}
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    return tsv_path, json_path


def print_summary(results: list[BenchmarkResult]) -> None:
    ranked = sorted(results, key=rank_key, reverse=True)
    print(
        'model\tstatus\tllm_stage\tnew_shortlist\tllm_candidates\tduration_s\tslo\ttimeout_rate\trc',
        flush=True,
    )
    for item in ranked:
        print(
            '\t'.join(
                [
                    item.model,
                    item.status,
                    item.llm_stage_status,
                    str(item.new_shortlist_count),
                    str(item.llm_candidate_count),
                    f'{item.duration_s:.1f}',
                    item.llm_slo_status,
                    f'{item.llm_slo_timeout_rate:.2f}',
                    str(item.return_code),
                ]
            ),
            flush=True,
        )
    if ranked:
        best = ranked[0]
        print(
            f'best_model={best.model} new_shortlist={best.new_shortlist_count} '
            f'llm_candidates={best.llm_candidate_count} duration_s={best.duration_s:.1f}'
        )


def model_demote_reason(*, result: BenchmarkResult, timeout_threshold: float) -> str:
    if int(result.return_code) != 0:
        return 'nonzero_return_code'
    if str(result.llm_stage_status) == 'stage_timeout':
        return 'stage_timeout'
    if str(result.llm_stage_status) not in {'ok', 'dry_run'}:
        return f'stage_status={result.llm_stage_status}'
    if str(result.status) not in {'ok', 'completed', 'ok_llm_degraded_empty', 'dry_run'}:
        return f'status={result.status}'
    if float(result.llm_slo_timeout_rate) > float(timeout_threshold):
        return f'timeout_rate>{timeout_threshold:.2f}'
    return ''


def select_recommended_models(*, results: list[BenchmarkResult], timeout_threshold: float) -> tuple[list[str], dict[str, str]]:
    reasons: dict[str, str] = {}
    recommended: list[str] = []
    for item in sorted(results, key=rank_key, reverse=True):
        reason = model_demote_reason(result=item, timeout_threshold=timeout_threshold)
        if reason:
            reasons[item.model] = reason
            continue
        recommended.append(item.model)
    if recommended:
        return recommended, reasons
    if not results:
        return [], reasons
    best = sorted(results, key=rank_key, reverse=True)[0]
    recommended = [best.model]
    reasons[best.model] = 'forced_keep_no_models_passed_demote_rule'
    return recommended, reasons


def main() -> int:
    args = parse_args()
    if not os.environ.get('OPENROUTER_API_KEY', '').strip():
        print('OPENROUTER_API_KEY is not set. Run via: direnv exec . python3 ...', file=sys.stderr)
        return 2

    explicit_models = parse_model_list(args.models)
    models = explicit_models if explicit_models else pick_bundle_models(args.bundle)
    if not models:
        print('No models configured for benchmark.', file=sys.stderr)
        return 2

    out_dir = Path(args.out_dir).expanduser().resolve()
    results: list[BenchmarkResult] = []
    for model in models:
        print(f'benchmark_start model={model}', flush=True)
        result = run_one(args, model=model, root_out=out_dir)
        results.append(result)
        print(
            f'benchmark_done model={model} rc={result.return_code} duration_s={result.duration_s:.1f} '
            f'llm_stage_status={result.llm_stage_status} new_shortlist={result.new_shortlist_count}',
            flush=True,
        )

    tsv_path, json_path = write_outputs(out_dir, results)
    recommended_models, demote_reasons = select_recommended_models(
        results=results,
        timeout_threshold=max(0.0, float(args.demote_timeout_rate)),
    )
    recommended_path = out_dir / 'recommended_models.txt'
    recommended_path.write_text(','.join(recommended_models) + '\n', encoding='utf-8')
    demote_path = out_dir / 'demotion_report.json'
    demote_payload = {
        'demote_timeout_rate': float(args.demote_timeout_rate),
        'recommended_models': recommended_models,
        'demote_reasons': demote_reasons,
    }
    demote_path.write_text(json.dumps(demote_payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print_summary(results)
    print(f'recommended_models={",".join(recommended_models)}')
    if demote_reasons:
        print(f'demotion_reasons={json.dumps(demote_reasons, ensure_ascii=False)}')
    if recommended_models:
        model_csv = ','.join(recommended_models)
        print(f'use_in_hybrid_env=export HYBRID_REMOTE_MODEL={model_csv}')
        print(f'use_in_continuous_env=export CONTINUOUS_REMOTE_MODELS={model_csv}')
        print(f'use_in_lane_cmd=--models {model_csv}')
    print(f'benchmark_results_tsv={tsv_path}')
    print(f'benchmark_results_json={json_path}')
    print(f'benchmark_recommended_models={recommended_path}')
    print(f'benchmark_demotion_report={demote_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
