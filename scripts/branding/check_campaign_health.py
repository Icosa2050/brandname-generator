#!/usr/bin/env python3
"""Check campaign + post-rank health for automation-friendly monitoring."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class HealthConfig:
    min_new_shortlist: int
    min_postrank_strong: int
    min_iqr: float
    max_ceiling_share: float
    require_llm_stage_ok: bool


def load_last_progress_row(out_dir: Path) -> dict[str, Any]:
    progress_csv = out_dir / 'campaign_progress.csv'
    if not progress_csv.exists():
        return {}
    try:
        with progress_csv.open('r', encoding='utf-8', newline='') as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return {}
    if not rows:
        return {}
    return rows[-1]


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


def load_postrank_summary(out_dir: Path) -> dict[str, Any]:
    summary_json = out_dir / 'postrank' / 'deterministic_rubric_summary.json'
    if not summary_json.exists():
        return {}
    try:
        payload = json.loads(summary_json.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def evaluate_health(*, progress_row: dict[str, Any], postrank_summary: dict[str, Any], cfg: HealthConfig) -> dict[str, Any]:
    violations: list[str] = []

    if not progress_row:
        violations.append('campaign_progress_missing')
        llm_stage_status = ''
        new_shortlist_count = 0
    else:
        llm_stage_status = str(progress_row.get('llm_stage_status') or '')
        new_shortlist_count = _to_int(progress_row.get('new_shortlist_count'))
        if cfg.require_llm_stage_ok and llm_stage_status != 'ok':
            violations.append(f'llm_stage_status_not_ok:{llm_stage_status}')
        if new_shortlist_count < cfg.min_new_shortlist:
            violations.append(f'new_shortlist_below_threshold:{new_shortlist_count}<{cfg.min_new_shortlist}')

    if not postrank_summary:
        violations.append('postrank_summary_missing')
        iqr = 0.0
        ceiling_share = 1.0
        strong_count = 0
    else:
        iqr = _to_float(postrank_summary.get('iqr_total_score'))
        ceiling_share = _to_float(postrank_summary.get('score_ceiling_share'))
        rec_counts = postrank_summary.get('recommendation_counts') or {}
        strong_count = _to_int(rec_counts.get('strong'))

        if iqr < cfg.min_iqr:
            violations.append(f'iqr_below_threshold:{iqr:.2f}<{cfg.min_iqr:.2f}')
        if ceiling_share > cfg.max_ceiling_share:
            violations.append(f'ceiling_share_above_threshold:{ceiling_share:.4f}>{cfg.max_ceiling_share:.4f}')
        if strong_count < cfg.min_postrank_strong:
            violations.append(f'strong_count_below_threshold:{strong_count}<{cfg.min_postrank_strong}')

    healthy = len(violations) == 0
    return {
        'healthy': healthy,
        'violations': violations,
        'metrics': {
            'llm_stage_status': llm_stage_status,
            'new_shortlist_count': new_shortlist_count,
            'iqr_total_score': iqr,
            'score_ceiling_share': ceiling_share,
            'strong_count': strong_count,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Check campaign health and post-rank quality signals.')
    parser.add_argument('--out-dir', required=True, help='Campaign output directory.')
    parser.add_argument('--output-json', default='', help='Optional output JSON path.')
    parser.add_argument('--min-new-shortlist', type=int, default=10)
    parser.add_argument('--min-postrank-strong', type=int, default=6)
    parser.add_argument('--min-iqr', type=float, default=10.0)
    parser.add_argument('--max-ceiling-share', type=float, default=0.20)
    parser.add_argument('--allow-llm-stage-non-ok', action='store_true', help='Disable strict llm_stage_status=ok requirement.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    cfg = HealthConfig(
        min_new_shortlist=max(0, int(args.min_new_shortlist)),
        min_postrank_strong=max(0, int(args.min_postrank_strong)),
        min_iqr=max(0.0, float(args.min_iqr)),
        max_ceiling_share=max(0.0, min(1.0, float(args.max_ceiling_share))),
        require_llm_stage_ok=not bool(args.allow_llm_stage_non_ok),
    )

    progress_row = load_last_progress_row(out_dir)
    postrank_summary = load_postrank_summary(out_dir)
    result = evaluate_health(progress_row=progress_row, postrank_summary=postrank_summary, cfg=cfg)
    result['out_dir'] = str(out_dir)

    output_json = (
        Path(args.output_json).expanduser().resolve()
        if str(args.output_json or '').strip()
        else out_dir / 'postrank' / 'health_check.json'
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

    print(f"health_check healthy={int(bool(result.get('healthy')))} out={output_json}")
    if not bool(result.get('healthy')):
        print('health_violations=' + json.dumps(result.get('violations', []), ensure_ascii=False))
        return 3
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
