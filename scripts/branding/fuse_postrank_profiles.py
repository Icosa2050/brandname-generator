#!/usr/bin/env python3
"""Fuse deterministic post-rank outputs from quality + remote_quality lanes."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any


RECOMMENDATION_ORDER = {
    'drop': 0,
    'maybe': 1,
    'consider': 2,
    'strong': 3,
}


def _to_float(raw: Any, default: float = 0.0) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _to_int(raw: Any, default: int = 0) -> int:
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def normalize_name(name: str) -> str:
    return str(name or '').strip().lower()


def normalize_recommendation(raw: str) -> str:
    value = str(raw or '').strip().lower()
    if value in RECOMMENDATION_ORDER:
        return value
    return 'maybe'


def load_rank_rows(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    try:
        with csv_path.open('r', encoding='utf-8', newline='') as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        name = str(row.get('name') or '').strip()
        if not name:
            continue
        out.append(
            {
                'name': name,
                'key': normalize_name(name),
                'total_score': _to_float(row.get('total_score'), 0.0),
                'recommendation': normalize_recommendation(str(row.get('recommendation') or '')),
            }
        )
    return out


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def derive_weight_from_out_dir(out_dir: Path, rows: list[dict[str, Any]]) -> float:
    health = load_json(out_dir / 'postrank' / 'health_check.json')
    metrics = health.get('metrics') if isinstance(health.get('metrics'), dict) else {}
    strong_count = _to_int(metrics.get('strong_count'), 0)
    new_shortlist = _to_int(metrics.get('new_shortlist_count'), 0)
    if new_shortlist > 0:
        ratio = strong_count / float(new_shortlist)
        return max(0.05, min(1.0, ratio))

    summary = load_json(out_dir / 'postrank' / 'deterministic_rubric_summary.json')
    rec_counts = summary.get('recommendation_counts') if isinstance(summary.get('recommendation_counts'), dict) else {}
    strong_count = _to_int(rec_counts.get('strong'), 0)
    shortlist_count = _to_int(summary.get('shortlist_count'), 0)
    if shortlist_count <= 0:
        shortlist_count = sum(_to_int(v, 0) for v in rec_counts.values())
    if shortlist_count <= 0:
        shortlist_count = len(rows)
    if shortlist_count > 0:
        ratio = strong_count / float(shortlist_count)
        return max(0.05, min(1.0, ratio))
    return 1.0


def normalize_weights(quality_weight: float, remote_weight: float) -> tuple[float, float]:
    q = max(0.0, float(quality_weight))
    r = max(0.0, float(remote_weight))
    total = q + r
    if total <= 0.0:
        return 0.5, 0.5
    return q / total, r / total


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    p = max(0.0, min(1.0, float(p)))
    idx = p * (len(ordered) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] + ((ordered[hi] - ordered[lo]) * frac)


def _best_recommendation(a: str, b: str) -> str:
    return a if RECOMMENDATION_ORDER.get(a, 0) >= RECOMMENDATION_ORDER.get(b, 0) else b


def fuse_rankings(
    *,
    quality_rows: list[dict[str, Any]],
    remote_rows: list[dict[str, Any]],
    quality_weight: float,
    remote_weight: float,
    top_n: int,
    rrf_k: int,
    score_mix: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_rows = {
        'quality': list(quality_rows),
        'remote_quality': list(remote_rows),
    }
    source_weights = {
        'quality': float(quality_weight),
        'remote_quality': float(remote_weight),
    }
    score_ranges: dict[str, tuple[float, float]] = {}
    for source, rows in source_rows.items():
        if not rows:
            score_ranges[source] = (0.0, 0.0)
            continue
        values = [float(row.get('total_score') or 0.0) for row in rows]
        score_ranges[source] = (min(values), max(values))

    merged: dict[str, dict[str, Any]] = {}
    for source, rows in source_rows.items():
        w = float(source_weights.get(source, 0.0))
        min_score, max_score = score_ranges.get(source, (0.0, 0.0))
        score_span = max_score - min_score
        for rank_idx, row in enumerate(rows, start=1):
            key = str(row.get('key') or '')
            if not key:
                continue
            score_value = float(row.get('total_score') or 0.0)
            if score_span > 0.0:
                score_norm = max(0.0, min(1.0, (score_value - min_score) / score_span))
            else:
                score_norm = 0.5
            rrf_component = w / float(max(1, int(rrf_k)) + rank_idx)
            score_component = w * score_norm * (max(0.0, float(score_mix)) / float(max(1, int(rrf_k)) + 1))
            fused_increment = rrf_component + score_component

            current = merged.get(key)
            if current is None:
                current = {
                    'name': str(row.get('name') or key),
                    'key': key,
                    'fusion_score': 0.0,
                    'recommendation': 'drop',
                    'source_profiles': set(),
                    'quality_rank': '',
                    'quality_score': '',
                    'quality_recommendation': '',
                    'remote_quality_rank': '',
                    'remote_quality_score': '',
                    'remote_quality_recommendation': '',
                }
                merged[key] = current

            current['fusion_score'] = float(current.get('fusion_score') or 0.0) + fused_increment
            current['source_profiles'].add(source)
            current[f'{source}_rank'] = rank_idx
            current[f'{source}_score'] = score_value
            current[f'{source}_recommendation'] = str(row.get('recommendation') or 'maybe')
            current['recommendation'] = _best_recommendation(
                str(current.get('recommendation') or 'drop'),
                str(row.get('recommendation') or 'drop'),
            )

    fused_rows = list(merged.values())
    fused_rows.sort(
        key=lambda row: (
            -float(row.get('fusion_score') or 0.0),
            -RECOMMENDATION_ORDER.get(str(row.get('recommendation') or ''), 0),
            str(row.get('name') or ''),
        )
    )
    if top_n > 0:
        fused_rows = fused_rows[:top_n]

    for idx, row in enumerate(fused_rows, start=1):
        row['rank'] = idx
        profiles = row.get('source_profiles')
        row['source_profiles'] = ','.join(sorted(str(p) for p in profiles)) if isinstance(profiles, set) else str(profiles or '')

    all_scores = [float(row.get('fusion_score') or 0.0) for row in fused_rows]
    overlap = 0
    for row in fused_rows:
        sources = set(str(row.get('source_profiles') or '').split(','))
        sources.discard('')
        if len(sources) > 1:
            overlap += 1
    recommendation_counts = {key: 0 for key in RECOMMENDATION_ORDER}
    for row in fused_rows:
        recommendation_counts[normalize_recommendation(str(row.get('recommendation') or ''))] += 1

    summary = {
        'top_n': max(0, int(top_n)),
        'weights': {
            'quality': float(quality_weight),
            'remote_quality': float(remote_weight),
        },
        'rows': {
            'quality': len(quality_rows),
            'remote_quality': len(remote_rows),
            'fused': len(fused_rows),
        },
        'overlap_count_top_n': overlap,
        'recommendation_counts': recommendation_counts,
        'iqr_fusion_score': _percentile(all_scores, 0.75) - _percentile(all_scores, 0.25),
        'top_names': [str(row.get('name') or '') for row in fused_rows[:20]],
    }
    return fused_rows, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Fuse quality and remote_quality post-rank outputs.')
    parser.add_argument('--quality-out-dir', required=True, help='Campaign out-dir from quality profile run.')
    parser.add_argument('--remote-quality-out-dir', required=True, help='Campaign out-dir from remote_quality profile run.')
    parser.add_argument('--out-dir', default='', help='Output directory for fused report (default: <quality-out-dir>/fusion_quality_remote).')
    parser.add_argument('--top-n', type=int, default=40, help='Max fused names to keep.')
    parser.add_argument('--rrf-k', type=int, default=30, help='Reciprocal-rank-fusion k constant.')
    parser.add_argument('--score-mix', type=float, default=0.5, help='Additional normalized-score bonus mix factor.')
    parser.add_argument('--quality-weight', type=float, default=-1.0, help='Optional explicit quality profile weight.')
    parser.add_argument('--remote-weight', type=float, default=-1.0, help='Optional explicit remote_quality profile weight.')
    parser.add_argument('--input-rank-name', default='deterministic_rubric_rank.csv', help='Post-rank CSV filename inside postrank/.')
    parser.add_argument('--output-csv', default='', help='Optional explicit output CSV path.')
    parser.add_argument('--output-json', default='', help='Optional explicit output summary JSON path.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    quality_out_dir = Path(args.quality_out_dir).expanduser().resolve()
    remote_out_dir = Path(args.remote_quality_out_dir).expanduser().resolve()
    quality_csv = quality_out_dir / 'postrank' / str(args.input_rank_name)
    remote_csv = remote_out_dir / 'postrank' / str(args.input_rank_name)
    quality_rows = load_rank_rows(quality_csv)
    remote_rows = load_rank_rows(remote_csv)
    if not quality_rows:
        print(f'fusion_error missing_or_empty_quality_csv={quality_csv}')
        return 2
    if not remote_rows:
        print(f'fusion_error missing_or_empty_remote_csv={remote_csv}')
        return 2

    derived_quality_weight = derive_weight_from_out_dir(quality_out_dir, quality_rows)
    derived_remote_weight = derive_weight_from_out_dir(remote_out_dir, remote_rows)
    quality_weight = float(args.quality_weight) if float(args.quality_weight) >= 0.0 else derived_quality_weight
    remote_weight = float(args.remote_weight) if float(args.remote_weight) >= 0.0 else derived_remote_weight
    quality_weight, remote_weight = normalize_weights(quality_weight, remote_weight)

    fused_rows, summary = fuse_rankings(
        quality_rows=quality_rows,
        remote_rows=remote_rows,
        quality_weight=quality_weight,
        remote_weight=remote_weight,
        top_n=max(0, int(args.top_n)),
        rrf_k=max(1, int(args.rrf_k)),
        score_mix=max(0.0, float(args.score_mix)),
    )
    summary['input'] = {
        'quality_out_dir': str(quality_out_dir),
        'remote_quality_out_dir': str(remote_out_dir),
        'input_rank_name': str(args.input_rank_name),
        'quality_csv': str(quality_csv),
        'remote_quality_csv': str(remote_csv),
    }
    summary['derived_weights'] = {
        'quality': derived_quality_weight,
        'remote_quality': derived_remote_weight,
    }

    default_out_dir = quality_out_dir / 'fusion_quality_remote'
    out_dir = Path(args.out_dir).expanduser().resolve() if str(args.out_dir or '').strip() else default_out_dir
    output_csv = (
        Path(args.output_csv).expanduser().resolve()
        if str(args.output_csv or '').strip()
        else out_dir / 'postrank' / 'fused_quality_remote_rank.csv'
    )
    output_json = (
        Path(args.output_json).expanduser().resolve()
        if str(args.output_json or '').strip()
        else out_dir / 'postrank' / 'fused_quality_remote_summary.json'
    )
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        'rank',
        'name',
        'fusion_score',
        'recommendation',
        'source_profiles',
        'quality_rank',
        'quality_score',
        'quality_recommendation',
        'remote_quality_rank',
        'remote_quality_score',
        'remote_quality_recommendation',
    ]
    with output_csv.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in fused_rows:
            writer.writerow(
                {
                    'rank': int(row.get('rank') or 0),
                    'name': str(row.get('name') or ''),
                    'fusion_score': f"{float(row.get('fusion_score') or 0.0):.8f}",
                    'recommendation': normalize_recommendation(str(row.get('recommendation') or '')),
                    'source_profiles': str(row.get('source_profiles') or ''),
                    'quality_rank': row.get('quality_rank') or '',
                    'quality_score': row.get('quality_score') if row.get('quality_score') != '' else '',
                    'quality_recommendation': row.get('quality_recommendation') or '',
                    'remote_quality_rank': row.get('remote_quality_rank') or '',
                    'remote_quality_score': row.get('remote_quality_score') if row.get('remote_quality_score') != '' else '',
                    'remote_quality_recommendation': row.get('remote_quality_recommendation') or '',
                }
            )

    output_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(
        f'fusion_complete quality_rows={len(quality_rows)} remote_rows={len(remote_rows)} fused_rows={len(fused_rows)} '
        f'weights=quality:{quality_weight:.3f},remote:{remote_weight:.3f} '
        f'output_csv={output_csv} output_json={output_json}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
