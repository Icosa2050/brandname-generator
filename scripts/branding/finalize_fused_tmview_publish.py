#!/usr/bin/env python3
"""Final TMview gate for fused finalists.

This script keeps the raw fused ranking intact and produces a publish-safe
subset plus explicit review / rejected queues for the top fused finalists.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from euipo_esearch_probe import EuipoProbe, EuipoProbeResult


def _to_int(raw: Any, default: int = 0) -> int:
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8', newline='') as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, '') for key in fieldnames})


def final_fieldnames(rows: list[dict[str, str]]) -> list[str]:
    base_fields = list(rows[0].keys()) if rows else []
    return base_fields + [
        'final_publish_rank',
        'tmview_bucket',
        'tmview_reason',
        'tmview_query_ok',
        'tmview_exact_hits',
        'tmview_near_hits',
        'tmview_active_exact_hits',
        'tmview_inactive_exact_hits',
        'tmview_unknown_exact_hits',
        'tmview_result_count',
        'tmview_url',
        'tmview_sample_text',
        'tmview_exact_sample_text',
        'tmview_error',
    ]


def classify_tmview_result(
    result: EuipoProbeResult,
    *,
    inactive_exact_policy: str,
) -> tuple[str, str]:
    if not bool(result.query_ok):
        return 'review', 'tmview_query_unknown'
    if int(result.active_exact_hits) > 0:
        return 'rejected', 'tmview_exact_active_collision'
    if int(result.exact_hits) > 0:
        if inactive_exact_policy == 'reject':
            return 'rejected', 'tmview_exact_inactive_collision'
        if int(result.inactive_exact_hits) > 0 and int(result.unknown_exact_hits) == 0:
            return 'review', 'tmview_exact_inactive_review'
        return 'review', 'tmview_exact_unknown_review'
    if int(result.near_hits) > 0:
        return 'review', 'tmview_near_review'
    return 'publish', ''


def enrich_row(row: dict[str, str], result: EuipoProbeResult, bucket: str, reason: str) -> dict[str, object]:
    enriched: dict[str, object] = dict(row)
    enriched.update(
        {
            'tmview_bucket': bucket,
            'tmview_reason': reason,
            'tmview_query_ok': int(bool(result.query_ok)),
            'tmview_exact_hits': int(result.exact_hits),
            'tmview_near_hits': int(result.near_hits),
            'tmview_active_exact_hits': int(result.active_exact_hits),
            'tmview_inactive_exact_hits': int(result.inactive_exact_hits),
            'tmview_unknown_exact_hits': int(result.unknown_exact_hits),
            'tmview_result_count': int(result.result_count),
            'tmview_url': result.url,
            'tmview_sample_text': result.sample_text,
            'tmview_exact_sample_text': result.exact_sample_text,
            'tmview_error': result.error,
        }
    )
    return enriched


def probe_names(
    names: list[str],
    *,
    timeout_ms: int,
    settle_ms: int,
    headless: bool,
) -> dict[str, EuipoProbeResult]:
    results: dict[str, EuipoProbeResult] = {}
    with EuipoProbe(timeout_ms=timeout_ms, settle_ms=settle_ms, headless=headless) as probe:
        for name in names:
            results[name] = probe.probe_name(name)
    return results


def finalize_rows(
    rows: list[dict[str, str]],
    *,
    top_n: int,
    probes: dict[str, EuipoProbeResult],
    inactive_exact_policy: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    selected = rows[: max(0, int(top_n))] if int(top_n) > 0 else list(rows)
    publish_rows: list[dict[str, object]] = []
    review_rows: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []

    for row in selected:
        name = str(row.get('name') or '').strip().lower()
        if not name:
            continue
        result = probes.get(name)
        if result is None:
            result = EuipoProbeResult(
                name=name,
                url='',
                query_ok=False,
                source='tmview_playwright',
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text='',
                error='probe_missing',
            )
        bucket, reason = classify_tmview_result(result, inactive_exact_policy=inactive_exact_policy)
        enriched = enrich_row(row, result, bucket, reason)
        if bucket == 'publish':
            publish_rows.append(enriched)
        elif bucket == 'review':
            review_rows.append(enriched)
        else:
            rejected_rows.append(enriched)

    for idx, row in enumerate(publish_rows, start=1):
        row['final_publish_rank'] = idx
    for row in review_rows + rejected_rows:
        row['final_publish_rank'] = ''

    return publish_rows, review_rows, rejected_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run final TMview gate on fused finalists.')
    parser.add_argument('--input-csv', required=True, help='Fused rank CSV path.')
    parser.add_argument('--out-dir', default='', help='Fused out-dir (default: parent of input postrank dir).')
    parser.add_argument('--top-n', type=int, default=20, help='How many fused finalists to probe.')
    parser.add_argument('--timeout-ms', type=int, default=15000)
    parser.add_argument('--settle-ms', type=int, default=2500)
    parser.add_argument('--headful', action='store_true')
    parser.add_argument(
        '--inactive-exact-policy',
        choices=['review', 'reject'],
        default='review',
        help='How to route exact TMview hits that appear inactive (default: review).',
    )
    parser.add_argument('--fail-on-empty-publish', action='store_true')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_csv = Path(args.input_csv).expanduser().resolve()
    if not input_csv.exists():
        print(f'fused_tmview_gate_error missing_input={input_csv}')
        return 2

    if str(args.out_dir or '').strip():
        out_dir = Path(args.out_dir).expanduser().resolve()
        postrank_dir = out_dir / 'postrank'
    else:
        postrank_dir = input_csv.parent
        out_dir = postrank_dir.parent

    rows = load_rows(input_csv)

    selected = rows[: max(0, int(args.top_n))] if int(args.top_n) > 0 else list(rows)
    names = [str(row.get('name') or '').strip().lower() for row in selected if str(row.get('name') or '').strip()]
    probes: dict[str, EuipoProbeResult] = {}
    publish_rows: list[dict[str, object]] = []
    review_rows: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []
    if rows:
        probes = probe_names(
            names,
            timeout_ms=max(3000, int(args.timeout_ms)),
            settle_ms=max(0, int(args.settle_ms)),
            headless=not bool(args.headful),
        )

        publish_rows, review_rows, rejected_rows = finalize_rows(
            rows,
            top_n=max(0, int(args.top_n)),
            probes=probes,
            inactive_exact_policy=str(args.inactive_exact_policy),
        )

    fieldnames = final_fieldnames(rows)

    publish_csv = postrank_dir / 'fused_publish_final.csv'
    review_csv = postrank_dir / 'fused_review_queue.csv'
    rejected_csv = postrank_dir / 'fused_rejected.csv'
    summary_json = postrank_dir / 'fused_tmview_gate_summary.json'
    probe_json = postrank_dir / 'fused_tmview_probe.json'

    write_rows(publish_csv, publish_rows, fieldnames)
    write_rows(review_csv, review_rows, fieldnames)
    write_rows(rejected_csv, rejected_rows, fieldnames)
    probe_json.write_text(
        json.dumps([asdict(probes[name]) for name in names if name in probes], indent=2, ensure_ascii=False) + '\n',
        encoding='utf-8',
    )

    summary = {
        'input_csv': str(input_csv),
        'checked_count': len(selected),
        'checked_top_n': max(0, int(args.top_n)) if int(args.top_n) > 0 else len(rows),
        'inactive_exact_policy': str(args.inactive_exact_policy),
        'publish_count': len(publish_rows),
        'review_count': len(review_rows),
        'rejected_count': len(rejected_rows),
        'publish_csv': str(publish_csv),
        'review_csv': str(review_csv),
        'rejected_csv': str(rejected_csv),
        'probe_json': str(probe_json),
        'top_publish_names': [str(row.get('name') or '') for row in publish_rows[:20]],
        'top_review_names': [str(row.get('name') or '') for row in review_rows[:20]],
        'top_rejected_names': [str(row.get('name') or '') for row in rejected_rows[:20]],
    }
    summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

    if not rows:
        print(f'fused_tmview_gate_empty input={input_csv} summary={summary_json}')
        if bool(args.fail_on_empty_publish):
            print('fused_tmview_gate_fail reason=no_publish_survivors_after_tmview')
            return 4
        return 0

    print(
        f'fused_tmview_gate_complete checked={len(selected)} publish={len(publish_rows)} '
        f'review={len(review_rows)} rejected={len(rejected_rows)} '
        f'publish_csv={publish_csv} review_csv={review_csv} rejected_csv={rejected_csv}'
    )
    if bool(args.fail_on_empty_publish) and not publish_rows:
        print('fused_tmview_gate_fail reason=no_publish_survivors_after_tmview')
        return 4
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
