#!/usr/bin/env python3
"""Build decision-pack artifacts from a campaign database.

This script produces a compact manual-review package:
- strict_strong.csv
- strict_good.csv
- brand_forward_needs_expensive_checks.csv
- review_unique_top*.csv
- README.md
- manifest.json
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Iterable

STRICT_CHECKS: tuple[str, ...] = ('domain', 'web', 'app_store', 'package', 'social')
REVIEW_HEADERS: tuple[str, ...] = (
    'rank',
    'id',
    'name_display',
    'name_normalized',
    'current_recommendation',
    'score',
    'risk',
    'expensive_ok_types',
    'expensive_bad_count',
    'source_lane',
    'keep',
    'maybe',
    'drop',
    'decision_notes',
)
BASE_HEADERS: tuple[str, ...] = (
    'id',
    'name_display',
    'name_normalized',
    'current_recommendation',
    'score',
    'risk',
    'expensive_ok_types',
    'expensive_bad_count',
)


def _now_utc_compact() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S')


def _now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _to_float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: object) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def parse_review_tiers(raw: str) -> list[int]:
    values: list[int] = []
    seen: set[int] = set()
    for token in str(raw or '').split(','):
        token = token.strip()
        if not token:
            continue
        try:
            value = int(token)
        except ValueError:
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        values = [120, 50]
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Build decision-pack files from campaign DB.')
    parser.add_argument('--db', required=True, help='Path to naming_campaign.db')
    parser.add_argument('--out-dir', required=True, help='Directory where decision pack folder is created')
    parser.add_argument('--pack-prefix', default='decision_pack', help='Decision pack folder prefix')
    parser.add_argument(
        '--review-tiers',
        default='120,50',
        help='Comma-separated review list sizes to emit as review_unique_top<N>.csv',
    )
    parser.add_argument(
        '--include-unchecked',
        action='store_true',
        help='Also include non-checked states when computing strict/forward subsets.',
    )
    return parser.parse_args()


def _write_csv(path: Path, rows: list[dict[str, object]], headers: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers_list = list(headers)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=headers_list)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, '') for key in headers_list})


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout = 5000;')
    return conn


def _fetch_candidate_rows(conn: sqlite3.Connection, *, include_unchecked: bool) -> list[dict[str, object]]:
    check_placeholders = ','.join('?' for _ in STRICT_CHECKS)
    state_clause = '' if include_unchecked else "WHERE c.state = 'checked'"
    sql = f"""
    WITH latest AS (
      SELECT
        vr.candidate_id,
        vr.check_type,
        vr.status,
        ROW_NUMBER() OVER (
          PARTITION BY vr.candidate_id, vr.check_type
          ORDER BY vr.checked_at DESC, vr.id DESC
        ) AS rn
      FROM validation_results vr
      WHERE vr.check_type IN ({check_placeholders})
    ),
    latest_pick AS (
      SELECT candidate_id, check_type, status
      FROM latest
      WHERE rn = 1
    ),
    expensive AS (
      SELECT
        lp.candidate_id AS candidate_id,
        COUNT(DISTINCT CASE WHEN lp.status IN ('pass', 'warn') THEN lp.check_type END) AS expensive_ok_types,
        SUM(CASE WHEN lp.status IN ('fail', 'error') THEN 1 ELSE 0 END) AS expensive_bad_count
      FROM latest_pick lp
      GROUP BY lp.candidate_id
    ),
    shortlist AS (
      SELECT candidate_id, MAX(CASE WHEN selected = 1 THEN 1 ELSE 0 END) AS shortlisted
      FROM shortlist_decisions
      GROUP BY candidate_id
    )
    SELECT
      c.id AS id,
      c.name_display AS name_display,
      c.name_normalized AS name_normalized,
      COALESCE(c.current_recommendation, '') AS current_recommendation,
      COALESCE(c.current_score, 0) AS score,
      COALESCE(c.current_risk, 0) AS risk,
      COALESCE(exp.expensive_ok_types, 0) AS expensive_ok_types,
      COALESCE(exp.expensive_bad_count, 0) AS expensive_bad_count,
      COALESCE(s.shortlisted, 0) AS shortlisted
    FROM candidates c
    LEFT JOIN expensive exp ON exp.candidate_id = c.id
    LEFT JOIN shortlist s ON s.candidate_id = c.id
    {state_clause}
    ORDER BY
      c.current_score DESC,
      c.current_risk ASC,
      c.id DESC
    """
    rows = conn.execute(sql, STRICT_CHECKS).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def _strict_pass(row: dict[str, object]) -> bool:
    return _to_int(row.get('expensive_bad_count')) == 0 and _to_int(row.get('expensive_ok_types')) == len(STRICT_CHECKS)


def _is_good_recommendation(row: dict[str, object]) -> bool:
    rec = str(row.get('current_recommendation') or '').strip().lower()
    return rec in {'strong', 'consider'}


def _is_strong_recommendation(row: dict[str, object]) -> bool:
    rec = str(row.get('current_recommendation') or '').strip().lower()
    return rec == 'strong'


def _sort_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    rows = list(rows)
    rows.sort(
        key=lambda row: (
            -_to_float(row.get('score')),
            _to_float(row.get('risk')),
            -_to_int(row.get('shortlisted')),
            -_to_int(row.get('id')),
        )
    )
    return rows


def _dedupe_by_name(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        name = str(row.get('name_normalized') or '').strip().lower()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(row)
    return out


def _build_review_rows(
    strict_strong: list[dict[str, object]],
    strict_good: list[dict[str, object]],
    needs_expensive: list[dict[str, object]],
    *,
    limit: int,
) -> list[dict[str, object]]:
    combined: list[dict[str, object]] = []

    for row in strict_strong:
        item = dict(row)
        item['source_lane'] = 'strict_strong'
        combined.append(item)
    for row in strict_good:
        item = dict(row)
        item['source_lane'] = 'strict_good'
        combined.append(item)
    for row in needs_expensive:
        item = dict(row)
        item['source_lane'] = 'needs_expensive'
        combined.append(item)

    deduped = _dedupe_by_name(combined)[:limit]
    out: list[dict[str, object]] = []
    for idx, row in enumerate(deduped, start=1):
        out.append(
            {
                'rank': idx,
                'id': _to_int(row.get('id')),
                'name_display': str(row.get('name_display') or ''),
                'name_normalized': str(row.get('name_normalized') or ''),
                'current_recommendation': str(row.get('current_recommendation') or ''),
                'score': _to_float(row.get('score')),
                'risk': _to_float(row.get('risk')),
                'expensive_ok_types': _to_int(row.get('expensive_ok_types')),
                'expensive_bad_count': _to_int(row.get('expensive_bad_count')),
                'source_lane': str(row.get('source_lane') or ''),
                'keep': '',
                'maybe': '',
                'drop': '',
                'decision_notes': '',
            }
        )
    return out


def _write_readme(
    path: Path,
    *,
    db_path: Path,
    strict_strong_count: int,
    strict_good_count: int,
    forward_count: int,
    review_counts: dict[int, int],
) -> None:
    lines = [
        '# Decision Pack',
        '',
        f'- db: {db_path}',
        f'- created_at: {_now_utc_iso()}',
        f'- strict_strong_count: {strict_strong_count}',
        f'- strict_good_count: {strict_good_count}',
        f'- needs_expensive_checks_count: {forward_count}',
    ]
    for tier, count in sorted(review_counts.items(), reverse=True):
        lines.append(f'- review_unique_top{tier}_count: {count}')
    lines.extend(
        [
            '',
            'Files:',
            '- strict_strong.csv',
            '- strict_good.csv',
            '- brand_forward_needs_expensive_checks.csv',
        ]
    )
    for tier in sorted(review_counts.keys(), reverse=True):
        lines.append(f'- review_unique_top{tier}.csv')
    lines.extend(
        [
            '- manifest.json',
            '',
            'Manual review columns:',
            '- keep / maybe / drop / decision_notes',
        ]
    )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    out_root = Path(args.out_dir).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f'db not found: {db_path}')
    out_root.mkdir(parents=True, exist_ok=True)

    review_tiers = parse_review_tiers(args.review_tiers)
    pack_dir = out_root / f'{args.pack_prefix}_{_now_utc_compact()}'
    pack_dir.mkdir(parents=True, exist_ok=False)

    conn = _connect(db_path)
    try:
        rows = _fetch_candidate_rows(conn, include_unchecked=bool(args.include_unchecked))
    finally:
        conn.close()

    rows = _sort_rows(rows)
    strict_strong = _sort_rows([row for row in rows if _strict_pass(row) and _is_strong_recommendation(row)])
    strict_good = _sort_rows([row for row in rows if _strict_pass(row) and _is_good_recommendation(row)])
    needs_expensive = _sort_rows(
        [
            row
            for row in rows
            if _is_good_recommendation(row)
            and _to_int(row.get('expensive_bad_count')) == 0
            and _to_int(row.get('expensive_ok_types')) < len(STRICT_CHECKS)
        ]
    )

    _write_csv(pack_dir / 'strict_strong.csv', strict_strong, BASE_HEADERS)
    _write_csv(pack_dir / 'strict_good.csv', strict_good, BASE_HEADERS)
    _write_csv(pack_dir / 'brand_forward_needs_expensive_checks.csv', needs_expensive, BASE_HEADERS)

    review_counts: dict[int, int] = {}
    review_files: dict[int, str] = {}
    for tier in review_tiers:
        review_rows = _build_review_rows(strict_strong, strict_good, needs_expensive, limit=tier)
        _write_csv(pack_dir / f'review_unique_top{tier}.csv', review_rows, REVIEW_HEADERS)
        review_counts[tier] = len(review_rows)
        review_files[tier] = f'review_unique_top{tier}.csv'

    _write_readme(
        pack_dir / 'README.md',
        db_path=db_path,
        strict_strong_count=len(strict_strong),
        strict_good_count=len(strict_good),
        forward_count=len(needs_expensive),
        review_counts=review_counts,
    )

    manifest = {
        'created_at': _now_utc_iso(),
        'db_source': str(db_path),
        'strict_checks': list(STRICT_CHECKS),
        'strict_criteria': {
            'required_ok_types': len(STRICT_CHECKS),
            'allowed_statuses': ['pass', 'warn'],
            'forbidden_statuses': ['fail', 'error'],
        },
        'counts': {
            'strict_strong': len(strict_strong),
            'strict_good': len(strict_good),
            'needs_expensive_checks': len(needs_expensive),
            **{f'review_unique_top{tier}': review_counts[tier] for tier in review_tiers},
        },
        'files': {
            'strict_strong': 'strict_strong.csv',
            'strict_good': 'strict_good.csv',
            'needs_expensive_checks': 'brand_forward_needs_expensive_checks.csv',
            **{f'review_unique_top{tier}': review_files[tier] for tier in review_tiers},
            'readme': 'README.md',
        },
    }
    (pack_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

    max_tier = max(review_tiers)
    print(f'decision_pack_ok pack_dir={pack_dir}')
    print(f'decision_pack_review_csv={pack_dir / f"review_unique_top{max_tier}.csv"}')
    print(
        'decision_pack_counts '
        f'strict_strong={len(strict_strong)} '
        f'strict_good={len(strict_good)} '
        f'needs_expensive={len(needs_expensive)} '
        + ' '.join(f'top{tier}={review_counts[tier]}' for tier in sorted(review_tiers, reverse=True))
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
