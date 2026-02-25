#!/usr/bin/env python3
"""Build acceptance-preflight artifacts from curated decision CSV + campaign DB.

This script is meant for "option (2)": evaluate what is likely to be acceptable
without launching new generation runs. It joins curated keep/maybe rows with the
latest expensive-check outcomes already stored in naming_campaign.db.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path

EXPENSIVE_CHECKS: tuple[str, ...] = ('domain', 'web', 'app_store', 'package', 'social')
OK_STATUSES = {'pass', 'warn'}
BAD_STATUSES = {'fail', 'error'}


@dataclass(frozen=True)
class DecisionRow:
    name: str
    decision_tag: str
    decision_notes: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Build acceptance preflight from decision CSV + campaign DB.')
    parser.add_argument(
        '--decision-csv',
        required=True,
        help='Path to review_unique_top120.csv (or compatible file with keep/maybe/drop columns).',
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to naming_campaign.db to query latest validation outcomes.',
    )
    parser.add_argument(
        '--out-dir',
        required=True,
        help='Output directory for preflight artifacts.',
    )
    parser.add_argument(
        '--mode',
        choices=['keep', 'keep_maybe'],
        default='keep_maybe',
        help='Which decision rows to include from decision CSV.',
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=20,
        help='How many ranked rows to include in markdown preview sections.',
    )
    return parser.parse_args()


def normalize_token(raw: str) -> str:
    return ''.join(ch for ch in str(raw or '').strip().lower() if ch.isalpha())


def is_x(raw: str) -> bool:
    return str(raw or '').strip().lower() == 'x'


def load_decisions(path: Path, mode: str) -> list[DecisionRow]:
    rows: list[DecisionRow] = []
    seen: set[str] = set()
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            keep = is_x(row.get('keep') or '')
            maybe = is_x(row.get('maybe') or '')
            include = keep or (mode == 'keep_maybe' and maybe)
            if not include:
                continue
            name = normalize_token(row.get('name_normalized') or row.get('name_display') or '')
            if not name or name in seen:
                continue
            seen.add(name)
            tag = 'keep' if keep else 'maybe'
            rows.append(
                DecisionRow(
                    name=name,
                    decision_tag=tag,
                    decision_notes=str(row.get('decision_notes') or '').strip(),
                )
            )
    return rows


def fetch_candidates(
    *,
    db_path: Path,
    names: list[str],
) -> dict[str, dict[str, object]]:
    if not names:
        return {}
    placeholders = ','.join('?' for _ in names)
    check_placeholders = ','.join('?' for _ in EXPENSIVE_CHECKS)
    sql = f"""
    WITH latest AS (
      SELECT
        vr.candidate_id,
        vr.check_type,
        vr.status,
        vr.reason,
        ROW_NUMBER() OVER (
          PARTITION BY vr.candidate_id, vr.check_type
          ORDER BY vr.checked_at DESC, vr.id DESC
        ) AS rn
      FROM validation_results vr
      WHERE vr.check_type IN ({check_placeholders})
    ),
    latest_pick AS (
      SELECT candidate_id, check_type, status, reason
      FROM latest
      WHERE rn = 1
    ),
    shortlist AS (
      SELECT
        candidate_id,
        MAX(CASE WHEN selected = 1 THEN 1 ELSE 0 END) AS shortlisted
      FROM shortlist_decisions
      GROUP BY candidate_id
    )
    SELECT
      c.id AS candidate_id,
      c.name_display,
      c.name_normalized,
      c.current_recommendation,
      c.current_score,
      c.current_risk,
      c.state,
      COALESCE(s.shortlisted, 0) AS shortlisted,
      MAX(CASE WHEN lp.check_type = 'domain' THEN lp.status END) AS domain_status,
      MAX(CASE WHEN lp.check_type = 'web' THEN lp.status END) AS web_status,
      MAX(CASE WHEN lp.check_type = 'app_store' THEN lp.status END) AS app_store_status,
      MAX(CASE WHEN lp.check_type = 'package' THEN lp.status END) AS package_status,
      MAX(CASE WHEN lp.check_type = 'social' THEN lp.status END) AS social_status,
      MAX(CASE WHEN lp.check_type = 'domain' THEN lp.reason END) AS domain_reason,
      MAX(CASE WHEN lp.check_type = 'web' THEN lp.reason END) AS web_reason,
      MAX(CASE WHEN lp.check_type = 'app_store' THEN lp.reason END) AS app_store_reason,
      MAX(CASE WHEN lp.check_type = 'package' THEN lp.reason END) AS package_reason,
      MAX(CASE WHEN lp.check_type = 'social' THEN lp.reason END) AS social_reason
    FROM candidates c
    LEFT JOIN latest_pick lp ON lp.candidate_id = c.id
    LEFT JOIN shortlist s ON s.candidate_id = c.id
    WHERE c.name_normalized IN ({placeholders})
    GROUP BY
      c.id,
      c.name_display,
      c.name_normalized,
      c.current_recommendation,
      c.current_score,
      c.current_risk,
      c.state,
      shortlisted
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql, [*EXPENSIVE_CHECKS, *names])
        out: dict[str, dict[str, object]] = {}
        for row in cur.fetchall():
            payload = {k: row[k] for k in row.keys()}
            out[str(payload.get('name_normalized') or '')] = payload
        return out
    finally:
        conn.close()


def rec_rank(value: object) -> int:
    token = str(value or '').strip().lower()
    if token == 'strong':
        return 3
    if token == 'consider':
        return 2
    if token == 'weak':
        return 1
    return 0


def status_counts(payload: dict[str, object]) -> tuple[int, int]:
    statuses = [
        str(payload.get('domain_status') or '').strip().lower(),
        str(payload.get('web_status') or '').strip().lower(),
        str(payload.get('app_store_status') or '').strip().lower(),
        str(payload.get('package_status') or '').strip().lower(),
        str(payload.get('social_status') or '').strip().lower(),
    ]
    ok = sum(1 for token in statuses if token in OK_STATUSES)
    bad = sum(1 for token in statuses if token in BAD_STATUSES)
    return ok, bad


def to_float(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def build_rows(decisions: list[DecisionRow], candidates: dict[str, dict[str, object]]) -> tuple[list[dict[str, object]], list[DecisionRow]]:
    rows: list[dict[str, object]] = []
    missing: list[DecisionRow] = []
    for item in decisions:
        payload = candidates.get(item.name)
        if payload is None:
            missing.append(item)
            continue
        ok_types, bad_count = status_counts(payload)
        recommendation = str(payload.get('current_recommendation') or '').strip().lower()
        strict_pass = bad_count == 0 and ok_types == len(EXPENSIVE_CHECKS)
        strict_strong = strict_pass and recommendation == 'strong'
        row = dict(payload)
        row['decision_tag'] = item.decision_tag
        row['decision_notes'] = item.decision_notes
        row['expensive_ok_types'] = ok_types
        row['expensive_bad_count'] = bad_count
        row['strict_pass'] = 1 if strict_pass else 0
        row['strict_strong'] = 1 if strict_strong else 0
        rows.append(row)
    rows.sort(
        key=lambda r: (
            -int(r.get('strict_strong') or 0),
            -int(r.get('strict_pass') or 0),
            -int(r.get('shortlisted') or 0),
            -rec_rank(r.get('current_recommendation')),
            -to_float(r.get('current_score')),
            to_float(r.get('current_risk')),
            str(r.get('name_normalized') or ''),
        )
    )
    for idx, row in enumerate(rows, start=1):
        row['rank'] = idx
    return rows, missing


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    headers = [
        'rank',
        'name_display',
        'name_normalized',
        'decision_tag',
        'decision_notes',
        'state',
        'current_recommendation',
        'current_score',
        'current_risk',
        'shortlisted',
        'strict_pass',
        'strict_strong',
        'expensive_ok_types',
        'expensive_bad_count',
        'domain_status',
        'web_status',
        'app_store_status',
        'package_status',
        'social_status',
        'domain_reason',
        'web_reason',
        'app_store_reason',
        'package_reason',
        'social_reason',
    ]
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, '') for key in headers})


def write_markdown(
    *,
    path: Path,
    rows: list[dict[str, object]],
    missing: list[DecisionRow],
    top_n: int,
    decision_csv: Path,
    db_path: Path,
    mode: str,
) -> None:
    strict_pass = [row for row in rows if int(row.get('strict_pass') or 0) == 1]
    strict_strong = [row for row in rows if int(row.get('strict_strong') or 0) == 1]
    lines: list[str] = []
    lines.append('# Acceptance Preflight Summary')
    lines.append('')
    lines.append(f'- decision_csv: `{decision_csv}`')
    lines.append(f'- db: `{db_path}`')
    lines.append(f'- mode: `{mode}`')
    lines.append(f'- selected_input_count: {len(rows) + len(missing)}')
    lines.append(f'- found_in_db_count: {len(rows)}')
    lines.append(f'- missing_in_db_count: {len(missing)}')
    lines.append(f'- strict_pass_count: {len(strict_pass)}')
    lines.append(f'- strict_strong_count: {len(strict_strong)}')
    lines.append('')

    lines.append(f'## Top {max(1, top_n)} Ranked')
    lines.append('')
    lines.append('| rank | name | decision | rec | score | risk | strict_pass | strict_strong | shortlisted |')
    lines.append('|---:|---|---|---|---:|---:|---:|---:|---:|')
    for row in rows[: max(1, top_n)]:
        lines.append(
            '| {rank} | {name_display} | {decision_tag} | {rec} | {score:.1f} | {risk:.1f} | {strict_pass} | {strict_strong} | {shortlisted} |'.format(
                rank=int(row.get('rank') or 0),
                name_display=str(row.get('name_display') or ''),
                decision_tag=str(row.get('decision_tag') or ''),
                rec=str(row.get('current_recommendation') or ''),
                score=to_float(row.get('current_score')),
                risk=to_float(row.get('current_risk')),
                strict_pass=int(row.get('strict_pass') or 0),
                strict_strong=int(row.get('strict_strong') or 0),
                shortlisted=int(row.get('shortlisted') or 0),
            )
        )
    lines.append('')

    if missing:
        lines.append('## Missing In DB')
        lines.append('')
        for item in missing:
            lines.append(f'- `{item.name}` ({item.decision_tag})')
        lines.append('')

    path.write_text('\n'.join(lines), encoding='utf-8')


def main() -> int:
    args = parse_args()
    decision_csv = Path(args.decision_csv).expanduser()
    db_path = Path(args.db).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    decisions = load_decisions(decision_csv, args.mode)
    selected_names = [row.name for row in decisions]
    candidates = fetch_candidates(db_path=db_path, names=selected_names)
    ranked_rows, missing_rows = build_rows(decisions, candidates)

    ranked_csv = out_dir / 'acceptance_ranked.csv'
    strict_csv = out_dir / 'acceptance_strict_pass.csv'
    summary_md = out_dir / 'acceptance_preflight_summary.md'
    write_csv(ranked_csv, ranked_rows)
    write_csv(strict_csv, [row for row in ranked_rows if int(row.get('strict_pass') or 0) == 1])
    write_markdown(
        path=summary_md,
        rows=ranked_rows,
        missing=missing_rows,
        top_n=max(1, int(args.top_n)),
        decision_csv=decision_csv,
        db_path=db_path,
        mode=args.mode,
    )
    print(f'acceptance_preflight_ok out_dir={out_dir}')
    print(f'acceptance_ranked_csv={ranked_csv}')
    print(f'acceptance_strict_csv={strict_csv}')
    print(f'acceptance_summary_md={summary_md}')
    print(f'input_count={len(decisions)} found_count={len(ranked_rows)} missing_count={len(missing_rows)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
