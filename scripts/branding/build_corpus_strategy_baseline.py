#!/usr/bin/env python3
"""Build baseline diagnostics artifacts for corpus strategy rollout."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import sqlite3
from pathlib import Path


EXPENSIVE_CHECKS = ('domain', 'web', 'app_store', 'package', 'social')
EXPENSIVE_CHECK_COUNT = len(EXPENSIVE_CHECKS)
EXPENSIVE_CHECK_SQL_LIST = ','.join(f"'{check}'" for check in EXPENSIVE_CHECKS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Build corpus strategy baseline diagnostics from campaign DB.')
    parser.add_argument('--db', required=True, help='Campaign SQLite DB path.')
    parser.add_argument('--out-dir', required=True, help='Output directory for markdown/csv artifacts.')
    parser.add_argument('--label', default='baseline', help='Label suffix for generated artifact filenames.')
    parser.add_argument('--top-n', type=int, default=20, help='Top-N rows for markdown snippets.')
    return parser.parse_args()


def scalar(conn: sqlite3.Connection, query: str, params: tuple = ()) -> int:
    row = conn.execute(query, params).fetchone()
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def query_rows(conn: sqlite3.Connection, query: str, params: tuple = ()) -> list[tuple]:
    return [tuple(row) for row in conn.execute(query, params).fetchall()]


def write_csv(path: Path, headers: list[str], rows: list[tuple]) -> None:
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def to_md_table(headers: list[str], rows: list[tuple]) -> str:
    if not rows:
        return '_none_'
    header_line = '| ' + ' | '.join(headers) + ' |'
    sep_line = '| ' + ' | '.join(['---'] * len(headers)) + ' |'
    body = ['| ' + ' | '.join(str(value) for value in row) + ' |' for row in rows]
    return '\n'.join([header_line, sep_line, *body])


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    if not db_path.exists():
        print(f'DB not found: {db_path}')
        return 1

    label = str(args.label or 'baseline').strip().replace(' ', '_')
    expensive_mix_csv = out_dir / f'corpus_{label}_expensive_fail_mix.csv'
    hard_fail_csv = out_dir / f'corpus_{label}_hard_fail_reasons.csv'
    summary_md = out_dir / f'corpus_{label}_summary.md'

    with sqlite3.connect(db_path) as conn:
        runs = scalar(conn, 'SELECT IFNULL(MAX(id), 0) FROM naming_runs')
        candidates_total = scalar(conn, 'SELECT COUNT(*) FROM candidates')
        checked_total = scalar(conn, "SELECT COUNT(*) FROM candidates WHERE state='checked'")
        checked_strong = scalar(
            conn,
            "SELECT COUNT(*) FROM candidates WHERE state='checked' AND current_recommendation='strong'",
        )
        checked_consider = scalar(
            conn,
            "SELECT COUNT(*) FROM candidates WHERE state='checked' AND current_recommendation='consider'",
        )

        strict_good = scalar(
            conn,
            f"""
            WITH exp AS (
              SELECT candidate_id,
                     COUNT(DISTINCT CASE
                       WHEN check_type IN ({EXPENSIVE_CHECK_SQL_LIST})
                        AND status IN ('pass','warn') THEN check_type END) AS ok_types,
                     SUM(CASE
                       WHEN check_type IN ({EXPENSIVE_CHECK_SQL_LIST})
                        AND status IN ('fail','error') THEN 1 ELSE 0 END) AS bad_cnt
              FROM validation_results
              GROUP BY candidate_id
            )
            SELECT COUNT(*)
            FROM candidates c
            LEFT JOIN exp e ON e.candidate_id = c.id
            WHERE c.state='checked'
              AND c.current_recommendation IN ('strong','consider')
              AND IFNULL(e.bad_cnt,0)=0
              AND IFNULL(e.ok_types,0)={EXPENSIVE_CHECK_COUNT}
            """,
        )
        strict_strong = scalar(
            conn,
            f"""
            WITH exp AS (
              SELECT candidate_id,
                     COUNT(DISTINCT CASE
                       WHEN check_type IN ({EXPENSIVE_CHECK_SQL_LIST})
                        AND status IN ('pass','warn') THEN check_type END) AS ok_types,
                     SUM(CASE
                       WHEN check_type IN ({EXPENSIVE_CHECK_SQL_LIST})
                        AND status IN ('fail','error') THEN 1 ELSE 0 END) AS bad_cnt
              FROM validation_results
              GROUP BY candidate_id
            )
            SELECT COUNT(*)
            FROM candidates c
            LEFT JOIN exp e ON e.candidate_id = c.id
            WHERE c.state='checked'
              AND c.current_recommendation='strong'
              AND IFNULL(e.bad_cnt,0)=0
              AND IFNULL(e.ok_types,0)={EXPENSIVE_CHECK_COUNT}
            """,
        )

        state_counts = query_rows(
            conn,
            "SELECT state, COUNT(*) FROM candidates GROUP BY state ORDER BY COUNT(*) DESC, state ASC",
        )
        expensive_mix = query_rows(
            conn,
            f"""
            SELECT check_type, status, COUNT(*) AS n
            FROM validation_results
            WHERE check_type IN ({EXPENSIVE_CHECK_SQL_LIST})
            GROUP BY check_type, status
            ORDER BY check_type ASC, status ASC
            """,
        )
        hard_fail_reasons = query_rows(
            conn,
            """
            SELECT COALESCE(NULLIF(reason, ''), 'hard_fail') AS reason, COUNT(*) AS n
            FROM validation_results
            WHERE hard_fail = 1
            GROUP BY reason
            ORDER BY n DESC, reason ASC
            LIMIT ?
            """,
            (max(1, int(args.top_n)),),
        )
        top_source_labels = query_rows(
            conn,
            """
            SELECT COALESCE(NULLIF(source_label, ''), '(none)') AS source_label, COUNT(*) AS n
            FROM source_atoms
            GROUP BY source_label
            ORDER BY n DESC, source_label ASC
            LIMIT ?
            """,
            (max(1, int(args.top_n)),),
        )

    write_csv(expensive_mix_csv, ['check_type', 'status', 'count'], expensive_mix)
    write_csv(hard_fail_csv, ['reason', 'count'], hard_fail_reasons)

    generated_at = dt.datetime.now().isoformat(timespec='seconds')
    summary_lines = [
        '# Corpus Strategy Baseline',
        '',
        f'- generated_at: {generated_at}',
        f'- db: {db_path}',
        f'- runs: {runs}',
        f'- candidates_total: {candidates_total}',
        f'- checked_total: {checked_total}',
        f'- checked_strong: {checked_strong}',
        f'- checked_consider: {checked_consider}',
        f'- strict_good: {strict_good}',
        f'- strict_strong: {strict_strong}',
        '',
        '## Candidate State Counts',
        to_md_table(['state', 'count'], state_counts[: max(1, int(args.top_n))]),
        '',
        '## Expensive Check Status Mix',
        to_md_table(['check_type', 'status', 'count'], expensive_mix),
        '',
        '## Top Hard-Fail Reasons',
        to_md_table(['reason', 'count'], hard_fail_reasons),
        '',
        '## Top Source Labels',
        to_md_table(['source_label', 'count'], top_source_labels),
        '',
        '## Artifact Files',
        f'- expensive_mix_csv: {expensive_mix_csv}',
        f'- hard_fail_reasons_csv: {hard_fail_csv}',
    ]
    summary_md.write_text('\n'.join(summary_lines) + '\n', encoding='utf-8')

    print(f'corpus_baseline_complete summary={summary_md} expensive_mix_csv={expensive_mix_csv} hard_fail_csv={hard_fail_csv}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
