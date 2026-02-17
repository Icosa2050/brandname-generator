#!/usr/bin/env python3
"""SQLite candidate lake for naming pipeline.

This module provides:
- schema initialization
- import of historical naming artifacts (CSV/JSON/JSONL)
- normalized candidate deduplication
- basic stats reporting

This is screening infrastructure only; not legal advice.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import re
import sqlite3
from pathlib import Path
from typing import Iterable


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec='seconds')


def normalize_name(value: str) -> str:
    return re.sub(r'[^a-z]+', '', value.lower())


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS naming_runs (
          id INTEGER PRIMARY KEY,
          created_at TEXT NOT NULL,
          scope TEXT,
          gate_mode TEXT,
          variation_profile TEXT,
          config_json TEXT,
          status TEXT NOT NULL,
          summary_json TEXT
        );

        CREATE TABLE IF NOT EXISTS candidates (
          id INTEGER PRIMARY KEY,
          name_display TEXT NOT NULL,
          name_normalized TEXT NOT NULL UNIQUE,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          current_score REAL,
          current_risk REAL,
          current_recommendation TEXT,
          state TEXT NOT NULL,
          state_updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS candidate_sources (
          id INTEGER PRIMARY KEY,
          candidate_id INTEGER NOT NULL,
          run_id INTEGER NOT NULL,
          source_type TEXT NOT NULL,
          source_label TEXT,
          prompt_or_seed TEXT,
          metadata_json TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
          FOREIGN KEY(run_id) REFERENCES naming_runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS validation_results (
          id INTEGER PRIMARY KEY,
          candidate_id INTEGER NOT NULL,
          run_id INTEGER NOT NULL,
          check_type TEXT NOT NULL,
          status TEXT NOT NULL,
          score_delta REAL,
          hard_fail INTEGER NOT NULL,
          reason TEXT,
          evidence_json TEXT,
          checked_at TEXT NOT NULL,
          cache_expires_at TEXT,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
          FOREIGN KEY(run_id) REFERENCES naming_runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS validation_jobs (
          id INTEGER PRIMARY KEY,
          run_id INTEGER NOT NULL,
          candidate_id INTEGER NOT NULL,
          check_type TEXT NOT NULL,
          status TEXT NOT NULL,
          attempt_count INTEGER NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          last_error TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(run_id, candidate_id, check_type),
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
          FOREIGN KEY(run_id) REFERENCES naming_runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS candidate_scores (
          id INTEGER PRIMARY KEY,
          candidate_id INTEGER NOT NULL,
          run_id INTEGER NOT NULL,
          quality_score REAL,
          risk_score REAL,
          external_penalty REAL,
          total_score REAL,
          recommendation TEXT,
          hard_fail INTEGER NOT NULL,
          reason TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
          FOREIGN KEY(run_id) REFERENCES naming_runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS state_transitions (
          id INTEGER PRIMARY KEY,
          candidate_id INTEGER NOT NULL,
          from_state TEXT,
          to_state TEXT NOT NULL,
          actor TEXT,
          note TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_candidates_state
          ON candidates(state);
        CREATE INDEX IF NOT EXISTS idx_validation_candidate_check
          ON validation_results(candidate_id, check_type);
        CREATE INDEX IF NOT EXISTS idx_validation_jobs_run_status
          ON validation_jobs(run_id, status);
        CREATE INDEX IF NOT EXISTS idx_validation_jobs_candidate_check
          ON validation_jobs(candidate_id, check_type);
        CREATE INDEX IF NOT EXISTS idx_candidate_scores_run
          ON candidate_scores(run_id);
        """
    )


def create_run(
    conn: sqlite3.Connection,
    *,
    source_path: str,
    scope: str | None,
    gate_mode: str | None,
    variation_profile: str = 'import',
    status: str = 'completed',
    config: dict | None = None,
    summary: dict | None = None,
) -> int:
    run_config = json.dumps(config or {'source_path': source_path}, ensure_ascii=False)
    run_summary = json.dumps(summary or {}, ensure_ascii=False)
    created_at = now_iso()
    cur = conn.execute(
        """
        INSERT INTO naming_runs(created_at, scope, gate_mode, variation_profile, config_json, status, summary_json)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (created_at, scope, gate_mode, variation_profile, run_config, status, run_summary),
    )
    return int(cur.lastrowid)


def upsert_candidate(
    conn: sqlite3.Connection,
    *,
    name_display: str,
    total_score: float | None,
    risk_score: float | None,
    recommendation: str | None,
) -> int:
    normalized = normalize_name(name_display)
    if not normalized:
        raise ValueError('Candidate name is empty after normalization')

    ts = now_iso()
    conn.execute(
        """
        INSERT INTO candidates(
          name_display, name_normalized, first_seen_at, last_seen_at,
          current_score, current_risk, current_recommendation, state, state_updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name_normalized) DO UPDATE SET
          name_display = excluded.name_display,
          last_seen_at = excluded.last_seen_at,
          current_score = COALESCE(excluded.current_score, candidates.current_score),
          current_risk = COALESCE(excluded.current_risk, candidates.current_risk),
          current_recommendation = COALESCE(excluded.current_recommendation, candidates.current_recommendation)
        """,
        (
            name_display,
            normalized,
            ts,
            ts,
            total_score,
            risk_score,
            recommendation,
            'new',
            ts,
        ),
    )
    row = conn.execute('SELECT id FROM candidates WHERE name_normalized = ?', (normalized,)).fetchone()
    if not row:
        raise RuntimeError(f'Failed to upsert candidate: {name_display}')
    return int(row[0])


def add_source(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    run_id: int,
    source_type: str,
    source_label: str,
    metadata: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO candidate_sources(
          candidate_id, run_id, source_type, source_label, prompt_or_seed, metadata_json, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            run_id,
            source_type,
            source_label,
            '',
            json.dumps(metadata, ensure_ascii=False),
            now_iso(),
        ),
    )


def add_score_snapshot(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    run_id: int,
    quality_score: float | None,
    risk_score: float | None,
    external_penalty: float | None,
    total_score: float | None,
    recommendation: str | None,
    hard_fail: bool,
    reason: str,
) -> None:
    conn.execute(
        """
        INSERT INTO candidate_scores(
          candidate_id, run_id, quality_score, risk_score, external_penalty,
          total_score, recommendation, hard_fail, reason, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            run_id,
            quality_score,
            risk_score,
            external_penalty,
            total_score,
            recommendation,
            1 if hard_fail else 0,
            reason,
            now_iso(),
        ),
    )


def add_validation_result(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    run_id: int,
    check_type: str,
    status: str,
    score_delta: float,
    hard_fail: bool,
    reason: str,
    evidence: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO validation_results(
          candidate_id, run_id, check_type, status, score_delta, hard_fail,
          reason, evidence_json, checked_at, cache_expires_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            run_id,
            check_type,
            status,
            score_delta,
            1 if hard_fail else 0,
            reason,
            json.dumps(evidence, ensure_ascii=False),
            now_iso(),
            None,
        ),
    )


def create_validation_job(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    candidate_id: int,
    check_type: str,
    status: str = 'pending',
) -> int:
    ts = now_iso()
    conn.execute(
        """
        INSERT OR IGNORE INTO validation_jobs(
          run_id, candidate_id, check_type, status, attempt_count,
          started_at, finished_at, last_error, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, candidate_id, check_type, status, 0, None, None, '', ts, ts),
    )
    row = conn.execute(
        'SELECT id FROM validation_jobs WHERE run_id = ? AND candidate_id = ? AND check_type = ?',
        (run_id, candidate_id, check_type),
    ).fetchone()
    if not row:
        raise RuntimeError('Failed to create/read validation job row')
    return int(row[0])


def update_validation_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    status: str,
    attempt_count: int | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    last_error: str | None = None,
) -> None:
    ts = now_iso()
    current = conn.execute(
        """
        SELECT attempt_count, started_at, finished_at, last_error
        FROM validation_jobs
        WHERE id = ?
        """,
        (job_id,),
    ).fetchone()
    if not current:
        raise RuntimeError(f'Validation job {job_id} not found')

    next_attempt = int(current[0] if attempt_count is None else attempt_count)
    next_started = current[1] if started_at is None else started_at
    next_finished = current[2] if finished_at is None else finished_at
    next_error = current[3] if last_error is None else last_error
    conn.execute(
        """
        UPDATE validation_jobs
        SET status = ?, attempt_count = ?, started_at = ?, finished_at = ?,
            last_error = ?, updated_at = ?
        WHERE id = ?
        """,
        (status, next_attempt, next_started, next_finished, next_error, ts, job_id),
    )


def to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {'1', 'true', 'yes', 'y'}


def parse_csv_rows(path: Path) -> tuple[list[dict], str | None, str | None]:
    rows: list[dict] = []
    scope: str | None = None
    gate_mode: str | None = None
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
            if scope is None and row.get('scope'):
                scope = str(row['scope'])
            if gate_mode is None and row.get('gate'):
                gate_mode = str(row['gate'])
    return rows, scope, gate_mode


def parse_json_rows(path: Path) -> tuple[list[dict], str | None, str | None]:
    data = json.loads(path.read_text(encoding='utf-8'))
    if isinstance(data, dict) and isinstance(data.get('candidates'), list):
        rows = [dict(item) for item in data['candidates'] if isinstance(item, dict)]
        return rows, data.get('scope'), data.get('gate')
    if isinstance(data, list):
        rows = [dict(item) for item in data if isinstance(item, dict)]
        return rows, None, None
    return [], None, None


def parse_jsonl_rows(path: Path) -> tuple[list[dict], str | None, str | None]:
    rows: list[dict] = []
    scope: str | None = None
    gate_mode: str | None = None
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                continue
            # Run summaries may contain top_candidates only.
            if isinstance(obj.get('top_candidates'), list):
                for top in obj['top_candidates']:
                    if isinstance(top, dict):
                        rows.append(dict(top))
            elif 'name' in obj:
                rows.append(obj)
            if scope is None and obj.get('scope'):
                scope = str(obj['scope'])
            if gate_mode is None and obj.get('gate'):
                gate_mode = str(obj['gate'])
    return rows, scope, gate_mode


def import_file(conn: sqlite3.Connection, path: Path, source_type: str) -> tuple[int, int]:
    suffix = path.suffix.lower()
    if suffix == '.csv':
        rows, scope, gate_mode = parse_csv_rows(path)
    elif suffix == '.json':
        rows, scope, gate_mode = parse_json_rows(path)
    elif suffix == '.jsonl':
        rows, scope, gate_mode = parse_jsonl_rows(path)
    else:
        return 0, 0

    if not rows:
        return 0, 0

    run_id = create_run(conn, source_path=str(path), scope=scope, gate_mode=gate_mode)

    imported = 0
    for row in rows:
        name = str(row.get('name') or row.get('name_display') or '').strip()
        if not name:
            continue
        total_score = to_float(row.get('total_score'))
        risk_score = to_float(row.get('challenge_risk') or row.get('risk_score'))
        recommendation = str(row.get('recommendation') or '').strip() or None

        candidate_id = upsert_candidate(
            conn,
            name_display=name,
            total_score=total_score,
            risk_score=risk_score,
            recommendation=recommendation,
        )
        add_source(
            conn,
            candidate_id=candidate_id,
            run_id=run_id,
            source_type=source_type,
            source_label=path.suffix.lower().lstrip('.'),
            metadata={'path': str(path)},
        )

        add_score_snapshot(
            conn,
            candidate_id=candidate_id,
            run_id=run_id,
            quality_score=to_float(row.get('quality_score')),
            risk_score=risk_score,
            external_penalty=to_float(row.get('external_penalty')),
            total_score=total_score,
            recommendation=recommendation,
            hard_fail=to_bool(row.get('hard_fail')),
            reason=str(row.get('fail_reason') or ''),
        )
        imported += 1

    return imported, run_id


def expand_inputs(inputs: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in inputs:
        matches = glob.glob(pattern)
        if matches:
            files.extend(Path(m) for m in matches)
            continue
        p = Path(pattern)
        if p.exists():
            files.append(p)
    return sorted(set(files))


def print_stats(conn: sqlite3.Connection) -> None:
    candidates = conn.execute('SELECT COUNT(*) FROM candidates').fetchone()[0]
    runs = conn.execute('SELECT COUNT(*) FROM naming_runs').fetchone()[0]
    scores = conn.execute('SELECT COUNT(*) FROM candidate_scores').fetchone()[0]
    jobs = conn.execute('SELECT COUNT(*) FROM validation_jobs').fetchone()[0]
    states = conn.execute(
        'SELECT state, COUNT(*) FROM candidates GROUP BY state ORDER BY state'
    ).fetchall()
    job_states = conn.execute(
        'SELECT status, COUNT(*) FROM validation_jobs GROUP BY status ORDER BY status'
    ).fetchall()

    print(f'db_stats candidates={candidates} runs={runs} score_rows={scores} validation_jobs={jobs}')
    if states:
        print('state_counts:')
        for state, count in states:
            print(f'- {state}: {count}')
    if job_states:
        print('validation_job_status_counts:')
        for state, count in job_states:
            print(f'- {state}: {count}')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='SQLite candidate lake for naming pipeline.')
    parser.add_argument('--db', default='docs/branding/naming_pipeline.db', help='SQLite DB path.')
    sub = parser.add_subparsers(dest='command', required=True)

    sub.add_parser('init', help='Initialize DB schema.')

    p_import = sub.add_parser('import', help='Import historical artifact files (CSV/JSON/JSONL).')
    p_import.add_argument('--inputs', nargs='+', required=True, help='Input file paths or glob patterns.')
    p_import.add_argument(
        '--source-type',
        default='import',
        choices=['import', 'manual', 'ai', 'rule'],
        help='Source type metadata to assign to imported candidates.',
    )

    sub.add_parser('stats', help='Print simple DB stats.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        ensure_schema(conn)

        if args.command == 'init':
            conn.commit()
            print(f'initialized db: {db_path}')
            return 0

        if args.command == 'stats':
            print_stats(conn)
            return 0

        if args.command == 'import':
            files = expand_inputs(args.inputs)
            if not files:
                print('No matching input files found.')
                return 1

            total_imported = 0
            runs_created = 0
            for path in files:
                imported, run_id = import_file(conn, path, args.source_type)
                if imported > 0:
                    total_imported += imported
                    runs_created += 1
                    print(f'imported {imported:4d} rows from {path} (run_id={run_id})')
                else:
                    print(f'skipped {path} (no candidate rows)')

            conn.commit()
            print(
                f'import_complete files={len(files)} runs={runs_created} imported_rows={total_imported} db={db_path}'
            )
            return 0

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
