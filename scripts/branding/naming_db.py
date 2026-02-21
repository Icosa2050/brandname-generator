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


DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 5000


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec='seconds')


def normalize_name(value: str) -> str:
    return re.sub(r'[^a-z]+', '', value.lower())


def configure_connection(
    conn: sqlite3.Connection,
    *,
    busy_timeout_ms: int = DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
    wal: bool = True,
) -> None:
    if wal:
        conn.execute('PRAGMA journal_mode = WAL;')
    conn.execute(f'PRAGMA busy_timeout = {max(0, int(busy_timeout_ms))};')
    conn.execute('PRAGMA foreign_keys = ON;')


def open_connection(
    db_path: Path,
    *,
    busy_timeout_ms: int = DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
    wal: bool = True,
) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    configure_connection(conn, busy_timeout_ms=busy_timeout_ms, wal=wal)
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f'PRAGMA table_info({table})').fetchall()}


def _ensure_column(conn: sqlite3.Connection, table: str, column_name: str, definition_sql: str) -> None:
    if column_name in _table_columns(conn, table):
        return
    conn.execute(f'ALTER TABLE {table} ADD COLUMN {column_name} {definition_sql}')


def ensure_schema(
    conn: sqlite3.Connection,
    *,
    busy_timeout_ms: int = DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
    wal: bool = True,
) -> None:
    configure_connection(conn, busy_timeout_ms=busy_timeout_ms, wal=wal)
    conn.executescript(
        """
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

        CREATE TABLE IF NOT EXISTS source_atoms (
          id INTEGER PRIMARY KEY,
          atom_display TEXT NOT NULL,
          atom_normalized TEXT NOT NULL UNIQUE,
          language_hint TEXT,
          semantic_category TEXT,
          source_label TEXT,
          confidence_weight REAL,
          metadata_json TEXT,
          active INTEGER NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
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

        CREATE TABLE IF NOT EXISTS candidate_lineage (
          id INTEGER PRIMARY KEY,
          candidate_id INTEGER NOT NULL,
          run_id INTEGER NOT NULL,
          generator_family TEXT NOT NULL,
          source_atom_id INTEGER,
          contribution_weight REAL,
          note TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
          FOREIGN KEY(run_id) REFERENCES naming_runs(id) ON DELETE CASCADE,
          FOREIGN KEY(source_atom_id) REFERENCES source_atoms(id) ON DELETE SET NULL
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

        CREATE TABLE IF NOT EXISTS shortlist_decisions (
          id INTEGER PRIMARY KEY,
          candidate_id INTEGER NOT NULL,
          run_id INTEGER NOT NULL,
          selected INTEGER NOT NULL,
          shortlist_rank INTEGER,
          bucket_key TEXT,
          reason TEXT,
          score REAL,
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
        CREATE INDEX IF NOT EXISTS idx_source_atoms_category
          ON source_atoms(semantic_category, language_hint);
        CREATE INDEX IF NOT EXISTS idx_candidate_lineage_candidate
          ON candidate_lineage(candidate_id, run_id);
        CREATE INDEX IF NOT EXISTS idx_candidate_lineage_source
          ON candidate_lineage(source_atom_id);
        CREATE INDEX IF NOT EXISTS idx_shortlist_run_candidate
          ON shortlist_decisions(run_id, candidate_id, selected);
        """
    )

    # Lightweight schema evolution for v3 candidate fields while preserving v2 compatibility.
    _ensure_column(conn, 'candidates', 'engine_id', "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, 'candidates', 'parent_ids', "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, 'candidates', 'status', "TEXT NOT NULL DEFAULT 'new'")
    _ensure_column(conn, 'candidates', 'rejection_reason', "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, 'candidates', 'score_quality', 'REAL')
    _ensure_column(conn, 'candidates', 'score_total', 'REAL')
    conn.execute(
        """
        UPDATE candidates
        SET status = state
        WHERE (status IS NULL OR status = '')
          AND state IS NOT NULL
          AND state <> ''
        """
    )
    conn.execute(
        """
        UPDATE candidates
        SET score_total = current_score
        WHERE score_total IS NULL
          AND current_score IS NOT NULL
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
    quality_score: float | None = None,
    engine_id: str | None = None,
    parent_ids: str | None = None,
    status: str | None = None,
    rejection_reason: str | None = None,
) -> int:
    normalized = normalize_name(name_display)
    if not normalized:
        raise ValueError('Candidate name is empty after normalization')

    ts = now_iso()
    status_insert = (status or '').strip() or 'new'
    engine_insert = (engine_id or '').strip()
    parent_insert = (parent_ids or '').strip()
    rejection_insert = (rejection_reason or '').strip()
    conn.execute(
        """
        INSERT INTO candidates(
          name_display, name_normalized, first_seen_at, last_seen_at,
          current_score, current_risk, current_recommendation, state, state_updated_at,
          engine_id, parent_ids, status, rejection_reason, score_quality, score_total
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name_normalized) DO UPDATE SET
          name_display = excluded.name_display,
          last_seen_at = excluded.last_seen_at,
          current_score = COALESCE(excluded.current_score, candidates.current_score),
          current_risk = COALESCE(excluded.current_risk, candidates.current_risk),
          current_recommendation = COALESCE(excluded.current_recommendation, candidates.current_recommendation),
          engine_id = CASE
            WHEN excluded.engine_id IS NOT NULL AND excluded.engine_id <> '' THEN excluded.engine_id
            ELSE candidates.engine_id
          END,
          parent_ids = CASE
            WHEN excluded.parent_ids IS NOT NULL AND excluded.parent_ids <> '' THEN excluded.parent_ids
            ELSE candidates.parent_ids
          END,
          rejection_reason = excluded.rejection_reason,
          score_quality = COALESCE(excluded.score_quality, candidates.score_quality),
          score_total = COALESCE(excluded.score_total, candidates.score_total)
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
            engine_insert,
            parent_insert,
            status_insert,
            rejection_insert,
            quality_score,
            total_score,
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


def upsert_source_atom(
    conn: sqlite3.Connection,
    *,
    atom_display: str,
    language_hint: str = '',
    semantic_category: str = '',
    source_label: str = '',
    confidence_weight: float | None = None,
    metadata: dict | None = None,
    active: bool = True,
) -> int:
    atom_normalized = normalize_name(atom_display)
    if not atom_normalized:
        raise ValueError('Source atom is empty after normalization')

    ts = now_iso()
    conn.execute(
        """
        INSERT INTO source_atoms(
          atom_display, atom_normalized, language_hint, semantic_category,
          source_label, confidence_weight, metadata_json, active, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(atom_normalized) DO UPDATE SET
          atom_display = excluded.atom_display,
          language_hint = CASE
            WHEN excluded.language_hint IS NOT NULL AND excluded.language_hint <> '' THEN excluded.language_hint
            ELSE source_atoms.language_hint
          END,
          semantic_category = CASE
            WHEN excluded.semantic_category IS NOT NULL AND excluded.semantic_category <> '' THEN excluded.semantic_category
            ELSE source_atoms.semantic_category
          END,
          source_label = CASE
            WHEN excluded.source_label IS NOT NULL AND excluded.source_label <> '' THEN excluded.source_label
            ELSE source_atoms.source_label
          END,
          confidence_weight = COALESCE(excluded.confidence_weight, source_atoms.confidence_weight),
          metadata_json = CASE
            WHEN excluded.metadata_json IS NOT NULL AND excluded.metadata_json <> '' THEN excluded.metadata_json
            ELSE source_atoms.metadata_json
          END,
          active = excluded.active,
          updated_at = excluded.updated_at
        """,
        (
            atom_display,
            atom_normalized,
            language_hint.strip(),
            semantic_category.strip(),
            source_label.strip(),
            confidence_weight,
            json.dumps(metadata or {}, ensure_ascii=False),
            1 if active else 0,
            ts,
            ts,
        ),
    )
    row = conn.execute('SELECT id FROM source_atoms WHERE atom_normalized = ?', (atom_normalized,)).fetchone()
    if not row:
        raise RuntimeError(f'Failed to upsert source atom: {atom_display}')
    return int(row[0])


def list_source_atoms(
    conn: sqlite3.Connection,
    *,
    limit: int = 500,
    language_hint: str | None = None,
    semantic_category: str | None = None,
    min_confidence: float = 0.0,
    include_inactive: bool = False,
) -> list[dict]:
    where: list[str] = []
    params: list[object] = []

    if not include_inactive:
        where.append('active = 1')
    if language_hint:
        where.append('LOWER(language_hint) = ?')
        params.append(language_hint.strip().lower())
    if semantic_category:
        where.append('LOWER(semantic_category) = ?')
        params.append(semantic_category.strip().lower())
    if min_confidence > 0.0:
        where.append('COALESCE(confidence_weight, 0) >= ?')
        params.append(float(min_confidence))

    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''
    query = f"""
        SELECT id, atom_display, atom_normalized, language_hint, semantic_category,
               source_label, confidence_weight, metadata_json, active
        FROM source_atoms
        {where_sql}
        ORDER BY COALESCE(confidence_weight, 0) DESC, atom_display ASC
        LIMIT ?
    """
    params.append(max(1, int(limit)))
    rows = conn.execute(query, tuple(params)).fetchall()

    out: list[dict] = []
    for row in rows:
        metadata_json = str(row[7] or '')
        try:
            metadata = json.loads(metadata_json) if metadata_json else {}
        except json.JSONDecodeError:
            metadata = {}
        out.append(
            {
                'id': int(row[0]),
                'atom_display': str(row[1]),
                'atom_normalized': str(row[2]),
                'language_hint': str(row[3] or ''),
                'semantic_category': str(row[4] or ''),
                'source_label': str(row[5] or ''),
                'confidence_weight': float(row[6] or 0.0),
                'metadata': metadata,
                'active': bool(int(row[8] or 0)),
            }
        )
    return out


def add_candidate_lineage(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    run_id: int,
    generator_family: str,
    source_atom_id: int | None = None,
    contribution_weight: float | None = None,
    note: str = '',
) -> None:
    conn.execute(
        """
        INSERT INTO candidate_lineage(
          candidate_id, run_id, generator_family, source_atom_id, contribution_weight, note, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            run_id,
            generator_family,
            source_atom_id,
            contribution_weight,
            note,
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


def add_shortlist_decision(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    run_id: int,
    selected: bool,
    shortlist_rank: int,
    bucket_key: str,
    reason: str,
    score: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO shortlist_decisions(
          candidate_id, run_id, selected, shortlist_rank, bucket_key, reason, score, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            run_id,
            1 if selected else 0,
            shortlist_rank if shortlist_rank > 0 else None,
            bucket_key,
            reason,
            score,
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
        quality_score = to_float(row.get('quality_score'))
        engine_id = str(row.get('engine_id') or row.get('generator_family') or '').strip() or None
        parent_ids = str(row.get('parent_ids') or row.get('lineage_atoms') or '').strip() or None
        status = str(row.get('status') or '').strip() or None
        rejection_reason = str(row.get('rejection_reason') or row.get('fail_reason') or '').strip() or None

        candidate_id = upsert_candidate(
            conn,
            name_display=name,
            total_score=total_score,
            risk_score=risk_score,
            recommendation=recommendation,
            quality_score=quality_score,
            engine_id=engine_id,
            parent_ids=parent_ids,
            status=status,
            rejection_reason=rejection_reason,
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
    shortlist_rows = conn.execute('SELECT COUNT(*) FROM shortlist_decisions').fetchone()[0]
    jobs = conn.execute('SELECT COUNT(*) FROM validation_jobs').fetchone()[0]
    source_atoms = conn.execute('SELECT COUNT(*) FROM source_atoms').fetchone()[0]
    lineage_rows = conn.execute('SELECT COUNT(*) FROM candidate_lineage').fetchone()[0]
    states = conn.execute(
        'SELECT state, COUNT(*) FROM candidates GROUP BY state ORDER BY state'
    ).fetchall()
    job_states = conn.execute(
        'SELECT status, COUNT(*) FROM validation_jobs GROUP BY status ORDER BY status'
    ).fetchall()

    print(
        'db_stats '
        f'candidates={candidates} runs={runs} score_rows={scores} shortlist_rows={shortlist_rows} '
        f'validation_jobs={jobs} '
        f'source_atoms={source_atoms} lineage_rows={lineage_rows}'
    )
    if states:
        print('state_counts:')
        for state, count in states:
            print(f'- {state}: {count}')
    if job_states:
        print('validation_job_status_counts:')
        for state, count in job_states:
            print(f'- {state}: {count}')


def print_source_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute('SELECT COUNT(*) FROM source_atoms').fetchone()[0]
    active = conn.execute('SELECT COUNT(*) FROM source_atoms WHERE active = 1').fetchone()[0]
    with_conf = conn.execute('SELECT COUNT(*) FROM source_atoms WHERE confidence_weight IS NOT NULL').fetchone()[0]
    categories = conn.execute(
        """
        SELECT COALESCE(NULLIF(semantic_category, ''), '(none)') AS cat, COUNT(*)
        FROM source_atoms
        GROUP BY cat
        ORDER BY COUNT(*) DESC, cat ASC
        """
    ).fetchall()
    langs = conn.execute(
        """
        SELECT COALESCE(NULLIF(language_hint, ''), '(none)') AS lang, COUNT(*)
        FROM source_atoms
        GROUP BY lang
        ORDER BY COUNT(*) DESC, lang ASC
        """
    ).fetchall()
    labels = conn.execute(
        """
        SELECT COALESCE(NULLIF(source_label, ''), '(none)') AS label, COUNT(*)
        FROM source_atoms
        GROUP BY label
        ORDER BY COUNT(*) DESC, label ASC
        """
    ).fetchall()

    print(f'source_stats total={total} active={active} with_confidence={with_conf}')
    if categories:
        print('source_categories:')
        for cat, count in categories:
            print(f'- {cat}: {count}')
    if langs:
        print('source_languages:')
        for lang, count in langs:
            print(f'- {lang}: {count}')
    if labels:
        print('source_labels:')
        for label, count in labels:
            print(f'- {label}: {count}')


def assert_run_contract(
    conn: sqlite3.Connection,
    *,
    run_id: int | None,
    min_candidates: int,
    require_shortlist: bool,
) -> tuple[bool, dict[str, object]]:
    resolved_run_id = run_id
    if resolved_run_id is None:
        row = conn.execute(
            """
            SELECT nr.id
            FROM naming_runs nr
            WHERE EXISTS (
              SELECT 1 FROM candidate_sources cs WHERE cs.run_id = nr.id
            )
            ORDER BY nr.id DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return False, {'error': 'no_runs_found'}
        resolved_run_id = int(row[0])

    candidate_count = int(
        conn.execute(
            'SELECT COUNT(*) FROM candidate_sources WHERE run_id = ?',
            (resolved_run_id,),
        ).fetchone()[0]
    )
    score_count = int(
        conn.execute(
            'SELECT COUNT(*) FROM candidate_scores WHERE run_id = ?',
            (resolved_run_id,),
        ).fetchone()[0]
    )
    lineage_count = int(
        conn.execute(
            'SELECT COUNT(*) FROM candidate_lineage WHERE run_id = ?',
            (resolved_run_id,),
        ).fetchone()[0]
    )
    shortlist_total = int(
        conn.execute(
            'SELECT COUNT(*) FROM shortlist_decisions WHERE run_id = ?',
            (resolved_run_id,),
        ).fetchone()[0]
    )
    shortlist_selected = int(
        conn.execute(
            'SELECT COUNT(*) FROM shortlist_decisions WHERE run_id = ? AND selected = 1',
            (resolved_run_id,),
        ).fetchone()[0]
    )
    missing_engine = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM candidate_sources cs
            JOIN candidates c ON c.id = cs.candidate_id
            WHERE cs.run_id = ? AND COALESCE(TRIM(c.engine_id), '') = ''
            """,
            (resolved_run_id,),
        ).fetchone()[0]
    )
    missing_parent_ids = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM candidate_sources cs
            JOIN candidates c ON c.id = cs.candidate_id
            WHERE cs.run_id = ? AND COALESCE(TRIM(c.parent_ids), '') = ''
            """,
            (resolved_run_id,),
        ).fetchone()[0]
    )

    details = {
        'run_id': resolved_run_id,
        'candidate_count': candidate_count,
        'score_count': score_count,
        'lineage_count': lineage_count,
        'shortlist_total': shortlist_total,
        'shortlist_selected': shortlist_selected,
        'missing_engine': missing_engine,
        'missing_parent_ids': missing_parent_ids,
        'require_shortlist': require_shortlist,
    }

    if candidate_count < max(1, min_candidates):
        details['error'] = 'candidate_count_below_threshold'
        return False, details
    if score_count < candidate_count:
        details['error'] = 'missing_score_snapshots'
        return False, details
    if lineage_count == 0:
        details['error'] = 'missing_lineage_rows'
        return False, details
    if missing_engine > 0:
        details['error'] = 'missing_engine_id'
        return False, details
    if missing_parent_ids > 0:
        details['error'] = 'missing_parent_ids'
        return False, details
    if require_shortlist and shortlist_selected == 0:
        details['error'] = 'missing_shortlist_selection'
        return False, details
    return True, details


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
    sub.add_parser('source-stats', help='Print source-atom stats.')
    p_assert = sub.add_parser('assert-contract', help='Assert deterministic run contract/provenance.')
    p_assert.add_argument('--run-id', type=int, default=0, help='Run ID to validate (default latest run).')
    p_assert.add_argument('--min-candidates', type=int, default=5, help='Minimum candidate rows expected.')
    p_assert.add_argument(
        '--require-shortlist',
        action='store_true',
        default=True,
        help='Require at least one shortlisted candidate in shortlist_decisions.',
    )
    p_assert.add_argument(
        '--no-require-shortlist',
        dest='require_shortlist',
        action='store_false',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with open_connection(db_path) as conn:
        ensure_schema(conn)

        if args.command == 'init':
            conn.commit()
            print(f'initialized db: {db_path}')
            return 0

        if args.command == 'stats':
            print_stats(conn)
            return 0
        if args.command == 'source-stats':
            print_source_stats(conn)
            return 0
        if args.command == 'assert-contract':
            ok, details = assert_run_contract(
                conn,
                run_id=args.run_id if args.run_id > 0 else None,
                min_candidates=max(1, int(args.min_candidates)),
                require_shortlist=bool(args.require_shortlist),
            )
            print(f'contract_assertion={json.dumps(details, ensure_ascii=False)}')
            return 0 if ok else 1

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
