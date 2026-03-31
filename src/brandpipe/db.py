from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import unicodedata

from .models import NameFamily, RunStatus, SurfacePolicy, SurfacedCandidate

DENSE_EXTERNAL_REASONS = frozenset(
    {
        "tmview_exact_collision",
        "tmview_near_collision",
        "social_handle_crowded",
        "web_exact_collision",
        "web_first_hit_exact",
        "web_near_collision",
    }
)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def open_db(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
          id INTEGER PRIMARY KEY,
          title TEXT NOT NULL,
          brief_json TEXT NOT NULL,
          config_json TEXT NOT NULL,
          batch_id TEXT NOT NULL DEFAULT '',
          batch_index INTEGER,
          metrics_json TEXT NOT NULL DEFAULT '{}',
          status TEXT NOT NULL,
          current_step TEXT NOT NULL,
          error_class TEXT NOT NULL DEFAULT '',
          error_message TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS candidates (
          id INTEGER PRIMARY KEY,
          run_id INTEGER NOT NULL,
          name TEXT NOT NULL,
          name_normalized TEXT NOT NULL,
          display_name TEXT NOT NULL DEFAULT '',
          comparison_name_normalized TEXT NOT NULL DEFAULT '',
          family TEXT NOT NULL DEFAULT 'smooth_blend',
          surface_policy TEXT NOT NULL DEFAULT 'alpha_lower',
          source_kind TEXT NOT NULL,
          source_detail TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE(run_id, name_normalized),
          FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS candidate_results (
          id INTEGER PRIMARY KEY,
          candidate_id INTEGER NOT NULL,
          result_key TEXT NOT NULL,
          status TEXT NOT NULL,
          score_delta REAL NOT NULL,
          reason TEXT NOT NULL,
          details_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(candidate_id, result_key),
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS candidate_rankings (
          id INTEGER PRIMARY KEY,
          candidate_id INTEGER NOT NULL UNIQUE,
          total_score REAL NOT NULL,
          family_score REAL NOT NULL DEFAULT 0.0,
          family_rank INTEGER NOT NULL DEFAULT 0,
          rank_position INTEGER NOT NULL DEFAULT 0,
          blocker_count INTEGER NOT NULL,
          unavailable_count INTEGER NOT NULL,
          unsupported_count INTEGER NOT NULL,
          warning_count INTEGER NOT NULL,
          decision TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS validation_jobs (
          id INTEGER PRIMARY KEY,
          run_id INTEGER NOT NULL,
          candidate_id INTEGER NOT NULL,
          shortlist_fingerprint TEXT NOT NULL DEFAULT '',
          job_order INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL,
          resume_check TEXT NOT NULL DEFAULT '',
          attempt_count INTEGER NOT NULL DEFAULT 0,
          next_retry_at TEXT,
          last_error_kind TEXT NOT NULL DEFAULT '',
          last_error_message TEXT NOT NULL DEFAULT '',
          started_at TEXT,
          finished_at TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(run_id, candidate_id),
          FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS validation_attempts (
          id INTEGER PRIMARY KEY,
          job_id INTEGER NOT NULL,
          run_id INTEGER NOT NULL,
          candidate_id INTEGER NOT NULL,
          check_name TEXT NOT NULL,
          attempt_number INTEGER NOT NULL,
          status TEXT NOT NULL,
          reason TEXT NOT NULL,
          error_kind TEXT NOT NULL DEFAULT '',
          retryable INTEGER NOT NULL DEFAULT 0,
          http_status INTEGER,
          retry_after_s REAL,
          headers_json TEXT NOT NULL DEFAULT '{}',
          evidence_json TEXT NOT NULL DEFAULT '{}',
          details_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          FOREIGN KEY(job_id) REFERENCES validation_jobs(id) ON DELETE CASCADE,
          FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_candidates_run ON candidates(run_id, name);
        CREATE INDEX IF NOT EXISTS idx_candidate_results_candidate ON candidate_results(candidate_id, result_key);
        CREATE INDEX IF NOT EXISTS idx_candidate_rankings_candidate ON candidate_rankings(candidate_id);
        CREATE INDEX IF NOT EXISTS idx_validation_jobs_run_status ON validation_jobs(run_id, status, job_order);
        CREATE INDEX IF NOT EXISTS idx_validation_jobs_retry ON validation_jobs(run_id, next_retry_at, job_order);
        CREATE INDEX IF NOT EXISTS idx_validation_attempts_job_check ON validation_attempts(job_id, check_name, attempt_number);
        """
    )
    try:
        conn.execute("ALTER TABLE runs ADD COLUMN batch_id TEXT NOT NULL DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE runs ADD COLUMN batch_index INTEGER")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE runs ADD COLUMN metrics_json TEXT NOT NULL DEFAULT '{}'")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE candidate_rankings ADD COLUMN unsupported_count INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE candidates ADD COLUMN display_name TEXT NOT NULL DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE candidates ADD COLUMN comparison_name_normalized TEXT NOT NULL DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE candidates ADD COLUMN family TEXT NOT NULL DEFAULT 'smooth_blend'")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE candidates ADD COLUMN surface_policy TEXT NOT NULL DEFAULT 'alpha_lower'")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE candidate_rankings ADD COLUMN family_score REAL NOT NULL DEFAULT 0.0")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE candidate_rankings ADD COLUMN family_rank INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE candidate_rankings ADD COLUMN rank_position INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    conn.execute(
        """
        UPDATE candidates
        SET display_name = CASE
          WHEN TRIM(COALESCE(display_name, '')) = '' THEN name
          ELSE display_name
        END
        """
    )
    conn.execute(
        """
        UPDATE candidates
        SET comparison_name_normalized = CASE
          WHEN TRIM(COALESCE(comparison_name_normalized, '')) = '' THEN name_normalized
          ELSE comparison_name_normalized
        END
        """
    )
    conn.commit()


def create_run(
    conn: sqlite3.Connection,
    *,
    title: str,
    brief: dict[str, object],
    config: dict[str, object],
    batch_id: str = "",
    batch_index: int | None = None,
) -> int:
    timestamp = now_iso()
    cur = conn.execute(
        """
        INSERT INTO runs(title, brief_json, config_json, batch_id, batch_index, metrics_json, status, current_step, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            json.dumps(brief, ensure_ascii=False, sort_keys=True),
            json.dumps(config, ensure_ascii=False, sort_keys=True),
            batch_id,
            batch_index,
            "{}",
            RunStatus.CREATED.value,
            "created",
            timestamp,
            timestamp,
        ),
    )
    return int(cur.lastrowid)


def set_run_state(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    status: str,
    current_step: str,
    error_class: str = "",
    error_message: str = "",
    completed: bool = False,
) -> None:
    completed_at = now_iso() if completed else None
    conn.execute(
        """
        UPDATE runs
        SET status = ?,
            current_step = ?,
            error_class = ?,
            error_message = ?,
            updated_at = ?,
            completed_at = COALESCE(?, completed_at)
        WHERE id = ?
        """,
        (status, current_step, error_class, error_message, now_iso(), completed_at, run_id),
    )


def update_run_metrics(conn: sqlite3.Connection, *, run_id: int, metrics: dict[str, object]) -> None:
    conn.execute(
        """
        UPDATE runs
        SET metrics_json = ?, updated_at = ?
        WHERE id = ?
        """,
        (json.dumps(metrics, ensure_ascii=False, sort_keys=True), now_iso(), run_id),
    )


def add_candidates(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    names: list[str],
    source_kind: str,
    source_detail: str,
) -> None:
    def normalize_surface(name: str) -> str:
        return name.strip().casefold()

    def normalize_compare(raw: str) -> str:
        folded = unicodedata.normalize("NFKD", str(raw or ""))
        plain = "".join(ch for ch in folded if not unicodedata.combining(ch)).lower()
        return re.sub(r"[^a-z0-9]", "", plain)

    timestamp = now_iso()
    conn.executemany(
        """
        INSERT OR IGNORE INTO candidates(
          run_id, name, name_normalized, display_name, comparison_name_normalized,
          family, surface_policy, source_kind, source_detail, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                name,
                normalize_surface(name),
                name,
                normalize_compare(name),
                NameFamily.SMOOTH_BLEND.value,
                SurfacePolicy.ALPHA_LOWER.value,
                source_kind,
                source_detail,
                timestamp,
            )
            for name in names
        ],
    )


def add_candidate_surfaces(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    candidates: Iterable[SurfacedCandidate],
) -> None:
    timestamp = now_iso()
    rows = [
        (
            int(run_id),
            candidate.display_name,
            str(candidate.display_name).strip().casefold(),
            candidate.display_name,
            candidate.name_normalized,
            candidate.family.value,
            candidate.surface_policy.value,
            candidate.source_kind,
            candidate.source_detail,
            timestamp,
        )
        for candidate in candidates
        if str(candidate.display_name).strip()
    ]
    if not rows:
        return
    conn.executemany(
        """
        INSERT OR IGNORE INTO candidates(
          run_id, name, name_normalized, display_name, comparison_name_normalized,
          family, surface_policy, source_kind, source_detail, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def list_candidates(conn: sqlite3.Connection, *, run_id: int) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT
          id,
          run_id,
          name,
          display_name,
          CASE
            WHEN TRIM(COALESCE(comparison_name_normalized, '')) = '' THEN name_normalized
            ELSE comparison_name_normalized
          END AS name_normalized,
          name_normalized AS surface_key,
          family,
          surface_policy,
          source_kind,
          source_detail
        FROM candidates
        WHERE run_id = ?
        ORDER BY name ASC
        """,
        (run_id,),
    ).fetchall()
    return list(rows)


def list_candidates_by_ids(conn: sqlite3.Connection, *, candidate_ids: Iterable[int]) -> list[sqlite3.Row]:
    ids = [int(candidate_id) for candidate_id in candidate_ids]
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT
          id,
          run_id,
          name,
          display_name,
          CASE
            WHEN TRIM(COALESCE(comparison_name_normalized, '')) = '' THEN name_normalized
            ELSE comparison_name_normalized
          END AS name_normalized,
          name_normalized AS surface_key,
          family,
          surface_policy,
          source_kind,
          source_detail
        FROM candidates
        WHERE id IN ({placeholders})
        ORDER BY id ASC
        """,
        tuple(ids),
    ).fetchall()
    return list(rows)


def get_candidate(conn: sqlite3.Connection, *, candidate_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
          id,
          run_id,
          name,
          display_name,
          CASE
            WHEN TRIM(COALESCE(comparison_name_normalized, '')) = '' THEN name_normalized
            ELSE comparison_name_normalized
          END AS name_normalized,
          name_normalized AS surface_key,
          family,
          surface_policy,
          source_kind,
          source_detail
        FROM candidates
        WHERE id = ?
        """,
        (int(candidate_id),),
    ).fetchone()


def upsert_result(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    result_key: str,
    status: str,
    score_delta: float,
    reason: str,
    details: dict[str, object],
) -> None:
    timestamp = now_iso()
    conn.execute(
        """
        INSERT INTO candidate_results(
          candidate_id, result_key, status, score_delta, reason, details_json, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id, result_key) DO UPDATE SET
          status = excluded.status,
          score_delta = excluded.score_delta,
          reason = excluded.reason,
          details_json = excluded.details_json,
          updated_at = excluded.updated_at
        """,
        (
            candidate_id,
            result_key,
            status,
            float(score_delta),
            reason,
            json.dumps(details, ensure_ascii=False, sort_keys=True),
            timestamp,
            timestamp,
        ),
    )


def delete_results_for_run(conn: sqlite3.Connection, *, run_id: int) -> None:
    conn.execute(
        """
        DELETE FROM candidate_results
        WHERE candidate_id IN (SELECT id FROM candidates WHERE run_id = ?)
        """,
        (int(run_id),),
    )
    conn.execute(
        """
        DELETE FROM candidate_rankings
        WHERE candidate_id IN (SELECT id FROM candidates WHERE run_id = ?)
        """,
        (int(run_id),),
    )


def find_latest_run_by_title(conn: sqlite3.Connection, *, title: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT id, title, brief_json, config_json, batch_id, batch_index, metrics_json, status, current_step, error_class, error_message, created_at, updated_at, completed_at
        FROM runs
        WHERE title = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (title,),
    ).fetchone()


def ensure_validation_jobs(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    ordered_candidate_ids: list[int],
    shortlist_fingerprint: str,
) -> None:
    timestamp = now_iso()
    conn.executemany(
        """
        INSERT INTO validation_jobs(
          run_id, candidate_id, shortlist_fingerprint, job_order, status, resume_check,
          attempt_count, next_retry_at, last_error_kind, last_error_message, started_at,
          finished_at, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, 'pending', '', 0, NULL, '', '', NULL, NULL, ?, ?)
        ON CONFLICT(run_id, candidate_id) DO NOTHING
        """,
        [
            (int(run_id), int(candidate_id), shortlist_fingerprint, index, timestamp, timestamp)
            for index, candidate_id in enumerate(ordered_candidate_ids)
        ],
    )


def list_validation_jobs(conn: sqlite3.Connection, *, run_id: int) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT id, run_id, candidate_id, shortlist_fingerprint, job_order, status, resume_check,
               attempt_count, next_retry_at, last_error_kind, last_error_message, started_at,
               finished_at, created_at, updated_at
        FROM validation_jobs
        WHERE run_id = ?
        ORDER BY job_order ASC, id ASC
        """,
        (int(run_id),),
    ).fetchall()
    return list(rows)


def get_validation_job(conn: sqlite3.Connection, *, job_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT id, run_id, candidate_id, shortlist_fingerprint, job_order, status, resume_check,
               attempt_count, next_retry_at, last_error_kind, last_error_message, started_at,
               finished_at, created_at, updated_at
        FROM validation_jobs
        WHERE id = ?
        """,
        (int(job_id),),
    ).fetchone()


def claim_next_validation_job(conn: sqlite3.Connection, *, run_id: int, now: str) -> sqlite3.Row | None:
    row = conn.execute(
        """
        SELECT id
        FROM validation_jobs
        WHERE run_id = ?
          AND (
            status = 'pending'
            OR (status = 'retry_wait' AND (next_retry_at IS NULL OR next_retry_at <= ?))
          )
        ORDER BY job_order ASC, id ASC
        LIMIT 1
        """,
        (int(run_id), now),
    ).fetchone()
    if row is None:
        return None
    job_id = int(row["id"])
    conn.execute(
        """
        UPDATE validation_jobs
        SET status = 'running',
            started_at = COALESCE(started_at, ?),
            updated_at = ?
        WHERE id = ?
        """,
        (now, now, job_id),
    )
    return get_validation_job(conn, job_id=job_id)


def update_validation_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    status: str,
    resume_check: str = "",
    attempt_count: int | None = None,
    next_retry_at: str | None = None,
    last_error_kind: str = "",
    last_error_message: str = "",
    finished: bool = False,
) -> None:
    current = get_validation_job(conn, job_id=job_id)
    if current is None:
        raise RuntimeError(f"validation_job_not_found:{job_id}")
    timestamp = now_iso()
    conn.execute(
        """
        UPDATE validation_jobs
        SET status = ?,
            resume_check = ?,
            attempt_count = ?,
            next_retry_at = ?,
            last_error_kind = ?,
            last_error_message = ?,
            finished_at = CASE WHEN ? THEN ? ELSE finished_at END,
            updated_at = ?
        WHERE id = ?
        """,
        (
            status,
            resume_check,
            int(current["attempt_count"] if attempt_count is None else attempt_count),
            next_retry_at,
            last_error_kind,
            last_error_message,
            1 if finished else 0,
            timestamp,
            timestamp,
            int(job_id),
        ),
    )


def count_validation_jobs(conn: sqlite3.Connection, *, run_id: int) -> Counter[str]:
    counts: Counter[str] = Counter()
    rows = conn.execute(
        """
        SELECT status, COUNT(*) AS total
        FROM validation_jobs
        WHERE run_id = ?
        GROUP BY status
        """,
        (int(run_id),),
    ).fetchall()
    for row in rows:
        counts[str(row["status"])] = int(row["total"])
    return counts


def record_validation_attempt(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    run_id: int,
    candidate_id: int,
    check_name: str,
    attempt_number: int,
    status: str,
    reason: str,
    error_kind: str,
    retryable: bool,
    http_status: int | None,
    retry_after_s: float | None,
    headers: dict[str, object],
    evidence: dict[str, object],
    details: dict[str, object],
) -> None:
    conn.execute(
        """
        INSERT INTO validation_attempts(
          job_id, run_id, candidate_id, check_name, attempt_number, status, reason, error_kind,
          retryable, http_status, retry_after_s, headers_json, evidence_json, details_json, created_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(job_id),
            int(run_id),
            int(candidate_id),
            check_name,
            int(attempt_number),
            status,
            reason,
            error_kind,
            1 if retryable else 0,
            None if http_status is None else int(http_status),
            None if retry_after_s is None else float(retry_after_s),
            json.dumps(headers, ensure_ascii=False, sort_keys=True),
            json.dumps(evidence, ensure_ascii=False, sort_keys=True),
            json.dumps(details, ensure_ascii=False, sort_keys=True),
            now_iso(),
        ),
    )


def fetch_validation_attempts(conn: sqlite3.Connection, *, job_id: int) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT id, job_id, run_id, candidate_id, check_name, attempt_number, status, reason, error_kind,
               retryable, http_status, retry_after_s, headers_json, evidence_json, details_json, created_at
        FROM validation_attempts
        WHERE job_id = ?
        ORDER BY attempt_number ASC, id ASC
        """,
        (int(job_id),),
    ).fetchall()
    return list(rows)


def upsert_ranking(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    total_score: float,
    family_score: float = 0.0,
    family_rank: int = 0,
    rank_position: int = 0,
    blocker_count: int,
    unavailable_count: int,
    unsupported_count: int,
    warning_count: int,
    decision: str,
) -> None:
    timestamp = now_iso()
    conn.execute(
        """
        INSERT INTO candidate_rankings(
          candidate_id, total_score, family_score, family_rank, rank_position,
          blocker_count, unavailable_count, unsupported_count, warning_count, decision, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id) DO UPDATE SET
          total_score = excluded.total_score,
          family_score = excluded.family_score,
          family_rank = excluded.family_rank,
          rank_position = excluded.rank_position,
          blocker_count = excluded.blocker_count,
          unavailable_count = excluded.unavailable_count,
          unsupported_count = excluded.unsupported_count,
          warning_count = excluded.warning_count,
          decision = excluded.decision,
          updated_at = excluded.updated_at
        """,
        (
            candidate_id,
            float(total_score),
            float(family_score),
            int(family_rank),
            int(rank_position),
            int(blocker_count),
            int(unavailable_count),
            int(unsupported_count),
            int(warning_count),
            decision,
            timestamp,
            timestamp,
        ),
    )


def upsert_rankings(
    conn: sqlite3.Connection,
    *,
    rows: Iterable[tuple],
) -> None:
    timestamp = now_iso()
    normalized_rows = []
    for row in rows:
        if len(row) == 7:
            candidate_id, total_score, blocker_count, unavailable_count, unsupported_count, warning_count, decision = row
            family_score = 0.0
            family_rank = 0
            rank_position = 0
        elif len(row) == 10:
            (
                candidate_id,
                total_score,
                family_score,
                family_rank,
                rank_position,
                blocker_count,
                unavailable_count,
                unsupported_count,
                warning_count,
                decision,
            ) = row
        else:
            raise ValueError(f"unsupported_ranking_row_shape:{len(row)}")
        normalized_rows.append(
            (
                int(candidate_id),
                float(total_score),
                float(family_score),
                int(family_rank),
                int(rank_position),
                int(blocker_count),
                int(unavailable_count),
                int(unsupported_count),
                int(warning_count),
                decision,
                timestamp,
                timestamp,
            )
        )
    conn.executemany(
        """
        INSERT INTO candidate_rankings(
          candidate_id, total_score, family_score, family_rank, rank_position,
          blocker_count, unavailable_count, unsupported_count, warning_count, decision, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id) DO UPDATE SET
          total_score = excluded.total_score,
          family_score = excluded.family_score,
          family_rank = excluded.family_rank,
          rank_position = excluded.rank_position,
          blocker_count = excluded.blocker_count,
          unavailable_count = excluded.unavailable_count,
          unsupported_count = excluded.unsupported_count,
          warning_count = excluded.warning_count,
          decision = excluded.decision,
          updated_at = excluded.updated_at
        """,
        normalized_rows,
    )


def list_runs(conn: sqlite3.Connection, *, limit: int = 20, batch_id: str = "") -> list[sqlite3.Row]:
    if batch_id:
        rows = conn.execute(
            """
            SELECT id, title, batch_id, batch_index, metrics_json, status, current_step, error_class, error_message, created_at, updated_at, completed_at
            FROM runs
            WHERE batch_id = ?
            ORDER BY batch_index ASC, id ASC
            LIMIT ?
            """,
            (batch_id, max(1, int(limit))),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, title, batch_id, batch_index, metrics_json, status, current_step, error_class, error_message, created_at, updated_at, completed_at
            FROM runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    return list(rows)


def get_run(conn: sqlite3.Connection, *, run_id: int) -> sqlite3.Row | None:
    row = conn.execute(
        """
        SELECT id, title, brief_json, config_json, batch_id, batch_index, metrics_json, status, current_step, error_class, error_message, created_at, updated_at, completed_at
        FROM runs
        WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    return row


def fetch_results_for_run(conn: sqlite3.Connection, *, run_id: int) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT
          c.name,
          c.display_name,
          CASE
            WHEN TRIM(COALESCE(c.comparison_name_normalized, '')) = '' THEN c.name_normalized
            ELSE c.comparison_name_normalized
          END AS name_normalized,
          c.family,
          c.surface_policy,
          r.result_key,
          r.status,
          r.score_delta,
          r.reason,
          r.details_json
        FROM candidate_results r
        JOIN candidates c ON c.id = r.candidate_id
        WHERE c.run_id = ?
        ORDER BY c.name ASC, r.result_key ASC
        """,
        (run_id,),
    ).fetchall()
    return list(rows)


def fetch_results_for_candidate(conn: sqlite3.Connection, *, candidate_id: int) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT
          c.name,
          c.display_name,
          CASE
            WHEN TRIM(COALESCE(c.comparison_name_normalized, '')) = '' THEN c.name_normalized
            ELSE c.comparison_name_normalized
          END AS name_normalized,
          c.family,
          c.surface_policy,
          r.result_key,
          r.status,
          r.score_delta,
          r.reason,
          r.details_json
        FROM candidate_results r
        JOIN candidates c ON c.id = r.candidate_id
        WHERE c.id = ?
        ORDER BY r.result_key ASC
        """,
        (candidate_id,),
    ).fetchall()
    return list(rows)


def fetch_ranked_rows(conn: sqlite3.Connection, *, run_id: int, limit: int = 25) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT
          c.name,
          c.display_name,
          CASE
            WHEN TRIM(COALESCE(c.comparison_name_normalized, '')) = '' THEN c.name_normalized
            ELSE c.comparison_name_normalized
          END AS name_normalized,
          c.family,
          c.surface_policy,
          r.total_score,
          r.family_score,
          r.family_rank,
          r.rank_position,
          r.blocker_count,
          r.unavailable_count,
          r.unsupported_count,
          r.warning_count,
          r.decision
        FROM candidate_rankings r
        JOIN candidates c ON c.id = r.candidate_id
        WHERE c.run_id = ?
        ORDER BY
          CASE WHEN r.rank_position > 0 THEN 0 ELSE 1 END ASC,
          CASE WHEN r.rank_position > 0 THEN r.rank_position ELSE 999999 END ASC,
          r.blocker_count ASC,
          r.unavailable_count ASC,
          r.unsupported_count ASC,
          r.warning_count ASC,
          r.total_score DESC,
          c.name ASC
        LIMIT ?
        """,
        (run_id, max(1, int(limit))),
    ).fetchall()
    return list(rows)


def count_ranked_rows(conn: sqlite3.Connection, *, run_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM candidate_rankings r
        JOIN candidates c ON c.id = r.candidate_id
        WHERE c.run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        return 0
    return int(row["total"])


def fetch_pending_web_rows(
    conn: sqlite3.Connection,
    *,
    run_id: int | None = None,
    batch_id: str = "",
    limit: int = 100,
) -> list[sqlite3.Row]:
    query = """
        SELECT
          c.id AS candidate_id,
          c.run_id AS run_id,
          c.name AS name,
          rk.decision AS decision,
          cr.status AS web_status,
          cr.reason AS web_reason
        FROM candidate_results cr
        JOIN candidates c ON c.id = cr.candidate_id
        JOIN candidate_rankings rk ON rk.candidate_id = c.id
        JOIN runs r ON r.id = c.run_id
        WHERE cr.result_key = 'web'
          AND (
            (cr.status = 'warn' AND cr.reason = 'web_check_pending' AND rk.decision = 'watch')
            OR
            (cr.status = 'unavailable' AND cr.reason = 'web_search_unavailable')
          )
    """
    params: list[object] = []
    if run_id is not None:
        query += " AND c.run_id = ?"
        params.append(int(run_id))
    if str(batch_id).strip():
        query += " AND r.batch_id = ?"
        params.append(str(batch_id).strip())
    query += """
        ORDER BY c.run_id ASC, c.id ASC
        LIMIT ?
    """
    params.append(max(1, int(limit)))
    rows = conn.execute(query, tuple(params)).fetchall()
    return list(rows)


def fetch_tmview_recheck_rows(
    conn: sqlite3.Connection,
    *,
    run_id: int | None = None,
    batch_id: str = "",
    limit: int = 25,
    force: bool = False,
) -> list[sqlite3.Row]:
    query = """
        SELECT
          c.id AS candidate_id,
          c.run_id AS run_id,
          c.name AS name,
          rk.decision AS decision,
          rk.blocker_count AS blocker_count,
          rk.unavailable_count AS unavailable_count,
          rk.unsupported_count AS unsupported_count,
          rk.warning_count AS warning_count,
          rk.total_score AS total_score,
          tr.status AS tmview_status,
          tr.reason AS tmview_reason
        FROM candidate_rankings rk
        JOIN candidates c ON c.id = rk.candidate_id
        JOIN runs r ON r.id = c.run_id
        LEFT JOIN candidate_results tr
          ON tr.candidate_id = c.id
         AND tr.result_key = 'tmview'
        WHERE rk.decision IN ('candidate', 'watch')
    """
    params: list[object] = []
    if not force:
        query += " AND tr.id IS NULL"
    if run_id is not None:
        query += " AND c.run_id = ?"
        params.append(int(run_id))
    if str(batch_id).strip():
        query += " AND r.batch_id = ?"
        params.append(str(batch_id).strip())
    query += """
        ORDER BY
          c.run_id ASC,
          rk.blocker_count ASC,
          rk.unavailable_count ASC,
          rk.unsupported_count ASC,
          rk.warning_count ASC,
          rk.total_score DESC,
          c.name ASC
        LIMIT ?
    """
    params.append(max(1, int(limit)))
    rows = conn.execute(query, tuple(params)).fetchall()
    return list(rows)


def recent_ranked_name_corpus(
    conn: sqlite3.Connection,
    *,
    run_window: int = 24,
    limit: int = 256,
    exclude_batch_id: str = "",
) -> list[dict[str, object]]:
    query = """
        SELECT c.name, cr.decision, c.run_id
        FROM candidate_rankings cr
        JOIN candidates c ON c.id = cr.candidate_id
        JOIN runs r ON r.id = c.run_id
        WHERE r.status = ?
    """
    params: list[object] = [RunStatus.COMPLETED.value]
    if str(exclude_batch_id).strip():
        query += " AND r.batch_id != ?"
        params.append(str(exclude_batch_id).strip())
    query += """
        ORDER BY c.run_id DESC, c.id DESC
        LIMIT ?
    """
    params.append(max(1, int(run_window) * max(1, int(limit))))
    rows = conn.execute(query, tuple(params)).fetchall()
    corpus: list[dict[str, object]] = []
    seen_runs: set[int] = set()
    for row in rows:
        run_id = int(row["run_id"])
        if len(seen_runs) >= max(1, int(run_window)) and run_id not in seen_runs:
            continue
        seen_runs.add(run_id)
        corpus.append(
            {
                "name": str(row["name"]).strip(),
                "decision": str(row["decision"]).strip(),
                "run_id": run_id,
            }
        )
        if len(corpus) >= max(1, int(limit)):
            break
    return corpus


def recent_external_fail_name_corpus(
    conn: sqlite3.Connection,
    *,
    run_window: int = 24,
    limit: int = 256,
    exclude_batch_id: str = "",
) -> list[dict[str, object]]:
    query = """
        SELECT c.name, r.reason, c.run_id
        FROM candidate_results r
        JOIN candidates c ON c.id = r.candidate_id
        JOIN runs run ON run.id = c.run_id
        WHERE run.status = ?
          AND r.status = 'fail'
          AND r.result_key != 'attractiveness'
    """
    params: list[object] = [RunStatus.COMPLETED.value]
    if str(exclude_batch_id).strip():
        query += " AND run.batch_id != ?"
        params.append(str(exclude_batch_id).strip())
    query += """
        ORDER BY c.run_id DESC, r.id DESC
        LIMIT ?
    """
    params.append(max(1, int(run_window) * max(1, int(limit))))
    rows = conn.execute(query, tuple(params)).fetchall()

    corpus: list[dict[str, object]] = []
    seen_runs: set[int] = set()
    seen_names: set[str] = set()
    for row in rows:
        run_id = int(row["run_id"])
        if len(seen_runs) >= max(1, int(run_window)) and run_id not in seen_runs:
            continue
        seen_runs.add(run_id)
        name = str(row["name"]).strip()
        if not name:
            continue
        normalized = name.casefold()
        if normalized in seen_names:
            continue
        seen_names.add(normalized)
        corpus.append(
            {
                "name": name,
                "decision": "external_fail",
                "reason": str(row["reason"] or "").strip(),
                "run_id": run_id,
            }
        )
        if len(corpus) >= max(1, int(limit)):
            break
    return corpus


def recent_avoidance_feedback(
    conn: sqlite3.Connection,
    *,
    run_window: int = 12,
    max_local_examples: int = 6,
    max_external_reasons: int = 4,
    max_examples_per_reason: int = 3,
    exclude_batch_id: str = "",
) -> dict[str, object]:
    recent_runs = list_runs(conn, limit=max(1, int(run_window)))
    local_examples: list[dict[str, str]] = []
    recent_run_ids: list[int] = []
    for row in recent_runs:
        if str(exclude_batch_id).strip() and str(row["batch_id"] or "").strip() == str(exclude_batch_id).strip():
            continue
        run_id = int(row["id"])
        recent_run_ids.append(run_id)
        if len(recent_run_ids) > max(1, int(run_window)):
            break
        try:
            metrics = json.loads(str(row["metrics_json"] or "{}"))
        except json.JSONDecodeError:
            metrics = {}
        dropped_examples = (((metrics.get("ideation") or {}).get("local_filter") or {}).get("dropped_examples") or {})
        if not isinstance(dropped_examples, dict):
            continue
        for reason, examples in dropped_examples.items():
            if not isinstance(examples, list):
                continue
            for example in examples:
                text = str(example).strip()
                if not text:
                    continue
                local_examples.append({"reason": str(reason).strip(), "example": text})

    deduped_local: list[dict[str, str]] = []
    seen_local: set[tuple[str, str]] = set()
    for item in local_examples:
        key = (item["reason"], item["example"].casefold())
        if key in seen_local:
            continue
        seen_local.add(key)
        deduped_local.append(item)
        if len(deduped_local) >= max(1, int(max_local_examples)):
            break

    local_prefix_counts: Counter[str] = Counter()
    local_suffix_counts: Counter[str] = Counter()
    for item in local_examples:
        sample = str(item.get("example") or "").strip()
        if ":" in sample:
            sample = sample.split(":", 1)[0]
        normalized = _normalize_pattern_name(sample)
        if len(normalized) < 6:
            continue
        local_prefix_counts[normalized[:4]] += 1
        local_suffix_counts[normalized[-3:]] += 1
    local_patterns = {
        "prefixes": [
            prefix
            for prefix, count in local_prefix_counts.most_common(6)
            if count >= 2
        ][:4],
        "suffixes": [
            suffix
            for suffix, count in local_suffix_counts.most_common(6)
            if count >= 2
        ][:4],
    }

    external_failures: dict[str, list[str]] = {}
    external_patterns: dict[str, list[str]] = {}
    external_terminal_skeletons: list[str] = []
    external_terminal_families: list[str] = []
    external_lead_hints: list[str] = []
    external_tail_hints: list[str] = []
    external_fragment_hints: list[str] = []
    external_reason_patterns: dict[str, dict[str, object]] = {}
    external_avoid_names: list[str] = []
    if recent_run_ids:
        placeholders = ",".join("?" for _ in recent_run_ids)
        rows = conn.execute(
            f"""
            SELECT c.name, r.reason
            FROM candidate_results r
            JOIN candidates c ON c.id = r.candidate_id
            WHERE c.run_id IN ({placeholders})
              AND r.status = 'fail'
            ORDER BY c.run_id DESC, r.id DESC
            """,
            tuple(recent_run_ids),
        ).fetchall()
        reason_counts: Counter[str] = Counter()
        reason_examples: dict[str, list[str]] = {}
        seen_by_reason: dict[str, set[str]] = {}
        dense_reason_names: dict[str, list[str]] = {}
        seen_dense_reason_names: dict[str, set[str]] = {}
        unique_failed_names: list[str] = []
        seen_failed_names: set[str] = set()
        dense_failed_names: list[str] = []
        seen_dense_failed_names: set[str] = set()
        for row in rows:
            reason = str(row["reason"] or "").strip()
            name = str(row["name"] or "").strip()
            if not reason or not name:
                continue
            reason_counts[reason] += 1
            if name.casefold() not in seen_failed_names:
                seen_failed_names.add(name.casefold())
                unique_failed_names.append(name)
            if reason in DENSE_EXTERNAL_REASONS and name.casefold() not in seen_dense_failed_names:
                seen_dense_failed_names.add(name.casefold())
                dense_failed_names.append(name)
            if reason in DENSE_EXTERNAL_REASONS:
                seen_dense_reason_names.setdefault(reason, set())
                if name.casefold() not in seen_dense_reason_names[reason]:
                    seen_dense_reason_names[reason].add(name.casefold())
                    dense_reason_names.setdefault(reason, []).append(name)
            seen_by_reason.setdefault(reason, set())
            if name.casefold() in seen_by_reason[reason]:
                continue
            seen_by_reason[reason].add(name.casefold())
            reason_examples.setdefault(reason, []).append(name)
        for reason, _count in reason_counts.most_common(max(1, int(max_external_reasons))):
            examples = reason_examples.get(reason) or []
            if not examples:
                continue
            external_failures[reason] = examples[: max(1, int(max_examples_per_reason))]

        pattern_source_names = dense_failed_names or unique_failed_names
        prefix_counts: Counter[str] = Counter()
        suffix_counts: Counter[str] = Counter()
        for name in pattern_source_names:
            normalized = _normalize_pattern_name(name)
            if len(normalized) < 6:
                continue
            prefix_counts[normalized[:3]] += 1
            suffix_counts[normalized[-3:]] += 1
        external_patterns = {
            "prefixes": [
                prefix
                for prefix, count in prefix_counts.most_common(6)
                if count >= 2
            ][:4],
            "suffixes": [
                suffix
                for suffix, count in suffix_counts.most_common(6)
                if count >= 2
            ][:4],
        }
        skeleton_counts: Counter[str] = Counter()
        for name in dense_failed_names:
            skeleton = _terminal_skeleton(name)
            if len(skeleton) >= 2:
                skeleton_counts[skeleton] += 1
        external_terminal_skeletons = [
            skeleton
            for skeleton, count in skeleton_counts.most_common(8)
            if count >= 2
        ][:4]
        lead_fragment_counts: Counter[str] = Counter()
        tail_fragment_counts: Counter[str] = Counter()
        family_counts: Counter[str] = Counter()
        for name in dense_failed_names or unique_failed_names:
            normalized = _normalize_pattern_name(name)
            if len(normalized) < 5:
                continue
            lead_fragment = normalized[:5] if len(normalized) >= 7 else normalized[:4]
            tail_fragment = normalized[-4:] if len(normalized) >= 7 else normalized[-3:]
            if len(lead_fragment) >= 4:
                lead_fragment_counts[lead_fragment] += 1
            if len(tail_fragment) >= 3:
                tail_fragment_counts[tail_fragment] += 1
            family = normalized[-2:] if len(normalized) >= 6 else normalized[-1:]
            if len(family) >= 2:
                family_counts[family] += 1
        external_lead_hints = [
            fragment
            for fragment, _count in lead_fragment_counts.most_common(8)
        ][:6]
        external_tail_hints = [
            fragment
            for fragment, _count in tail_fragment_counts.most_common(8)
        ][:6]
        external_terminal_families = [
            family
            for family, count in family_counts.most_common(8)
            if count >= 2
        ][:4]
        seen_external_fragments: set[str] = set()
        for fragment in [*external_lead_hints, *external_tail_hints]:
            if fragment in seen_external_fragments:
                continue
            seen_external_fragments.add(fragment)
            external_fragment_hints.append(fragment)
            if len(external_fragment_hints) >= 8:
                break

        ordered_dense_reasons = [
            reason
            for reason, _count in reason_counts.most_common(max(1, int(max_external_reasons)))
            if reason in DENSE_EXTERNAL_REASONS
        ]
        seen_avoid_names: set[str] = set()
        for reason in ordered_dense_reasons:
            names = dense_reason_names.get(reason) or []
            if not names:
                continue
            prefix_counts: Counter[str] = Counter()
            suffix_counts: Counter[str] = Counter()
            lead_counts: Counter[str] = Counter()
            tail_counts: Counter[str] = Counter()
            family_counts_reason: Counter[str] = Counter()
            for name in names:
                normalized = _normalize_pattern_name(name)
                if len(normalized) < 5:
                    continue
                prefix_counts[normalized[:3]] += 1
                suffix_counts[normalized[-3:]] += 1
                lead_fragment = normalized[:5] if len(normalized) >= 7 else normalized[:4]
                tail_fragment = normalized[-4:] if len(normalized) >= 7 else normalized[-3:]
                if len(lead_fragment) >= 4:
                    lead_counts[lead_fragment] += 1
                if len(tail_fragment) >= 3:
                    tail_counts[tail_fragment] += 1
                family = normalized[-2:] if len(normalized) >= 6 else normalized[-1:]
                if len(family) >= 2:
                    family_counts_reason[family] += 1
            external_reason_patterns[reason] = {
                "examples": names[: max(1, min(2, int(max_examples_per_reason)))],
                "prefixes": [prefix for prefix, _count in prefix_counts.most_common(4)][:2],
                "suffixes": [suffix for suffix, _count in suffix_counts.most_common(4)][:2],
                "lead_hints": [fragment for fragment, _count in lead_counts.most_common(4)][:2],
                "tail_hints": [fragment for fragment, _count in tail_counts.most_common(4)][:2],
                "terminal_families": [family for family, count in family_counts_reason.most_common(4) if count >= 1][:2],
            }
            for name in names[:3]:
                normalized_name = str(name).strip().casefold()
                if normalized_name in seen_avoid_names:
                    continue
                seen_avoid_names.add(normalized_name)
                external_avoid_names.append(str(name).strip())
                if len(external_avoid_names) >= 10:
                    break
            if len(external_avoid_names) >= 10:
                break

    return {
        "run_ids": recent_run_ids[: max(1, int(run_window))],
        "local_examples": deduped_local,
        "local_patterns": local_patterns,
        "external_failures": external_failures,
        "external_patterns": external_patterns,
        "external_terminal_skeletons": external_terminal_skeletons,
        "external_terminal_families": external_terminal_families,
        "external_lead_hints": external_lead_hints,
        "external_tail_hints": external_tail_hints,
        "external_fragment_hints": external_fragment_hints,
        "external_reason_patterns": external_reason_patterns,
        "external_avoid_names": external_avoid_names,
    }


def _normalize_pattern_name(raw: object) -> str:
    return re.sub(r"[^a-z]", "", str(raw or "").strip().lower())


def _terminal_skeleton(raw: object) -> str:
    normalized = _normalize_pattern_name(raw)
    if not normalized:
        return ""
    window = normalized[-3:] if len(normalized) >= 3 else normalized
    skeleton = re.sub(r"[aeiouy]", "", window)
    if len(skeleton) < 2 and len(normalized) >= 4:
        skeleton = re.sub(r"[aeiouy]", "", normalized[-4:])
    return skeleton[:3]


def recent_positive_feedback(
    conn: sqlite3.Connection,
    *,
    run_window: int = 12,
    max_names: int = 4,
    exclude_batch_id: str = "",
) -> dict[str, object]:
    query = """
        SELECT c.name, cr.decision, c.run_id
        FROM candidate_rankings cr
        JOIN candidates c ON c.id = cr.candidate_id
        JOIN runs r ON r.id = c.run_id
        JOIN candidate_results ar
          ON ar.candidate_id = c.id
         AND ar.result_key = 'attractiveness'
         AND ar.status = 'pass'
         AND ar.score_delta >= 18.0
        WHERE r.status = ?
          AND cr.decision = 'candidate'
          AND cr.blocker_count = 0
          AND cr.warning_count = 0
          AND cr.unavailable_count = 0
          AND cr.unsupported_count = 0
          AND (
            SELECT COUNT(DISTINCT vr.result_key)
            FROM candidate_results vr
            WHERE vr.candidate_id = c.id
              AND vr.result_key != 'attractiveness'
              AND vr.status = 'pass'
          ) >= 3
    """
    params: list[object] = [RunStatus.COMPLETED.value]
    if str(exclude_batch_id).strip():
        query += " AND r.batch_id != ?"
        params.append(str(exclude_batch_id).strip())
    query += """
        ORDER BY c.run_id DESC, c.id DESC
        LIMIT ?
    """
    params.append(max(1, int(run_window) * max(1, int(max_names)) * 2))
    rows = conn.execute(query, tuple(params)).fetchall()

    names: list[str] = []
    endings: list[str] = []
    run_ids: list[int] = []
    seen_names: set[str] = set()
    seen_endings: set[str] = set()
    seen_runs: set[int] = set()
    for row in rows:
        run_id = int(row["run_id"])
        if len(seen_runs) >= max(1, int(run_window)) and run_id not in seen_runs:
            continue
        seen_runs.add(run_id)
        if run_id not in run_ids:
            run_ids.append(run_id)
        name = str(row["name"] or "").strip().lower()
        if not name or name in seen_names:
            continue
        seen_names.add(name)
        names.append(name)
        if len(name) >= 3:
            ending = name[-3:]
            if ending not in seen_endings:
                seen_endings.add(ending)
                endings.append(ending)
        if len(names) >= max(1, int(max_names)):
            break

    return {
        "run_ids": run_ids[: max(1, int(run_window))],
        "names": names,
        "endings": endings[: max(1, min(4, int(max_names)))],
    }


def recent_blocked_patterns(
    conn: sqlite3.Connection,
    *,
    run_window: int = 12,
    suffix_limit: int = 4,
    stem_limit: int = 3,
    min_occurrences: int = 2,
) -> dict[str, object]:
    recent_runs = conn.execute(
        """
        SELECT id
        FROM runs
        WHERE status = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (RunStatus.COMPLETED.value, max(1, int(run_window))),
    ).fetchall()
    run_ids = [int(row["id"]) for row in recent_runs]
    if not run_ids:
        return {
            "run_ids": [],
            "blocked_names": [],
            "suffixes": [],
            "stems": [],
        }

    placeholders = ",".join("?" for _ in run_ids)
    rows = conn.execute(
        f"""
        SELECT c.name
        FROM candidate_rankings cr
        JOIN candidates c ON c.id = cr.candidate_id
        WHERE c.run_id IN ({placeholders})
          AND cr.decision = 'blocked'
        ORDER BY c.run_id DESC, c.id DESC
        """,
        tuple(run_ids),
    ).fetchall()
    blocked_names = [_normalize_pattern_name(row["name"]) for row in rows]
    blocked_names = [name for name in blocked_names if len(name) >= 6]
    if not blocked_names:
        return {
            "run_ids": run_ids,
            "blocked_names": [],
            "suffixes": [],
            "stems": [],
        }

    suffix_counts: Counter[str] = Counter()
    stem_counts: Counter[str] = Counter()
    for name in blocked_names:
        suffix = name[-4:] if len(name) >= 7 else name[-3:]
        if len(suffix) >= 3:
            suffix_counts[suffix] += 1
        stem = name[:5]
        if len(stem) >= 4:
            stem_counts[stem] += 1

    suffixes = [
        suffix
        for suffix, count in suffix_counts.most_common(max(1, int(suffix_limit)) * 2)
        if count >= max(1, int(min_occurrences))
    ][: max(1, int(suffix_limit))]
    stems = [
        stem
        for stem, count in stem_counts.most_common(max(1, int(stem_limit)) * 2)
        if count >= max(1, int(min_occurrences))
    ][: max(1, int(stem_limit))]
    return {
        "run_ids": run_ids,
        "blocked_names": blocked_names[:24],
        "suffixes": suffixes,
        "stems": stems,
    }
