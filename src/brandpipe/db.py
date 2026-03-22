from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .models import RunStatus

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
          blocker_count INTEGER NOT NULL,
          unavailable_count INTEGER NOT NULL,
          unsupported_count INTEGER NOT NULL,
          warning_count INTEGER NOT NULL,
          decision TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_candidates_run ON candidates(run_id, name);
        CREATE INDEX IF NOT EXISTS idx_candidate_results_candidate ON candidate_results(candidate_id, result_key);
        CREATE INDEX IF NOT EXISTS idx_candidate_rankings_candidate ON candidate_rankings(candidate_id);
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
    def normalize(name: str) -> str:
        return name.strip().casefold()

    timestamp = now_iso()
    conn.executemany(
        """
        INSERT OR IGNORE INTO candidates(run_id, name, name_normalized, source_kind, source_detail, created_at)
        VALUES(?, ?, ?, ?, ?, ?)
        """,
        [(run_id, name, normalize(name), source_kind, source_detail, timestamp) for name in names],
    )


def list_candidates(conn: sqlite3.Connection, *, run_id: int) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT id, name, source_kind, source_detail
        FROM candidates
        WHERE run_id = ?
        ORDER BY name ASC
        """,
        (run_id,),
    ).fetchall()
    return list(rows)


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


def upsert_ranking(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    total_score: float,
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
          candidate_id, total_score, blocker_count, unavailable_count, unsupported_count, warning_count, decision, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id) DO UPDATE SET
          total_score = excluded.total_score,
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
    rows: Iterable[tuple[int, float, int, int, int, int, str]],
) -> None:
    timestamp = now_iso()
    conn.executemany(
        """
        INSERT INTO candidate_rankings(
          candidate_id, total_score, blocker_count, unavailable_count, unsupported_count, warning_count, decision, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id) DO UPDATE SET
          total_score = excluded.total_score,
          blocker_count = excluded.blocker_count,
          unavailable_count = excluded.unavailable_count,
          unsupported_count = excluded.unsupported_count,
          warning_count = excluded.warning_count,
          decision = excluded.decision,
          updated_at = excluded.updated_at
        """,
        [
            (
                int(candidate_id),
                float(total_score),
                int(blocker_count),
                int(unavailable_count),
                int(unsupported_count),
                int(warning_count),
                decision,
                timestamp,
                timestamp,
            )
            for candidate_id, total_score, blocker_count, unavailable_count, unsupported_count, warning_count, decision in rows
        ],
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
        SELECT c.name, r.result_key, r.status, r.score_delta, r.reason, r.details_json
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
        SELECT c.name, r.result_key, r.status, r.score_delta, r.reason, r.details_json
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
          r.total_score,
          r.blocker_count,
          r.unavailable_count,
          r.unsupported_count,
          r.warning_count,
          r.decision
        FROM candidate_rankings r
        JOIN candidates c ON c.id = r.candidate_id
        WHERE c.run_id = ?
        ORDER BY r.blocker_count ASC, r.unavailable_count ASC, r.unsupported_count ASC, r.warning_count ASC, r.total_score DESC, c.name ASC
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
