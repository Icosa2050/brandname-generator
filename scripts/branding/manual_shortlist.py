#!/usr/bin/env python3
"""Manage manually cleared shortlist candidates for later review."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


ROOT_DIR = Path(__file__).resolve().parents[2]
CURATED_DIR = ROOT_DIR / "test_outputs" / "branding" / "curated"
DEFAULT_DB_PATH = CURATED_DIR / "validated_names.db"
DEFAULT_CSV_PATH = CURATED_DIR / "manual_shortlist.csv"
DEFAULT_MD_PATH = CURATED_DIR / "manual_shortlist.md"


@dataclass(frozen=True)
class ShortlistEntry:
    name: str
    status: str
    recommendation: str
    source_title: str
    source_lane: str
    source_run_id: int
    total_score: float
    finding_summary: str
    watchouts: str
    watch_variants: str
    evidence_refs: str
    notes: str
    created_at: str
    updated_at: str


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS manual_shortlist (
    name TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    recommendation TEXT NOT NULL,
    source_title TEXT NOT NULL DEFAULT '',
    source_lane TEXT NOT NULL DEFAULT '',
    source_run_id INTEGER NOT NULL DEFAULT 0,
    total_score REAL NOT NULL DEFAULT 0,
    finding_summary TEXT NOT NULL DEFAULT '',
    watchouts TEXT NOT NULL DEFAULT '',
    watch_variants TEXT NOT NULL DEFAULT '',
    evidence_refs TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def upsert_entry(conn: sqlite3.Connection, entry: ShortlistEntry) -> None:
    conn.execute(
        """
        INSERT INTO manual_shortlist (
            name,
            status,
            recommendation,
            source_title,
            source_lane,
            source_run_id,
            total_score,
            finding_summary,
            watchouts,
            watch_variants,
            evidence_refs,
            notes,
            created_at,
            updated_at
        )
        VALUES (
            :name,
            :status,
            :recommendation,
            :source_title,
            :source_lane,
            :source_run_id,
            :total_score,
            :finding_summary,
            :watchouts,
            :watch_variants,
            :evidence_refs,
            :notes,
            :created_at,
            :updated_at
        )
        ON CONFLICT(name) DO UPDATE SET
            status = excluded.status,
            recommendation = excluded.recommendation,
            source_title = excluded.source_title,
            source_lane = excluded.source_lane,
            source_run_id = excluded.source_run_id,
            total_score = excluded.total_score,
            finding_summary = excluded.finding_summary,
            watchouts = excluded.watchouts,
            watch_variants = excluded.watch_variants,
            evidence_refs = excluded.evidence_refs,
            notes = excluded.notes,
            updated_at = excluded.updated_at
        """,
        asdict(entry),
    )


def load_entries(conn: sqlite3.Connection) -> list[ShortlistEntry]:
    rows = conn.execute(
        """
        SELECT
            name,
            status,
            recommendation,
            source_title,
            source_lane,
            source_run_id,
            total_score,
            finding_summary,
            watchouts,
            watch_variants,
            evidence_refs,
            notes,
            created_at,
            updated_at
        FROM manual_shortlist
        ORDER BY
            CASE status
                WHEN 'go' THEN 0
                WHEN 'watch' THEN 1
                WHEN 'hold' THEN 2
                ELSE 3
            END,
            total_score DESC,
            name ASC
        """
    ).fetchall()
    return [ShortlistEntry(*row) for row in rows]


def export_csv(path: Path, entries: Iterable[ShortlistEntry]) -> None:
    rows = [asdict(entry) for entry in entries]
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(ShortlistEntry.__dataclass_fields__.keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def export_markdown(path: Path, entries: Iterable[ShortlistEntry]) -> None:
    rows = list(entries)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Manual Shortlist")
    lines.append("")
    lines.append(f"- Exported: {utc_now()}")
    lines.append(f"- Entries: {len(rows)}")
    lines.append("")
    if not rows:
        lines.append("_No entries yet._")
    for entry in rows:
        lines.append(f"## {entry.name}")
        lines.append("")
        lines.append(f"- Status: `{entry.status}`")
        lines.append(f"- Recommendation: `{entry.recommendation}`")
        if entry.source_title:
            lines.append(
                f"- Source: run `{entry.source_run_id}` `{entry.source_title}`"
                + (f" lane `{entry.source_lane}`" if entry.source_lane else "")
            )
        if entry.total_score:
            lines.append(f"- Score: `{entry.total_score:.1f}`")
        if entry.finding_summary:
            lines.append(f"- Findings: {entry.finding_summary}")
        if entry.watchouts:
            lines.append(f"- Watchouts: {entry.watchouts}")
        if entry.watch_variants:
            lines.append(f"- Watch variants: {entry.watch_variants}")
        if entry.evidence_refs:
            lines.append(f"- Evidence: {entry.evidence_refs}")
        if entry.notes:
            lines.append(f"- Notes: {entry.notes}")
        lines.append(f"- Updated: `{entry.updated_at}`")
        lines.append("")
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def export_outputs(conn: sqlite3.Connection, csv_path: Path, md_path: Path) -> list[ShortlistEntry]:
    entries = load_entries(conn)
    export_csv(csv_path, entries)
    export_markdown(md_path, entries)
    return entries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage manually cleared shortlist candidates.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite DB path.")
    parser.add_argument("--csv-path", default=str(DEFAULT_CSV_PATH), help="CSV export path.")
    parser.add_argument("--md-path", default=str(DEFAULT_MD_PATH), help="Markdown export path.")

    sub = parser.add_subparsers(dest="command", required=True)

    add = sub.add_parser("add", help="Insert or update one shortlist entry.")
    add.add_argument("--name", required=True)
    add.add_argument("--status", default="go", choices=["go", "watch", "hold"])
    add.add_argument("--recommendation", default="finalist")
    add.add_argument("--source-title", default="")
    add.add_argument("--source-lane", default="")
    add.add_argument("--source-run-id", type=int, default=0)
    add.add_argument("--total-score", type=float, default=0.0)
    add.add_argument("--finding-summary", default="")
    add.add_argument("--watchouts", default="")
    add.add_argument("--watch-variants", default="")
    add.add_argument("--evidence-refs", default="")
    add.add_argument("--notes", default="")

    sub.add_parser("export", help="Refresh CSV and Markdown exports from the DB.")
    sub.add_parser("list", help="Print current shortlist rows.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    csv_path = Path(args.csv_path).expanduser().resolve()
    md_path = Path(args.md_path).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        ensure_schema(conn)
        if args.command == "add":
            now = utc_now()
            existing = conn.execute(
                "SELECT created_at FROM manual_shortlist WHERE name = ?",
                (str(args.name).strip().lower(),),
            ).fetchone()
            entry = ShortlistEntry(
                name=str(args.name).strip().lower(),
                status=args.status,
                recommendation=args.recommendation,
                source_title=args.source_title,
                source_lane=args.source_lane,
                source_run_id=max(0, int(args.source_run_id)),
                total_score=float(args.total_score),
                finding_summary=args.finding_summary.strip(),
                watchouts=args.watchouts.strip(),
                watch_variants=args.watch_variants.strip(),
                evidence_refs=args.evidence_refs.strip(),
                notes=args.notes.strip(),
                created_at=str(existing[0]) if existing else now,
                updated_at=now,
            )
            upsert_entry(conn, entry)
            conn.commit()
            entries = export_outputs(conn, csv_path, md_path)
            print(
                f"manual_shortlist upserted name={entry.name} status={entry.status} "
                f"entries={len(entries)} csv={csv_path} md={md_path}"
            )
            return 0
        if args.command == "export":
            entries = export_outputs(conn, csv_path, md_path)
            print(f"manual_shortlist exported entries={len(entries)} csv={csv_path} md={md_path}")
            return 0
        entries = load_entries(conn)
        for entry in entries:
            print(
                f"{entry.name}\t{entry.status}\t{entry.recommendation}\t"
                f"{entry.total_score:.1f}\t{entry.updated_at}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
