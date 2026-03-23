#!/usr/bin/env python3
"""Tests for manual_shortlist.py."""

from __future__ import annotations

import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

import manual_shortlist as ms


class ManualShortlistTest(unittest.TestCase):
    def test_upsert_and_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "validated_names.db"
            csv_path = root / "manual_shortlist.csv"
            md_path = root / "manual_shortlist.md"

            with sqlite3.connect(db_path) as conn:
                ms.ensure_schema(conn)
                created_at = "2026-03-23T00:00:00Z"
                entry = ms.ShortlistEntry(
                    name="fesigan",
                    status="go",
                    recommendation="finalist",
                    source_title="openrouter-attack-short:maintenance-billing",
                    source_lane="short",
                    source_run_id=9,
                    total_score=120.0,
                    finding_summary="Clear knockout screen.",
                    watchouts="Monitor FEIGEN/FEDIGAN neighbors.",
                    watch_variants="fesigen,fedigan",
                    evidence_refs="a.json; b.url",
                    notes="Screening only.",
                    created_at=created_at,
                    updated_at=created_at,
                )
                ms.upsert_entry(conn, entry)
                conn.commit()
                rows = ms.export_outputs(conn, csv_path, md_path)

            self.assertEqual(len(rows), 1)
            self.assertTrue(csv_path.exists())
            self.assertTrue(md_path.exists())

            with csv_path.open(newline="", encoding="utf-8") as handle:
                records = list(csv.DictReader(handle))
            self.assertEqual(records[0]["name"], "fesigan")
            self.assertEqual(records[0]["status"], "go")
            self.assertEqual(records[0]["source_run_id"], "9")

            markdown = md_path.read_text(encoding="utf-8")
            self.assertIn("## fesigan", markdown)
            self.assertIn("Monitor FEIGEN/FEDIGAN neighbors.", markdown)


if __name__ == "__main__":
    unittest.main()
