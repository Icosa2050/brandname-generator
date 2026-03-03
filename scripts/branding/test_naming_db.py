#!/usr/bin/env python3
"""Unit tests for naming_db connection configuration helpers."""

from __future__ import annotations

import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import naming_db as ndb


class NamingDbConnectionTest(unittest.TestCase):
    def test_open_connection_sets_pragmas(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'naming.db'
            with ndb.open_connection(db_path, busy_timeout_ms=3210, wal=True) as conn:
                ndb.ensure_schema(conn, busy_timeout_ms=3210, wal=True)
                journal_mode = str(conn.execute('PRAGMA journal_mode').fetchone()[0]).lower()
                busy_timeout = int(conn.execute('PRAGMA busy_timeout').fetchone()[0])
                foreign_keys = int(conn.execute('PRAGMA foreign_keys').fetchone()[0])
            self.assertEqual(journal_mode, 'wal')
            self.assertEqual(busy_timeout, 3210)
            self.assertEqual(foreign_keys, 1)

    def test_busy_timeout_waits_and_wal_allows_reads(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'contention.db'
            conn_a = ndb.open_connection(db_path, busy_timeout_ms=5000, wal=True)
            conn_b = ndb.open_connection(db_path, busy_timeout_ms=250, wal=True)
            try:
                ndb.ensure_schema(conn_a, busy_timeout_ms=5000, wal=True)
                conn_a.execute('CREATE TABLE IF NOT EXISTS lock_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)')
                conn_a.execute("INSERT OR REPLACE INTO lock_probe(id, value) VALUES(1, 'base')")
                conn_a.commit()

                conn_a.execute('BEGIN IMMEDIATE')
                conn_a.execute("UPDATE lock_probe SET value = 'locked' WHERE id = 1")

                read_value = conn_b.execute('SELECT value FROM lock_probe WHERE id = 1').fetchone()
                self.assertEqual(read_value[0], 'base')

                started = time.monotonic()
                with self.assertRaises(sqlite3.OperationalError):
                    conn_b.execute("UPDATE lock_probe SET value = 'other' WHERE id = 1")
                    conn_b.commit()
                elapsed = time.monotonic() - started
                self.assertGreaterEqual(elapsed, 0.18)
            finally:
                conn_a.rollback()
                conn_b.close()
                conn_a.close()

    def test_ensure_schema_adds_collision_rejection_metadata_columns(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'schema.db'
            with ndb.open_connection(db_path, busy_timeout_ms=5000, wal=True) as conn:
                ndb.ensure_schema(conn, busy_timeout_ms=5000, wal=True)
                cols = {
                    str(row[1])
                    for row in conn.execute('PRAGMA table_info(candidates)').fetchall()
                }
            self.assertIn('rejection_stage', cols)
            self.assertIn('rejection_reason_code', cols)
            self.assertIn('policy_version', cols)
            self.assertIn('query_fingerprint', cols)

    def test_parse_json_rows_invalid_payload_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'bad.json'
            path.write_text('{not json', encoding='utf-8')
            rows, scope, gate = ndb.parse_json_rows(path)
        self.assertEqual(rows, [])
        self.assertIsNone(scope)
        self.assertIsNone(gate)

    def test_parse_jsonl_rows_skips_invalid_lines(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'mixed.jsonl'
            path.write_text(
                '\n'.join(
                    [
                        '{"name": "validone", "scope": "global", "gate": "balanced"}',
                        '{not json}',
                        '{"top_candidates": [{"name": "validtwo"}]}',
                    ]
                )
                + '\n',
                encoding='utf-8',
            )
            rows, scope, gate = ndb.parse_jsonl_rows(path)
        self.assertEqual(len(rows), 2)
        self.assertEqual(scope, 'global')
        self.assertEqual(gate, 'balanced')

    def test_upsert_candidate_preserves_metadata_when_new_values_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'preserve.db'
            with ndb.open_connection(db_path, busy_timeout_ms=5000, wal=True) as conn:
                ndb.ensure_schema(conn, busy_timeout_ms=5000, wal=True)
                candidate_id = ndb.upsert_candidate(
                    conn,
                    name_display='Verodoma',
                    total_score=80.0,
                    risk_score=20.0,
                    recommendation='strong',
                    quality_score=81.0,
                    rejection_reason='validation_failed',
                    rejection_stage='validation_gate',
                    rejection_reason_code='domain_collision',
                    policy_version='collision_v1',
                    query_fingerprint='run:123',
                )
                ndb.upsert_candidate(
                    conn,
                    name_display='Verodoma',
                    total_score=79.0,
                    risk_score=21.0,
                    recommendation='consider',
                    quality_score=80.0,
                    rejection_reason='',
                    rejection_stage='',
                    rejection_reason_code='',
                    policy_version='',
                    query_fingerprint='',
                )
                row = conn.execute(
                    """
                    SELECT rejection_reason, rejection_stage, rejection_reason_code, policy_version, query_fingerprint
                    FROM candidates
                    WHERE id = ?
                    """,
                    (candidate_id,),
                ).fetchone()
        self.assertEqual(
            row,
            (
                '',
                'validation_gate',
                'domain_collision',
                'collision_v1',
                'run:123',
            ),
        )


if __name__ == '__main__':
    unittest.main()
