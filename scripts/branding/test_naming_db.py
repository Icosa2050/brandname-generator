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


if __name__ == '__main__':
    unittest.main()
