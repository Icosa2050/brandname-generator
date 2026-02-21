#!/usr/bin/env python3
"""Unit tests for naming_validate_async exclusion-memory helpers."""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

import naming_db as ndb
import naming_validate_async as nva


def _base_args() -> argparse.Namespace:
    return argparse.Namespace(
        adversarial_fail_threshold=82,
        adversarial_warn_threshold=68,
        cheap_trademark_screen=True,
        cheap_trademark_fail_threshold=90,
        cheap_trademark_warn_threshold=78,
        min_trust_proxy=50,
        warn_trust_proxy=62,
        max_spelling_risk=28,
        warn_spelling_risk=16,
        descriptive_fail_threshold=72,
        descriptive_warn_threshold=52,
        web_top=8,
        web_near_fail_threshold=2,
        social_unavailable_fail_threshold=3,
        strict_required_domains=False,
        scope='global',
        gate='balanced',
    )


class NamingValidateAsyncMemoryTest(unittest.TestCase):
    def test_parse_args_accepts_sqlite_busy_timeout(self) -> None:
        with mock.patch('sys.argv', ['naming_validate_async.py', '--sqlite-busy-timeout-ms', '1234']):
            args = nva.parse_args()
        self.assertEqual(args.sqlite_busy_timeout_ms, 1234)

    def test_policy_signature_is_stable(self) -> None:
        args = _base_args()
        flags = nva.ValidationFeatureFlags(pipeline_version='v3', v3_enabled=True, validation_tier='all')
        checks = ['adversarial', 'psych', 'descriptive', 'tm_cheap']
        one = nva.exclusion_memory_policy_signature(args=args, checks=checks, flags=flags)
        two = nva.exclusion_memory_policy_signature(args=args, checks=list(reversed(checks)), flags=flags)
        self.assertEqual(one, two)

    def test_exclusion_memory_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mem_path = Path(td) / 'memory.db'
            with closing(sqlite3.connect(mem_path)) as mem_conn:
                nva.ensure_exclusion_memory_schema(mem_conn)
                upserted = nva.upsert_exclusion_memory(
                    mem_conn,
                    exclusions={'verodoma': ['required_domain_com_not_available']},
                    scope='global',
                    gate='balanced',
                    policy_signature='sig-a',
                    ttl_days=30,
                )
                self.assertEqual(upserted, 1)
                hits = nva.load_memory_excluded_names(
                    mem_conn,
                    names=['Verodoma', 'othername'],
                    scope='global',
                    gate='balanced',
                    policy_signature='sig-a',
                )
                self.assertEqual(hits, {'verodoma'})
                miss = nva.load_memory_excluded_names(
                    mem_conn,
                    names=['Verodoma'],
                    scope='global',
                    gate='balanced',
                    policy_signature='sig-b',
                )
                self.assertEqual(miss, set())

    def test_mark_candidates_memory_excluded_sets_state(self) -> None:
        with closing(sqlite3.connect(':memory:')) as conn:
            ndb.ensure_schema(conn)
            candidate_id = ndb.upsert_candidate(
                conn,
                name_display='Verodoma',
                total_score=70.0,
                risk_score=35.0,
                recommendation='consider',
                quality_score=75.0,
                engine_id='explicit',
                parent_ids='',
                status='new',
                rejection_reason='',
            )
            row = nva.CandidateRow(
                candidate_id=candidate_id,
                name_display='Verodoma',
                state='new',
                current_score=70.0,
                current_recommendation='consider',
            )
            nva.mark_candidates_memory_excluded(
                conn,
                [row],
                actor='test',
                note='memory hit',
            )
            got = conn.execute(
                'SELECT state, status, rejection_reason FROM candidates WHERE id = ?',
                (candidate_id,),
            ).fetchone()
            self.assertEqual(got, ('memory_excluded', 'rejected_memory', 'memory_excluded'))

    def test_collect_hard_fail_reasons_by_name(self) -> None:
        with closing(sqlite3.connect(':memory:')) as conn:
            ndb.ensure_schema(conn)
            run_id = ndb.create_run(
                conn,
                source_path=':memory:',
                scope='global',
                gate_mode='balanced',
                variation_profile='test',
                status='completed',
                config={},
                summary={},
            )
            candidate_id = ndb.upsert_candidate(
                conn,
                name_display='Verodoma',
                total_score=65.0,
                risk_score=55.0,
                recommendation='weak',
                quality_score=70.0,
                engine_id='explicit',
                parent_ids='',
                status='new',
                rejection_reason='',
            )
            ndb.add_validation_result(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                check_type='domain',
                status='fail',
                score_delta=-20.0,
                hard_fail=True,
                reason='required_domain_com_not_available',
                evidence={},
            )
            ndb.add_validation_result(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                check_type='web',
                status='warn',
                score_delta=0.0,
                hard_fail=False,
                reason='',
                evidence={},
            )
            got = nva.collect_hard_fail_reasons_by_name(conn, run_id=run_id)
            self.assertIn('verodoma', got)
            self.assertEqual(got['verodoma'], ['required_domain_com_not_available'])


if __name__ == '__main__':
    unittest.main()
