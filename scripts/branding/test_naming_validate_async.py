#!/usr/bin/env python3
"""Unit tests for naming_validate_async exclusion-memory helpers."""

from __future__ import annotations

import asyncio
import argparse
import json
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
        web_exact_domain_fail_threshold=2,
        web_near_fail_threshold=2,
        social_unavailable_fail_threshold=3,
        strict_required_domains=False,
        scope='global',
        gate='balanced',
    )


def _job_args(**overrides: object) -> argparse.Namespace:
    args = _base_args()
    args.cheap_cache = True
    args.cheap_cache_ttl_s = 3600
    args.timeout_s = 1.0
    args.max_retries = 0
    args.retry_backoff_ms = 1
    args.track_job_lifecycle = True
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


class NamingValidateAsyncMemoryTest(unittest.TestCase):
    def test_parse_args_accepts_sqlite_busy_timeout(self) -> None:
        with mock.patch('sys.argv', ['naming_validate_async.py', '--sqlite-busy-timeout-ms', '1234']):
            args = nva.parse_args()
        self.assertEqual(args.sqlite_busy_timeout_ms, 1234)
        with mock.patch('sys.argv', ['naming_validate_async.py', '--min-concurrency', '3', '--max-concurrency', '9']):
            args = nva.parse_args()
        self.assertEqual(args.min_concurrency, 3)
        self.assertEqual(args.max_concurrency, 9)
        with mock.patch('sys.argv', ['naming_validate_async.py', '--no-track-job-lifecycle']):
            args = nva.parse_args()
        self.assertFalse(args.track_job_lifecycle)

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

    def test_demote_checked_candidates_with_validation_failures(self) -> None:
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
            hard_fail_id = ndb.upsert_candidate(
                conn,
                name_display='HardFailName',
                total_score=82.0,
                risk_score=20.0,
                recommendation='strong',
                quality_score=85.0,
                engine_id='explicit',
                parent_ids='',
                status='checked',
                rejection_reason='',
            )
            expensive_fail_id = ndb.upsert_candidate(
                conn,
                name_display='ExpensiveFailName',
                total_score=78.0,
                risk_score=21.0,
                recommendation='consider',
                quality_score=81.0,
                engine_id='explicit',
                parent_ids='',
                status='checked',
                rejection_reason='',
            )
            keep_id = ndb.upsert_candidate(
                conn,
                name_display='KeepName',
                total_score=79.0,
                risk_score=18.0,
                recommendation='strong',
                quality_score=82.0,
                engine_id='explicit',
                parent_ids='',
                status='checked',
                rejection_reason='',
            )
            conn.execute("UPDATE candidates SET state='checked' WHERE id IN (?, ?, ?)", (hard_fail_id, expensive_fail_id, keep_id))
            ndb.add_validation_result(
                conn,
                candidate_id=hard_fail_id,
                run_id=run_id,
                check_type='descriptive',
                status='fail',
                score_delta=-20.0,
                hard_fail=True,
                reason='descriptive_hard_fail',
                evidence={},
            )
            ndb.add_validation_result(
                conn,
                candidate_id=expensive_fail_id,
                run_id=run_id,
                check_type='domain',
                status='fail',
                score_delta=-10.0,
                hard_fail=False,
                reason='required_domain_com_not_available',
                evidence={},
            )
            ndb.add_validation_result(
                conn,
                candidate_id=keep_id,
                run_id=run_id,
                check_type='domain',
                status='warn',
                score_delta=0.0,
                hard_fail=False,
                reason='',
                evidence={},
            )
            demoted = nva.demote_checked_candidates_with_validation_failures(
                conn,
                actor='test',
            )
            self.assertEqual(demoted, 2)
            rows = conn.execute(
                """
                SELECT id, state, status, rejection_reason
                FROM candidates
                WHERE id IN (?, ?, ?)
                ORDER BY id
                """,
                (hard_fail_id, expensive_fail_id, keep_id),
            ).fetchall()
            got = {int(row[0]): (str(row[1]), str(row[2]), str(row[3])) for row in rows}
            self.assertEqual(got[hard_fail_id], ('rejected_validation', 'rejected', 'validation_failed'))
            self.assertEqual(got[expensive_fail_id], ('rejected_validation', 'rejected', 'validation_failed'))
            self.assertEqual(got[keep_id], ('checked', 'checked', ''))
            transition_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM state_transitions
                WHERE to_state='rejected_validation'
                """
            ).fetchone()[0]
            self.assertEqual(int(transition_count), 2)

    def test_load_candidates_prefers_newest_ids(self) -> None:
        with closing(sqlite3.connect(':memory:')) as conn:
            ndb.ensure_schema(conn)
            ids: list[int] = []
            for name in ['FirstName', 'SecondName', 'ThirdName']:
                ids.append(
                    ndb.upsert_candidate(
                        conn,
                        name_display=name,
                        total_score=70.0,
                        risk_score=20.0,
                        recommendation='consider',
                        quality_score=75.0,
                        engine_id='explicit',
                        parent_ids='',
                        status='new',
                        rejection_reason='',
                    )
                )
            rows = nva.load_candidates(conn, ['new'], 2)
            self.assertEqual([row.candidate_id for row in rows], [ids[2], ids[1]])

    def test_run_single_job_uses_two_lock_acquisitions_on_success(self) -> None:
        with closing(sqlite3.connect(':memory:')) as conn:
            ndb.ensure_schema(conn)
            run_id = ndb.create_run(
                conn,
                source_path=':memory:',
                scope='global',
                gate_mode='balanced',
                variation_profile='test',
                status='running',
                config={},
                summary={},
            )
            candidate_id = ndb.upsert_candidate(
                conn,
                name_display='Verodoma',
                total_score=65.0,
                risk_score=22.0,
                recommendation='strong',
                quality_score=71.0,
                engine_id='explicit',
                parent_ids='',
                status='new',
                rejection_reason='',
            )
            job_id = ndb.create_validation_job(
                conn,
                run_id=run_id,
                candidate_id=candidate_id,
                check_type='adversarial',
                status='pending',
            )
            conn.commit()
            spec = nva.ValidationJobSpec(
                job_id=job_id,
                run_id=run_id,
                candidate_id=candidate_id,
                candidate_name='Verodoma',
                candidate_prev_state='new',
                check_type='adversarial',
            )
            args = _job_args()
            lock = nva.InstrumentedLock()
            semaphore = asyncio.Semaphore(1)

            def _runner(_name: str, _args: argparse.Namespace) -> dict:
                return {'status': 'pass', 'score_delta': 0.0, 'hard_fail': False, 'reason': '', 'evidence': {}}

            asyncio.run(
                nva.run_single_job(
                    conn=conn,
                    args=args,
                    spec=spec,
                    runner=_runner,
                    db_lock=lock,
                    semaphore=semaphore,
                    on_complete=None,
                )
            )
            stats = lock.snapshot()
            self.assertEqual(stats['lock_acquisitions'], 2)
            status = conn.execute('SELECT status FROM validation_jobs WHERE id = ?', (job_id,)).fetchone()
            self.assertEqual(status[0], 'success')

    def test_instrumented_lock_reports_contention(self) -> None:
        lock = nva.InstrumentedLock()

        async def _holder() -> None:
            async with lock:
                await asyncio.sleep(0.03)

        async def _waiter() -> None:
            await asyncio.sleep(0.003)
            async with lock:
                return

        async def _run() -> None:
            await asyncio.gather(_holder(), _waiter())

        asyncio.run(_run())
        stats = lock.snapshot()
        self.assertEqual(stats['lock_acquisitions'], 2)
        self.assertGreaterEqual(stats['lock_total_wait_ms'], 0)
        self.assertGreaterEqual(stats['lock_max_wait_ms'], 0)
        self.assertGreaterEqual(stats['lock_contended_count'], 1)

    def test_adaptive_semaphore_adjust_respects_bounds(self) -> None:
        sem = nva.AdaptiveSemaphore(initial_concurrency=5, min_concurrency=2, max_concurrency=10)
        self.assertEqual(sem.current_limit, 5)
        asyncio.run(sem.adjust(50))
        self.assertEqual(sem.current_limit, 10)
        asyncio.run(sem.adjust(1))
        self.assertEqual(sem.current_limit, 2)

    def test_calculate_adaptive_concurrency_target_up_and_down(self) -> None:
        up_target, up_error_rate = nva.calculate_adaptive_concurrency_target(
            outcomes=['success'] * 50,
            current_concurrency=4,
            min_concurrency=2,
            max_concurrency=10,
        )
        self.assertGreaterEqual(up_target, 5)
        self.assertLess(up_error_rate, 0.05)

        down_target, down_error_rate = nva.calculate_adaptive_concurrency_target(
            outcomes=(['success'] * 35) + (['fail'] * 15),
            current_concurrency=8,
            min_concurrency=2,
            max_concurrency=10,
        )
        self.assertEqual(down_target, 4)
        self.assertGreater(down_error_rate, 0.20)

    def test_orchestrate_writes_lock_metrics_to_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                ndb.upsert_candidate(
                    conn,
                    name_display='Verodoma',
                    total_score=65.0,
                    risk_score=18.0,
                    recommendation='strong',
                    quality_score=74.0,
                    engine_id='explicit',
                    parent_ids='',
                    status='new',
                    rejection_reason='',
                )
                conn.commit()

            with mock.patch(
                'sys.argv',
                [
                    'naming_validate_async.py',
                    f'--db={db_path}',
                    '--checks=adversarial',
                    '--candidate-limit=1',
                    '--concurrency=1',
                    '--min-concurrency=1',
                    '--max-concurrency=4',
                    '--max-retries=0',
                    '--state-filter=new',
                    '--scope=global',
                    '--gate=balanced',
                    '--no-progress',
                ],
            ):
                args = nva.parse_args()
            code = asyncio.run(nva.orchestrate(args))
            self.assertEqual(code, 0)

            with closing(sqlite3.connect(db_path)) as conn:
                row = conn.execute(
                    "SELECT summary_json FROM naming_runs ORDER BY id DESC LIMIT 1",
                ).fetchone()
            self.assertIsNotNone(row)
            summary = json.loads(str(row[0] or '{}'))
            self.assertIn('lock_acquisitions', summary)
            self.assertIn('lock_total_wait_ms', summary)
            self.assertIn('lock_max_wait_ms', summary)
            self.assertIn('lock_contended_count', summary)
            self.assertIn('adaptive_concurrency', summary)
            self.assertEqual(summary['adaptive_concurrency'].get('min'), 1)
            self.assertEqual(summary['adaptive_concurrency'].get('max'), 4)

    def test_check_web_single_exact_domain_is_warning(self) -> None:
        args = _base_args()
        with mock.patch(
            'naming_validate_async.ng.web_collision_signal',
            return_value=(1, 0, 12, 'example.com', True, 'ddg'),
        ):
            got = nva.check_web('verasettle', args)
        self.assertEqual(got['status'], 'warn')
        self.assertFalse(got['hard_fail'])
        self.assertEqual(got['reason'], 'web_exact_warning')

    def test_check_web_two_exact_domains_is_hard_fail(self) -> None:
        args = _base_args()
        with mock.patch(
            'naming_validate_async.ng.web_collision_signal',
            return_value=(2, 0, 12, 'example.com;example.net', True, 'ddg'),
        ):
            got = nva.check_web('verasettle', args)
        self.assertEqual(got['status'], 'fail')
        self.assertTrue(got['hard_fail'])
        self.assertEqual(got['reason'], 'web_exact_collision')


if __name__ == '__main__':
    unittest.main()
