#!/usr/bin/env python3
"""Unit tests for naming_validate_async exclusion-memory helpers."""

from __future__ import annotations

import asyncio
import argparse
import csv
import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

import naming_db as ndb
import naming_validate_async as nva


def _base_args() -> argparse.Namespace:
    return argparse.Namespace(
        policy_version='collision_first_v1',
        class_profile='9,42',
        market_scope='eu,ch',
        adversarial_fail_threshold=82,
        adversarial_warn_threshold=68,
        cheap_trademark_screen=True,
        cheap_trademark_fail_threshold=90,
        cheap_trademark_warn_threshold=78,
        cheap_trademark_blocklist_file='',
        company_cheap_screen=True,
        company_cheap_top=8,
        company_cheap_exact_fail_threshold=1,
        company_cheap_near_fail_threshold=2,
        company_cheap_near_warn_threshold=1,
        min_trust_proxy=50,
        warn_trust_proxy=62,
        max_spelling_risk=28,
        warn_spelling_risk=16,
        descriptive_fail_threshold=72,
        descriptive_warn_threshold=52,
        web_top=8,
        web_exact_domain_fail_threshold=2,
        web_near_fail_threshold=2,
        web_google_like_enabled=True,
        web_google_top=10,
        web_google_exact_domain_fail_threshold=1,
        web_google_near_fail_threshold=3,
        web_google_near_warn_threshold=1,
        web_google_first_hit_hard_fail=True,
        web_google_cse_api_key='',
        web_google_cse_cx='',
        web_google_gl='de',
        web_google_hl='en',
        tm_registry_global_enabled=True,
        tm_registry_top=12,
        tm_registry_exact_fail_threshold=1,
        tm_registry_near_fail_threshold=10,
        tm_registry_near_warn_threshold=4,
        social_unavailable_fail_threshold=3,
        required_domain_tlds='',
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
        with mock.patch(
            'sys.argv',
            [
                'naming_validate_async.py',
                '--company-cheap-top',
                '11',
                '--company-cheap-exact-fail-threshold',
                '2',
                '--company-cheap-near-fail-threshold',
                '4',
                '--web-google-top',
                '9',
                '--tm-registry-top',
                '14',
                '--policy-version',
                'policy_x',
            ],
        ):
            args = nva.parse_args()
        self.assertEqual(args.company_cheap_top, 11)
        self.assertEqual(args.company_cheap_exact_fail_threshold, 2)
        self.assertEqual(args.company_cheap_near_fail_threshold, 4)
        self.assertEqual(args.web_google_top, 9)
        self.assertEqual(args.tm_registry_top, 14)
        self.assertEqual(args.policy_version, 'policy_x')
        with mock.patch.dict('os.environ', {'OPENROUTER_GOOGLE_CSE_API_KEY': 'key_from_env'}, clear=False):
            with mock.patch('sys.argv', ['naming_validate_async.py']):
                args = nva.parse_args()
        self.assertEqual(args.web_google_cse_api_key, 'key_from_env')
        with mock.patch(
            'sys.argv',
            [
                'naming_validate_async.py',
                '--tm-registry-unknown-hard-fail',
                '--tm-registry-require-tmview-ok',
                '--tmview-probe-enabled',
                '--tm-registry-tmview-probe-enabled',
                '--tmview-probe-timeout-ms',
                '25000',
                '--tm-registry-tmview-timeout-ms',
                '26000',
                '--tmview-probe-settle-ms',
                '3000',
                '--tm-registry-tmview-settle-ms',
                '3100',
            ],
        ):
            args = nva.parse_args()
        self.assertTrue(args.tm_registry_unknown_hard_fail)
        self.assertTrue(args.tm_registry_require_tmview_ok)
        self.assertTrue(args.tmview_probe_enabled)
        self.assertEqual(args.tmview_probe_timeout_ms, 26000)
        self.assertEqual(args.tmview_probe_settle_ms, 3100)
        with mock.patch('sys.argv', ['naming_validate_async.py', '--required-domain-tlds', 'com,at']):
            with self.assertRaises(SystemExit) as ctx:
                nva.parse_args()
        self.assertEqual(ctx.exception.code, 2)

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
                policy_version='policy_a',
                query_fingerprint='sig_abc',
            )
            got = conn.execute(
                """
                SELECT state, status, rejection_reason, rejection_stage, rejection_reason_code, policy_version, query_fingerprint
                FROM candidates
                WHERE id = ?
                """,
                (candidate_id,),
            ).fetchone()
            self.assertEqual(
                got,
                (
                    'memory_excluded',
                    'rejected_memory',
                    'memory_excluded',
                    'memory_prefilter',
                    'memory_excluded',
                    'policy_a',
                    'sig_abc',
                ),
            )

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
            timeout_id = ndb.upsert_candidate(
                conn,
                name_display='TimeoutName',
                total_score=77.0,
                risk_score=19.0,
                recommendation='consider',
                quality_score=80.0,
                engine_id='explicit',
                parent_ids='',
                status='checked',
                rejection_reason='',
            )
            conn.execute(
                "UPDATE candidates SET state='checked' WHERE id IN (?, ?, ?, ?)",
                (hard_fail_id, expensive_fail_id, keep_id, timeout_id),
            )
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
            ndb.add_validation_result(
                conn,
                candidate_id=timeout_id,
                run_id=run_id,
                check_type='app_store',
                status='error',
                score_delta=0.0,
                hard_fail=False,
                reason='validator_execution_timeout',
                evidence={},
            )
            demoted = nva.demote_checked_candidates_with_validation_failures(
                conn,
                actor='test',
                policy_version='policy_b',
                query_fingerprint='run:42',
            )
            self.assertEqual(demoted, 2)
            rows = conn.execute(
                """
                SELECT id, state, status, rejection_reason, rejection_stage, rejection_reason_code, policy_version, query_fingerprint
                FROM candidates
                WHERE id IN (?, ?, ?, ?)
                ORDER BY id
                """,
                (hard_fail_id, expensive_fail_id, keep_id, timeout_id),
            ).fetchall()
            got = {
                int(row[0]): (
                    str(row[1]),
                    str(row[2]),
                    str(row[3]),
                    str(row[4]),
                    str(row[5]),
                    str(row[6]),
                    str(row[7]),
                )
                for row in rows
            }
            self.assertEqual(
                got[hard_fail_id],
                (
                    'rejected_validation',
                    'rejected',
                    'validation_failed',
                    'validation_gate',
                    'validation_failed',
                    'policy_b',
                    'run:42',
                ),
            )
            self.assertEqual(
                got[expensive_fail_id],
                (
                    'rejected_validation',
                    'rejected',
                    'validation_failed',
                    'validation_gate',
                    'validation_failed',
                    'policy_b',
                    'run:42',
                ),
            )
            self.assertEqual(got[keep_id], ('checked', 'checked', '', '', '', '', ''))
            self.assertEqual(got[timeout_id], ('checked', 'checked', '', '', '', '', ''))
            transition_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM state_transitions
                WHERE to_state='rejected_validation'
                """
            ).fetchone()[0]
            self.assertEqual(int(transition_count), 2)

    def test_export_validation_publish_artifacts_splits_survivors_review_and_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            out_dir = Path(td) / 'campaign'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='running',
                    config={
                        'checks': ['company_cheap', 'domain', 'web_google_like', 'tm_registry_global', 'tmview_probe'],
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )

                survivor_id = ndb.upsert_candidate(
                    conn,
                    name_display='Survivora',
                    total_score=84.0,
                    risk_score=19.0,
                    recommendation='strong',
                    quality_score=81.0,
                    status='checked',
                )
                review_id = ndb.upsert_candidate(
                    conn,
                    name_display='Reviewora',
                    total_score=82.0,
                    risk_score=20.0,
                    recommendation='consider',
                    quality_score=79.0,
                    status='checked',
                )
                rejected_id = ndb.upsert_candidate(
                    conn,
                    name_display='Rejectora',
                    total_score=80.0,
                    risk_score=21.0,
                    recommendation='consider',
                    quality_score=77.0,
                    status='checked',
                )
                for idx, candidate_id in enumerate([survivor_id, review_id, rejected_id], start=1):
                    ndb.add_score_snapshot(
                        conn,
                        candidate_id=candidate_id,
                        run_id=run_id,
                        quality_score=80.0 - idx,
                        risk_score=20.0 + idx,
                        external_penalty=0.0,
                        total_score=85.0 - idx,
                        recommendation='strong' if idx == 1 else 'consider',
                        hard_fail=False,
                        reason='',
                    )
                    ndb.add_shortlist_decision(
                        conn,
                        candidate_id=candidate_id,
                        run_id=run_id,
                        selected=True,
                        shortlist_rank=idx,
                        bucket_key='test',
                        reason='selected',
                        score=85.0 - idx,
                    )
                ndb.add_validation_result(
                    conn,
                    candidate_id=survivor_id,
                    run_id=run_id,
                    check_type='company_cheap',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=survivor_id,
                    run_id=run_id,
                    check_type='domain',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=survivor_id,
                    run_id=run_id,
                    check_type='web_google_like',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=survivor_id,
                    run_id=run_id,
                    check_type='tm_registry_global',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=survivor_id,
                    run_id=run_id,
                    check_type='psych',
                    status='warn',
                    score_delta=-1.0,
                    hard_fail=False,
                    reason='psych_spelling_warning',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=review_id,
                    run_id=run_id,
                    check_type='company_cheap',
                    status='warn',
                    score_delta=-8.0,
                    hard_fail=False,
                    reason='company_house_near_active',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=review_id,
                    run_id=run_id,
                    check_type='domain',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=review_id,
                    run_id=run_id,
                    check_type='web_google_like',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=review_id,
                    run_id=run_id,
                    check_type='tm_registry_global',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=rejected_id,
                    run_id=run_id,
                    check_type='company_cheap',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=rejected_id,
                    run_id=run_id,
                    check_type='domain',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=rejected_id,
                    run_id=run_id,
                    check_type='web_google_like',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=rejected_id,
                    run_id=run_id,
                    check_type='tm_registry_global',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=rejected_id,
                    run_id=run_id,
                    check_type='tmview_probe',
                    status='fail',
                    score_delta=-9.0,
                    hard_fail=False,
                    reason='tmview_probe_exact_collision',
                    evidence={},
                )
                conn.commit()

                summary = nva.export_validation_publish_artifacts(conn, run_id=run_id, out_dir=out_dir)

            self.assertEqual(summary['survivor_count'], 1)
            self.assertEqual(summary['review_count'], 1)
            self.assertEqual(summary['rejected_count'], 1)

            survivors_csv = out_dir / 'postrank' / 'validated_survivors.csv'
            review_csv = out_dir / 'postrank' / 'validated_review_queue.csv'
            rejected_csv = out_dir / 'postrank' / 'validated_rejected.csv'
            self.assertTrue(survivors_csv.exists())
            self.assertTrue(review_csv.exists())
            self.assertTrue(rejected_csv.exists())

            with survivors_csv.open('r', encoding='utf-8', newline='') as handle:
                survivors = list(csv.DictReader(handle))
            with review_csv.open('r', encoding='utf-8', newline='') as handle:
                review_rows = list(csv.DictReader(handle))
            with rejected_csv.open('r', encoding='utf-8', newline='') as handle:
                rejected_rows = list(csv.DictReader(handle))

            self.assertEqual([row['name'] for row in survivors], ['Survivora'])
            self.assertEqual([row['name'] for row in review_rows], ['Reviewora'])
            self.assertEqual([row['name'] for row in rejected_rows], ['Rejectora'])
            self.assertEqual(review_rows[0]['publish_bucket'], 'review')
            self.assertIn('company_cheap:warn:company_house_near_active', review_rows[0]['review_reasons'])
            self.assertEqual(rejected_rows[0]['publish_bucket'], 'rejected')
            self.assertIn('tmview_probe:fail:tmview_probe_exact_collision', rejected_rows[0]['blocker_reasons'])

    def test_export_validation_publish_artifacts_downgrades_timeout_errors_to_review(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            out_dir = Path(td) / 'campaign'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='global',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='running',
                    config={
                        'checks': ['company_cheap', 'domain', 'web_google_like', 'tm_registry_global', 'app_store'],
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )

                candidate_id = ndb.upsert_candidate(
                    conn,
                    name_display='Timeoutora',
                    total_score=84.0,
                    risk_score=19.0,
                    recommendation='strong',
                    quality_score=81.0,
                    status='checked',
                )
                ndb.add_score_snapshot(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    quality_score=81.0,
                    risk_score=19.0,
                    external_penalty=0.0,
                    total_score=84.0,
                    recommendation='strong',
                    hard_fail=False,
                    reason='',
                )
                ndb.add_shortlist_decision(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    selected=True,
                    shortlist_rank=1,
                    bucket_key='keep',
                    reason='selected',
                    score=84.0,
                )
                for check_type in ['company_cheap', 'domain', 'web_google_like', 'tm_registry_global']:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=candidate_id,
                        run_id=run_id,
                        check_type=check_type,
                        status='pass',
                        score_delta=0.0,
                        hard_fail=False,
                        reason='',
                        evidence={},
                    )
                ndb.add_validation_result(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    check_type='app_store',
                    status='error',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='validator_execution_timeout',
                    evidence={},
                )
                conn.commit()

                summary = nva.export_validation_publish_artifacts(conn, run_id=run_id, out_dir=out_dir)

            self.assertEqual(summary['survivor_count'], 0)
            self.assertEqual(summary['review_count'], 1)
            self.assertEqual(summary['rejected_count'], 0)
            review_csv = out_dir / 'postrank' / 'validated_review_queue.csv'
            with review_csv.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual([row['name'] for row in rows], ['Timeoutora'])
            self.assertIn('app_store:error:validator_execution_timeout', rows[0]['review_reasons'])

    def test_export_validation_publish_artifacts_uses_historical_results_and_reviews_missing_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            out_dir = Path(td) / 'campaign'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                historical_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': ['company_cheap', 'web_google_like', 'tm_registry_global'],
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )
                source_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='expanded',
                    status='completed',
                    config={},
                    summary={},
                )
                current_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': ['company_cheap', 'web_google_like', 'tm_registry_global'],
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )

                survivor_id = ndb.upsert_candidate(
                    conn,
                    name_display='HistoryPass',
                    total_score=84.0,
                    risk_score=19.0,
                    recommendation='strong',
                    quality_score=81.0,
                    status='checked',
                )
                review_id = ndb.upsert_candidate(
                    conn,
                    name_display='HistoryReview',
                    total_score=82.0,
                    risk_score=20.0,
                    recommendation='consider',
                    quality_score=79.0,
                    status='checked',
                )
                rejected_id = ndb.upsert_candidate(
                    conn,
                    name_display='HistoryReject',
                    total_score=80.0,
                    risk_score=21.0,
                    recommendation='consider',
                    quality_score=77.0,
                    status='checked',
                )
                missing_id = ndb.upsert_candidate(
                    conn,
                    name_display='NoCoverage',
                    total_score=78.0,
                    risk_score=23.0,
                    recommendation='consider',
                    quality_score=75.0,
                    status='checked',
                )
                for idx, candidate_id in enumerate([survivor_id, review_id, rejected_id, missing_id], start=1):
                    ndb.add_score_snapshot(
                        conn,
                        candidate_id=candidate_id,
                        run_id=source_run_id,
                        quality_score=80.0 - idx,
                        risk_score=20.0 + idx,
                        external_penalty=0.0,
                        total_score=85.0 - idx,
                        recommendation='strong' if idx == 1 else 'consider',
                        hard_fail=False,
                        reason='',
                    )
                    ndb.add_shortlist_decision(
                        conn,
                        candidate_id=candidate_id,
                        run_id=source_run_id,
                        selected=True,
                        shortlist_rank=idx,
                        bucket_key='test',
                        reason='selected',
                        score=85.0 - idx,
                    )

                for check_type in ['company_cheap', 'web_google_like', 'tm_registry_global']:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=survivor_id,
                        run_id=historical_run_id,
                        check_type=check_type,
                        status='pass',
                        score_delta=0.0,
                        hard_fail=False,
                        reason='',
                        evidence={},
                    )
                ndb.add_validation_result(
                    conn,
                    candidate_id=review_id,
                    run_id=historical_run_id,
                    check_type='company_cheap',
                    status='warn',
                    score_delta=-8.0,
                    hard_fail=False,
                    reason='company_house_near_active',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=review_id,
                    run_id=historical_run_id,
                    check_type='web_google_like',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=review_id,
                    run_id=historical_run_id,
                    check_type='tm_registry_global',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=rejected_id,
                    run_id=historical_run_id,
                    check_type='company_cheap',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=rejected_id,
                    run_id=historical_run_id,
                    check_type='web_google_like',
                    status='fail',
                    score_delta=-12.0,
                    hard_fail=False,
                    reason='web_google_exact_collision',
                    evidence={},
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=rejected_id,
                    run_id=historical_run_id,
                    check_type='tm_registry_global',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                conn.commit()

                summary = nva.export_validation_publish_artifacts(conn, run_id=current_run_id, out_dir=out_dir)

            self.assertEqual(summary['survivor_count'], 1)
            self.assertEqual(summary['review_count'], 1)
            self.assertEqual(summary['rejected_count'], 1)
            self.assertEqual(summary['pending_coverage_count'], 1)
            self.assertEqual(summary['missing_validation_count'], 1)
            self.assertEqual(summary['missing_required_check_count'], 1)

            review_csv = out_dir / 'postrank' / 'validated_review_queue.csv'
            rejected_csv = out_dir / 'postrank' / 'validated_rejected.csv'
            survivors_csv = out_dir / 'postrank' / 'validated_survivors.csv'
            pending_csv = out_dir / 'postrank' / 'validated_pending_coverage.csv'
            with survivors_csv.open('r', encoding='utf-8', newline='') as handle:
                survivors = list(csv.DictReader(handle))
            with review_csv.open('r', encoding='utf-8', newline='') as handle:
                review_rows = list(csv.DictReader(handle))
            with rejected_csv.open('r', encoding='utf-8', newline='') as handle:
                rejected_rows = list(csv.DictReader(handle))
            with pending_csv.open('r', encoding='utf-8', newline='') as handle:
                pending_rows = list(csv.DictReader(handle))

            self.assertEqual([row['name'] for row in survivors], ['HistoryPass'])
            self.assertEqual([row['name'] for row in review_rows], ['HistoryReview'])
            self.assertEqual([row['name'] for row in rejected_rows], ['HistoryReject'])
            self.assertEqual([row['name'] for row in pending_rows], ['NoCoverage'])
            self.assertEqual(pending_rows[0]['publish_bucket'], 'pending_coverage')
            self.assertIn('validation:none', pending_rows[0]['review_reasons'])
            self.assertIn('company_cheap:missing', pending_rows[0]['review_reasons'])
            self.assertIn('web_google_like:missing', pending_rows[0]['review_reasons'])
            self.assertIn('tm_registry_global:missing', pending_rows[0]['review_reasons'])
            self.assertIn('web_google_like:fail:web_google_exact_collision', rejected_rows[0]['blocker_reasons'])

    def test_export_validation_publish_artifacts_dedupes_shortlist_rows_and_score_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            out_dir = Path(td) / 'campaign'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': ['company_cheap', 'domain', 'web_google_like', 'tm_registry_global'],
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )
                candidate_id = ndb.upsert_candidate(
                    conn,
                    name_display='Dupora',
                    total_score=84.0,
                    risk_score=19.0,
                    recommendation='strong',
                    quality_score=81.0,
                    status='checked',
                )
                ndb.add_score_snapshot(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    quality_score=75.0,
                    risk_score=22.0,
                    external_penalty=0.0,
                    total_score=78.0,
                    recommendation='consider',
                    hard_fail=False,
                    reason='',
                )
                ndb.add_score_snapshot(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    quality_score=82.0,
                    risk_score=18.0,
                    external_penalty=0.0,
                    total_score=88.0,
                    recommendation='strong',
                    hard_fail=False,
                    reason='',
                )
                ndb.add_shortlist_decision(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    selected=True,
                    shortlist_rank=4,
                    bucket_key='test',
                    reason='selected',
                    score=78.0,
                )
                ndb.add_shortlist_decision(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    selected=True,
                    shortlist_rank=1,
                    bucket_key='test',
                    reason='selected',
                    score=88.0,
                )
                for check_type in ['company_cheap', 'domain', 'web_google_like', 'tm_registry_global']:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=candidate_id,
                        run_id=run_id,
                        check_type=check_type,
                        status='pass',
                        score_delta=0.0,
                        hard_fail=False,
                        reason='',
                        evidence={},
                    )
                conn.commit()

                summary = nva.export_validation_publish_artifacts(conn, run_id=run_id, out_dir=out_dir)

            self.assertEqual(summary['shortlist_selected_count'], 1)
            self.assertEqual(summary['survivor_count'], 1)
            survivors_csv = out_dir / 'postrank' / 'validated_survivors.csv'
            with survivors_csv.open('r', encoding='utf-8', newline='') as handle:
                survivors = list(csv.DictReader(handle))
            self.assertEqual(len(survivors), 1)
            self.assertEqual(survivors[0]['name'], 'Dupora')
            self.assertEqual(survivors[0]['shortlist_rank'], '1')
            self.assertEqual(survivors[0]['total_score'], '88.00')

    def test_export_validation_publish_artifacts_derives_required_checks_from_run_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            out_dir = Path(td) / 'campaign'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                source_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='expanded',
                    status='completed',
                    config={},
                    summary={},
                )
                validation_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': ['company_cheap'],
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )

                candidate_id = ndb.upsert_candidate(
                    conn,
                    name_display='ConfigAware',
                    total_score=84.0,
                    risk_score=19.0,
                    recommendation='strong',
                    quality_score=81.0,
                    status='checked',
                )
                ndb.add_score_snapshot(
                    conn,
                    candidate_id=candidate_id,
                    run_id=source_run_id,
                    quality_score=81.0,
                    risk_score=19.0,
                    external_penalty=0.0,
                    total_score=84.0,
                    recommendation='strong',
                    hard_fail=False,
                    reason='',
                )
                ndb.add_shortlist_decision(
                    conn,
                    candidate_id=candidate_id,
                    run_id=source_run_id,
                    selected=True,
                    shortlist_rank=1,
                    bucket_key='test',
                    reason='selected',
                    score=84.0,
                )
                ndb.add_validation_result(
                    conn,
                    candidate_id=candidate_id,
                    run_id=validation_run_id,
                    check_type='company_cheap',
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )
                conn.commit()

                summary = nva.export_validation_publish_artifacts(conn, run_id=validation_run_id, out_dir=out_dir)

            self.assertEqual(summary['required_checks'], ['company_cheap'])
            self.assertEqual(summary['survivor_count'], 1)
            self.assertEqual(summary['review_count'], 0)

    def test_resolve_publish_policy_accepts_string_form_checks_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': 'company_cheap,domain',
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )
                policy = nva.resolve_publish_policy(conn, validation_run_id=run_id)
        self.assertEqual(policy['required_checks'], ['company_cheap', 'domain'])

    def test_export_validation_publish_artifacts_reviews_untrusted_historical_results(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            out_dir = Path(td) / 'campaign'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                historical_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': ['company_cheap', 'web_google_like', 'tm_registry_global'],
                        'policy_version': 'collision_first_v0',
                        'memory_policy_signature': 'publish_old',
                    },
                    summary={},
                )
                source_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='expanded',
                    status='completed',
                    config={},
                    summary={},
                )
                current_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='eu',
                    gate_mode='strict',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': ['company_cheap', 'web_google_like', 'tm_registry_global'],
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )

                candidate_id = ndb.upsert_candidate(
                    conn,
                    name_display='TrustGuard',
                    total_score=84.0,
                    risk_score=19.0,
                    recommendation='strong',
                    quality_score=81.0,
                    status='checked',
                )
                ndb.add_score_snapshot(
                    conn,
                    candidate_id=candidate_id,
                    run_id=source_run_id,
                    quality_score=81.0,
                    risk_score=19.0,
                    external_penalty=0.0,
                    total_score=84.0,
                    recommendation='strong',
                    hard_fail=False,
                    reason='',
                )
                ndb.add_shortlist_decision(
                    conn,
                    candidate_id=candidate_id,
                    run_id=source_run_id,
                    selected=True,
                    shortlist_rank=1,
                    bucket_key='test',
                    reason='selected',
                    score=84.0,
                )
                for check_type in ['company_cheap', 'web_google_like', 'tm_registry_global']:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=candidate_id,
                        run_id=historical_run_id,
                        check_type=check_type,
                        status='pass',
                        score_delta=0.0,
                        hard_fail=False,
                        reason='',
                        evidence={},
                    )
                conn.commit()

                summary = nva.export_validation_publish_artifacts(conn, run_id=current_run_id, out_dir=out_dir)

            self.assertEqual(summary['survivor_count'], 0)
            self.assertEqual(summary['review_count'], 1)
            review_csv = out_dir / 'postrank' / 'validated_review_queue.csv'
            with review_csv.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            self.assertIn('company_cheap:warn:untrusted_history_policy_mismatch', rows[0]['review_reasons'])
            self.assertIn('company_cheap:missing', rows[0]['review_reasons'])

    def test_export_validation_publish_artifacts_ignores_untrusted_optional_history(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'validator.db'
            out_dir = Path(td) / 'campaign'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                historical_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='global',
                    gate_mode='balanced',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': ['web', 'package'],
                        'policy_version': 'collision_first_v0',
                        'memory_policy_signature': 'publish_old',
                    },
                    summary={},
                )
                source_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='global',
                    gate_mode='balanced',
                    variation_profile='expanded',
                    status='completed',
                    config={},
                    summary={},
                )
                current_run_id = ndb.create_run(
                    conn,
                    source_path=str(db_path),
                    scope='global',
                    gate_mode='balanced',
                    variation_profile='validator_async',
                    status='completed',
                    config={
                        'checks': ['company_cheap', 'web_google_like', 'tm_registry_global'],
                        'policy_version': 'collision_first_v1',
                        'memory_policy_signature': 'publish_v1',
                    },
                    summary={},
                )

                candidate_id = ndb.upsert_candidate(
                    conn,
                    name_display='Freshpass',
                    total_score=86.0,
                    risk_score=18.0,
                    recommendation='strong',
                    quality_score=84.0,
                    status='checked',
                )
                ndb.add_score_snapshot(
                    conn,
                    candidate_id=candidate_id,
                    run_id=source_run_id,
                    quality_score=84.0,
                    risk_score=18.0,
                    external_penalty=0.0,
                    total_score=86.0,
                    recommendation='strong',
                    hard_fail=False,
                    reason='',
                )
                ndb.add_shortlist_decision(
                    conn,
                    candidate_id=candidate_id,
                    run_id=source_run_id,
                    selected=True,
                    shortlist_rank=1,
                    bucket_key='test',
                    reason='selected',
                    score=86.0,
                )
                for check_type in ['company_cheap', 'web_google_like', 'tm_registry_global']:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=candidate_id,
                        run_id=current_run_id,
                        check_type=check_type,
                        status='pass',
                        score_delta=0.0,
                        hard_fail=False,
                        reason='',
                        evidence={},
                    )
                for check_type in ['web', 'package']:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=candidate_id,
                        run_id=historical_run_id,
                        check_type=check_type,
                        status='warn',
                        score_delta=-2.0,
                        hard_fail=False,
                        reason=f'{check_type}_unknown',
                        evidence={},
                    )
                conn.commit()

                summary = nva.export_validation_publish_artifacts(conn, run_id=current_run_id, out_dir=out_dir)

            self.assertEqual(summary['survivor_count'], 1)
            self.assertEqual(summary['review_count'], 0)
            survivors_csv = out_dir / 'postrank' / 'validated_survivors.csv'
            with survivors_csv.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual([row['name'] for row in rows], ['Freshpass'])

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

    def test_load_shortlist_candidates_uses_shortlist_rank_order(self) -> None:
        with closing(sqlite3.connect(':memory:')) as conn:
            ndb.ensure_schema(conn)
            run_id = ndb.create_run(
                conn,
                source_path=':memory:',
                scope='eu',
                gate_mode='strict',
                variation_profile='expanded',
                status='completed',
                config={},
                summary={},
            )
            alpha_id = ndb.upsert_candidate(
                conn,
                name_display='Alpha',
                total_score=80.0,
                risk_score=20.0,
                recommendation='consider',
                quality_score=78.0,
                status='checked',
            )
            beta_id = ndb.upsert_candidate(
                conn,
                name_display='Beta',
                total_score=82.0,
                risk_score=18.0,
                recommendation='strong',
                quality_score=79.0,
                status='checked',
            )
            ndb.add_score_snapshot(
                conn,
                candidate_id=alpha_id,
                run_id=run_id,
                quality_score=78.0,
                risk_score=20.0,
                external_penalty=0.0,
                total_score=80.0,
                recommendation='consider',
                hard_fail=False,
                reason='',
            )
            ndb.add_score_snapshot(
                conn,
                candidate_id=beta_id,
                run_id=run_id,
                quality_score=79.0,
                risk_score=18.0,
                external_penalty=0.0,
                total_score=82.0,
                recommendation='strong',
                hard_fail=False,
                reason='',
            )
            ndb.add_score_snapshot(
                conn,
                candidate_id=beta_id,
                run_id=run_id,
                quality_score=81.0,
                risk_score=17.0,
                external_penalty=0.0,
                total_score=83.0,
                recommendation='strong',
                hard_fail=False,
                reason='latest',
            )
            ndb.add_shortlist_decision(
                conn,
                candidate_id=alpha_id,
                run_id=run_id,
                selected=True,
                shortlist_rank=2,
                bucket_key='test',
                reason='selected',
                score=80.0,
            )
            ndb.add_shortlist_decision(
                conn,
                candidate_id=beta_id,
                run_id=run_id,
                selected=True,
                shortlist_rank=1,
                bucket_key='test',
                reason='selected',
                score=82.0,
            )
            ndb.add_shortlist_decision(
                conn,
                candidate_id=beta_id,
                run_id=run_id,
                selected=True,
                shortlist_rank=5,
                bucket_key='test',
                reason='duplicate_selected',
                score=81.0,
            )
            rows = nva.load_shortlist_candidates(conn, run_id, 10)
            self.assertEqual([row.name_display for row in rows], ['Beta', 'Alpha'])
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].current_score, 83.0)

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

    def test_run_single_job_timeout_records_error_without_retry(self) -> None:
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
            args = _job_args(timeout_s=0.01, max_retries=5, retry_backoff_ms=1)
            lock = nva.InstrumentedLock()
            semaphore = asyncio.Semaphore(1)
            runner_calls = {'count': 0}

            def _runner(_name: str, _args: argparse.Namespace) -> dict:
                runner_calls['count'] += 1
                time.sleep(0.05)
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

            self.assertEqual(runner_calls['count'], 1)
            result = conn.execute(
                """
                SELECT status, reason
                FROM validation_results
                WHERE candidate_id = ? AND run_id = ? AND check_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (candidate_id, run_id, 'adversarial'),
            ).fetchone()
            self.assertEqual(result, ('error', 'validator_execution_timeout'))

            job = conn.execute(
                "SELECT status, attempt_count, last_error FROM validation_jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            self.assertEqual(job[0], 'fail')
            self.assertEqual(int(job[1]), 1)
            self.assertIn('TimeoutError', str(job[2]))

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

    def test_check_domain_required_domain_tlds_override_enforces_publish_domains(self) -> None:
        args = _base_args()
        args.scope = 'global'
        args.required_domain_tlds = 'com,de,ch'
        availability = {'com': 'yes', 'de': 'no', 'ch': 'yes'}
        with mock.patch(
            'naming_validate_async.ng.rdap_available',
            side_effect=lambda name, tld: availability[tld],
        ):
            got = nva.check_domain('medrona', args)
        self.assertEqual(got['status'], 'fail')
        self.assertTrue(got['hard_fail'])
        self.assertEqual(got['reason'], 'domain_unavailable_de')

    def test_check_domain_preserves_digits_in_candidate_name(self) -> None:
        args = _base_args()
        seen_names: list[str] = []

        def fake_rdap_available(name: str, tld: str) -> str:
            seen_names.append(name)
            return 'yes'

        with mock.patch('naming_validate_async.ng.rdap_available', side_effect=fake_rdap_available):
            got = nva.check_domain('set4you', args)

        self.assertEqual(got['status'], 'pass')
        self.assertTrue(seen_names)
        self.assertTrue(all(name == 'set4you' for name in seen_names))

    def test_resolve_required_domain_tlds_uses_scope_defaults_when_unset(self) -> None:
        args = _base_args()
        args.scope = 'global'
        args.required_domain_tlds = ''
        self.assertEqual(nva.resolve_required_domain_tlds(args), ['com'])

    def test_resolve_required_domain_tlds_rejects_unknown_tokens(self) -> None:
        args = _base_args()
        args.required_domain_tlds = 'com,at'
        with self.assertRaisesRegex(ValueError, 'Unsupported required domain TLDs: at'):
            nva.resolve_required_domain_tlds(args)

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

    def test_check_company_cheap_exact_hit_is_hard_fail(self) -> None:
        args = _base_args()
        with mock.patch(
            'naming_validate_async.company_collision_signal',
            return_value=(1, 0, 12, 'example.com', True, 'bing'),
        ):
            got = nva.check_company_cheap('verasettle', args)
        self.assertEqual(got['status'], 'fail')
        self.assertTrue(got['hard_fail'])
        self.assertEqual(got['reason'], 'company_exact_hit')

    def test_check_company_cheap_near_hit_warns_before_fail_threshold(self) -> None:
        args = _base_args()
        args.company_cheap_near_fail_threshold = 3
        args.company_cheap_near_warn_threshold = 1
        with mock.patch(
            'naming_validate_async.company_collision_signal',
            return_value=(0, 1, 8, 'example.org', True, 'bing'),
        ):
            got = nva.check_company_cheap('verasettle', args)
        self.assertEqual(got['status'], 'warn')
        self.assertFalse(got['hard_fail'])
        self.assertEqual(got['reason'], 'company_near_warning')

    def test_check_company_cheap_company_house_exact_active_is_hard_fail(self) -> None:
        args = _base_args()
        with (
            mock.patch(
                'naming_validate_async.company_house_company_signal',
                return_value={
                    'ok': True,
                    'source': 'companies_house',
                    'exact_active_hits': 1,
                    'near_active_hits': 0,
                    'result_count': 1,
                    'sample_titles': ['Clarivon Limited'],
                },
            ),
            mock.patch(
                'naming_validate_async.company_collision_signal',
                return_value=(0, 0, -1, '', False, ''),
            ),
        ):
            got = nva.check_company_cheap('clarivon', args)
        self.assertEqual(got['status'], 'fail')
        self.assertTrue(got['hard_fail'])
        self.assertEqual(got['reason'], 'company_house_exact_active')

    def test_check_company_cheap_company_house_clean_negative_can_pass(self) -> None:
        args = _base_args()
        with (
            mock.patch(
                'naming_validate_async.company_house_company_signal',
                return_value={
                    'ok': True,
                    'source': 'companies_house',
                    'exact_active_hits': 0,
                    'near_active_hits': 0,
                    'result_count': 0,
                    'sample_titles': [],
                },
            ),
            mock.patch(
                'naming_validate_async.company_collision_signal',
                return_value=(0, 0, -1, '', False, ''),
            ),
        ):
            got = nva.check_company_cheap('freshpass', args)
        self.assertEqual(got['status'], 'pass')
        self.assertFalse(got['hard_fail'])

    def test_check_web_google_like_first_hit_exact_is_hard_fail(self) -> None:
        args = _base_args()
        with mock.patch(
            'naming_validate_async.web_google_like_signal',
            return_value={
                'exact_hits': 1,
                'near_hits': 0,
                'result_count': 20,
                'sample_domains': 'metronis-technologies.de',
                'ok': True,
                'source': 'google_cse',
                'provider': 'google_cse',
                'first_hit_exact': True,
                'first_hit_url': 'https://www.metronis-technologies.de/',
                'first_hit_title': 'Metronis Technologies',
            },
        ):
            got = nva.check_web_google_like('metronis', args)
        self.assertEqual(got['status'], 'fail')
        self.assertTrue(got['hard_fail'])
        self.assertEqual(got['reason'], 'web_google_first_hit_exact')

    def test_check_web_google_like_near_warn(self) -> None:
        args = _base_args()
        with mock.patch(
            'naming_validate_async.web_google_like_signal',
            return_value={
                'exact_hits': 0,
                'near_hits': 1,
                'result_count': 20,
                'sample_domains': 'luci.ai;luci-vant.example',
                'ok': True,
                'source': 'google_cse',
                'provider': 'google_cse',
                'first_hit_exact': False,
                'first_hit_url': 'https://luci.ai',
                'first_hit_title': 'Luci AI',
            },
        ):
            got = nva.check_web_google_like('lucivant', args)
        self.assertEqual(got['status'], 'warn')
        self.assertFalse(got['hard_fail'])
        self.assertEqual(got['reason'], 'web_google_near_warning')

    def test_web_google_like_signal_uses_first_non_social_hit(self) -> None:
        args = _base_args()
        args.timeout_s = 8.0
        with mock.patch(
            'naming_validate_async._google_cse_search',
            side_effect=[
                ([], True, 'google_cse'),
                (
                    [
                        ('https://x.com/metronis', 'metronis on x'),
                        ('https://www.metronis-technologies.de/', 'Metronis Technologies'),
                        ('https://example.com/unrelated', 'unrelated'),
                    ],
                    True,
                    'google_cse',
                ),
            ],
        ):
            signal = nva.web_google_like_signal('metronis', args)
        self.assertTrue(bool(signal.get('ok')))
        self.assertEqual(signal.get('first_hit_url'), 'https://www.metronis-technologies.de/')
        self.assertTrue(bool(signal.get('first_hit_exact')))

    def test_tm_registry_global_signal_does_not_require_name_generator_probe_symbol(self) -> None:
        args = _base_args()
        with mock.patch(
            'naming_validate_async.ng.fetch_search_matches',
            return_value=([('https://registry.example/metronis', 'METRONIS')], True, 'ddg'),
        ):
            signal = nva.tm_registry_global_signal('metronis', args)
        self.assertTrue(bool(signal.get('ok')))
        self.assertEqual(int(signal.get('source_count', 0)), 5)
        self.assertGreaterEqual(int(signal.get('exact_hits_total', 0)), 1)

    def test_check_tm_registry_global_exact_is_hard_fail(self) -> None:
        args = _base_args()
        with mock.patch(
            'naming_validate_async.tm_registry_global_signal',
            return_value={
                'ok': True,
                'source_count': 5,
                'ok_source_count': 5,
                'exact_hits_total': 2,
                'near_hits_total': 5,
                'result_count_total': 44,
                'registry': {'tmview': {'exact_hits': 1}},
            },
        ):
            got = nva.check_tm_registry_global('metronis', args)
        self.assertEqual(got['status'], 'fail')
        self.assertTrue(got['hard_fail'])
        self.assertEqual(got['reason'], 'tm_registry_exact_collision')

    def test_check_tm_registry_global_near_warn(self) -> None:
        args = _base_args()
        with mock.patch(
            'naming_validate_async.tm_registry_global_signal',
            return_value={
                'ok': True,
                'source_count': 5,
                'ok_source_count': 4,
                'exact_hits_total': 0,
                'near_hits_total': 5,
                'result_count_total': 36,
                'registry': {'tmview': {'near_hits': 5}},
            },
        ):
            got = nva.check_tm_registry_global('lucivant', args)
        self.assertEqual(got['status'], 'warn')
        self.assertFalse(got['hard_fail'])
        self.assertEqual(got['reason'], 'tm_registry_near_warning')

    def test_tm_cheap_cache_signature_changes_when_blocklist_changes(self) -> None:
        args = _base_args()
        original = list(nva.CHEAP_TRADEMARK_BLOCKLIST)
        try:
            sig_before = nva.cheap_check_cache_signature('tm_cheap', args)
            nva.CHEAP_TRADEMARK_BLOCKLIST = original + ['collisiontokenx']
            nva._update_cheap_trademark_blocklist_fingerprint()
            sig_after = nva.cheap_check_cache_signature('tm_cheap', args)
        finally:
            nva.CHEAP_TRADEMARK_BLOCKLIST = original
            nva._update_cheap_trademark_blocklist_fingerprint()
        self.assertNotEqual(sig_before, sig_after)


if __name__ == '__main__':
    unittest.main()
