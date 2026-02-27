#!/usr/bin/env python3
"""Tests for decision-pack builder."""

from __future__ import annotations

import csv
import subprocess
import tempfile
import unittest
from pathlib import Path

import naming_db as ndb


STRICT_CHECKS = ('domain', 'web', 'app_store', 'package', 'social')


class BuildDecisionPackTest(unittest.TestCase):
    def _seed_db(self, db_path: Path) -> None:
        with ndb.open_connection(db_path) as conn:
            ndb.ensure_schema(conn)
            run_id = ndb.create_run(
                conn,
                source_path=str(db_path),
                scope='global',
                gate_mode='strict',
                variation_profile='test',
                status='completed',
                config={},
                summary={},
            )

            cid_a = ndb.upsert_candidate(
                conn,
                name_display='verasettle',
                total_score=90.0,
                risk_score=18.0,
                recommendation='strong',
                quality_score=92.0,
                engine_id='explicit',
                parent_ids='',
                status='checked',
                rejection_reason='',
            )
            cid_b = ndb.upsert_candidate(
                conn,
                name_display='settledue',
                total_score=82.0,
                risk_score=25.0,
                recommendation='consider',
                quality_score=85.0,
                engine_id='explicit',
                parent_ids='',
                status='checked',
                rejection_reason='',
            )
            cid_c = ndb.upsert_candidate(
                conn,
                name_display='nexusett',
                total_score=79.0,
                risk_score=31.0,
                recommendation='consider',
                quality_score=80.0,
                engine_id='explicit',
                parent_ids='',
                status='checked',
                rejection_reason='',
            )
            cid_d = ndb.upsert_candidate(
                conn,
                name_display='badcollision',
                total_score=77.0,
                risk_score=40.0,
                recommendation='strong',
                quality_score=75.0,
                engine_id='explicit',
                parent_ids='',
                status='checked',
                rejection_reason='',
            )

            conn.execute("UPDATE candidates SET state='checked', status='checked' WHERE id IN (?, ?, ?, ?)", (cid_a, cid_b, cid_c, cid_d))
            ndb.add_shortlist_decision(
                conn,
                candidate_id=cid_a,
                run_id=run_id,
                selected=True,
                shortlist_rank=1,
                bucket_key='test',
                reason='',
                score=90.0,
            )

            # A: strict strong (all pass)
            for check in STRICT_CHECKS:
                ndb.add_validation_result(
                    conn,
                    candidate_id=cid_a,
                    run_id=run_id,
                    check_type=check,
                    status='pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )

            # B: strict good consider (all warn/pass, no fail)
            for check in STRICT_CHECKS:
                ndb.add_validation_result(
                    conn,
                    candidate_id=cid_b,
                    run_id=run_id,
                    check_type=check,
                    status='warn' if check in {'web', 'social'} else 'pass',
                    score_delta=0.0,
                    hard_fail=False,
                    reason='',
                    evidence={},
                )

            # C: forward candidate (incomplete expensive checks)
            ndb.add_validation_result(
                conn,
                candidate_id=cid_c,
                run_id=run_id,
                check_type='domain',
                status='pass',
                score_delta=0.0,
                hard_fail=False,
                reason='',
                evidence={},
            )

            # D: bad candidate (fail)
            for check in STRICT_CHECKS:
                ndb.add_validation_result(
                    conn,
                    candidate_id=cid_d,
                    run_id=run_id,
                    check_type=check,
                    status='fail' if check == 'web' else 'pass',
                    score_delta=0.0,
                    hard_fail=check == 'web',
                    reason='web_exact_collision' if check == 'web' else '',
                    evidence={},
                )

            conn.commit()

    def test_build_decision_pack_outputs_expected_sets(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        script = repo_root / 'scripts/branding/build_decision_pack.py'

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / 'naming_campaign.db'
            out_dir = tmp / 'packs'
            self._seed_db(db_path)

            subprocess.run(
                [
                    'python3',
                    str(script),
                    '--db',
                    str(db_path),
                    '--out-dir',
                    str(out_dir),
                    '--review-tiers',
                    '3,2',
                ],
                check=True,
            )

            packs = sorted(out_dir.glob('decision_pack_*'))
            self.assertEqual(len(packs), 1)
            pack = packs[0]

            with (pack / 'strict_strong.csv').open('r', encoding='utf-8', newline='') as handle:
                strict_strong = list(csv.DictReader(handle))
            with (pack / 'strict_good.csv').open('r', encoding='utf-8', newline='') as handle:
                strict_good = list(csv.DictReader(handle))
            with (pack / 'brand_forward_needs_expensive_checks.csv').open('r', encoding='utf-8', newline='') as handle:
                forward = list(csv.DictReader(handle))
            with (pack / 'review_unique_top3.csv').open('r', encoding='utf-8', newline='') as handle:
                review_top3 = list(csv.DictReader(handle))
            with (pack / 'review_unique_top2.csv').open('r', encoding='utf-8', newline='') as handle:
                review_top2 = list(csv.DictReader(handle))

            self.assertEqual([r['name_normalized'] for r in strict_strong], ['verasettle'])
            self.assertEqual([r['name_normalized'] for r in strict_good], ['verasettle', 'settledue'])
            self.assertIn('nexusett', [r['name_normalized'] for r in forward])
            self.assertNotIn('badcollision', [r['name_normalized'] for r in strict_good])

            self.assertEqual(len(review_top3), 3)
            self.assertEqual(review_top3[0]['name_normalized'], 'verasettle')
            self.assertEqual(review_top3[0]['source_lane'], 'strict_strong')
            self.assertEqual(review_top3[1]['name_normalized'], 'settledue')
            self.assertEqual(review_top3[1]['source_lane'], 'strict_good')
            self.assertEqual(review_top3[2]['name_normalized'], 'nexusett')
            self.assertEqual(review_top3[2]['source_lane'], 'needs_expensive')
            self.assertEqual(len(review_top2), 2)

            self.assertTrue((pack / 'README.md').exists())
            self.assertTrue((pack / 'manifest.json').exists())


if __name__ == '__main__':
    unittest.main()
