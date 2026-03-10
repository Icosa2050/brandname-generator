#!/usr/bin/env python3
"""Tests for finalize_fused_tmview_publish."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import finalize_fused_tmview_publish as fftp
from euipo_esearch_probe import EuipoProbeResult


class FinalizeFusedTmviewPublishTest(unittest.TestCase):
    def test_classify_tmview_result_rejects_active_exact(self) -> None:
        result = EuipoProbeResult(
            name='stelarum',
            url='https://example.test',
            query_ok=True,
            source='tmview_playwright',
            exact_hits=2,
            near_hits=0,
            result_count=2,
            sample_text='',
            active_exact_hits=1,
            inactive_exact_hits=1,
            unknown_exact_hits=0,
        )
        bucket, reason = fftp.classify_tmview_result(result, inactive_exact_policy='review')
        self.assertEqual(bucket, 'rejected')
        self.assertEqual(reason, 'tmview_exact_active_collision')

    def test_classify_tmview_result_reviews_inactive_exact(self) -> None:
        result = EuipoProbeResult(
            name='equidral',
            url='https://example.test',
            query_ok=True,
            source='tmview_playwright',
            exact_hits=5,
            near_hits=1,
            result_count=6,
            sample_text='',
            active_exact_hits=0,
            inactive_exact_hits=5,
            unknown_exact_hits=0,
        )
        bucket, reason = fftp.classify_tmview_result(result, inactive_exact_policy='review')
        self.assertEqual(bucket, 'review')
        self.assertEqual(reason, 'tmview_exact_inactive_review')

    def test_finalize_rows_routes_publish_review_rejected(self) -> None:
        rows = [
            {'rank': '1', 'name': 'solvaivo', 'fusion_score': '0.1'},
            {'rank': '2', 'name': 'equidral', 'fusion_score': '0.09'},
            {'rank': '3', 'name': 'stelarum', 'fusion_score': '0.08'},
        ]
        probes = {
            'solvaivo': EuipoProbeResult(
                name='solvaivo',
                url='https://example.test/solvaivo',
                query_ok=True,
                source='tmview_playwright',
                exact_hits=0,
                near_hits=0,
                result_count=0,
                sample_text='',
            ),
            'equidral': EuipoProbeResult(
                name='equidral',
                url='https://example.test/equidral',
                query_ok=True,
                source='tmview_playwright',
                exact_hits=5,
                near_hits=1,
                result_count=6,
                sample_text='EQUIDRAL expired',
                active_exact_hits=0,
                inactive_exact_hits=5,
                unknown_exact_hits=0,
            ),
            'stelarum': EuipoProbeResult(
                name='stelarum',
                url='https://example.test/stelarum',
                query_ok=True,
                source='tmview_playwright',
                exact_hits=1,
                near_hits=0,
                result_count=1,
                sample_text='STELARUM registered',
                active_exact_hits=1,
                inactive_exact_hits=0,
                unknown_exact_hits=0,
            ),
        }
        publish_rows, review_rows, rejected_rows = fftp.finalize_rows(
            rows,
            top_n=3,
            probes=probes,
            inactive_exact_policy='review',
        )
        self.assertEqual([row['name'] for row in publish_rows], ['solvaivo'])
        self.assertEqual([row['name'] for row in review_rows], ['equidral'])
        self.assertEqual([row['name'] for row in rejected_rows], ['stelarum'])
        self.assertEqual(publish_rows[0]['final_publish_rank'], 1)

    def test_main_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            postrank_dir = root / 'postrank'
            postrank_dir.mkdir(parents=True, exist_ok=True)
            input_csv = postrank_dir / 'fused_quality_remote_rank.csv'
            with input_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['rank', 'name', 'fusion_score', 'recommendation'])
                writer.writeheader()
                writer.writerow({'rank': '1', 'name': 'solvaivo', 'fusion_score': '0.1', 'recommendation': 'strong'})
                writer.writerow({'rank': '2', 'name': 'equidral', 'fusion_score': '0.09', 'recommendation': 'strong'})

            probes = {
                'solvaivo': EuipoProbeResult(
                    name='solvaivo',
                    url='https://example.test/solvaivo',
                    query_ok=True,
                    source='tmview_playwright',
                    exact_hits=0,
                    near_hits=0,
                    result_count=0,
                    sample_text='',
                ),
                'equidral': EuipoProbeResult(
                    name='equidral',
                    url='https://example.test/equidral',
                    query_ok=True,
                    source='tmview_playwright',
                    exact_hits=2,
                    near_hits=0,
                    result_count=2,
                    sample_text='EQUIDRAL expired',
                    active_exact_hits=0,
                    inactive_exact_hits=2,
                    unknown_exact_hits=0,
                ),
            }

            argv = [
                'finalize_fused_tmview_publish.py',
                '--input-csv',
                str(input_csv),
                '--out-dir',
                str(root),
                '--top-n',
                '5',
            ]
            with mock.patch('sys.argv', argv), mock.patch.object(fftp, 'probe_names', return_value=probes):
                rc = fftp.main()
            self.assertEqual(rc, 0)

            publish_csv = postrank_dir / 'fused_publish_final.csv'
            review_csv = postrank_dir / 'fused_review_queue.csv'
            summary_json = postrank_dir / 'fused_tmview_gate_summary.json'
            probe_json = postrank_dir / 'fused_tmview_probe.json'

            self.assertTrue(publish_csv.exists())
            self.assertTrue(review_csv.exists())
            self.assertTrue(summary_json.exists())
            self.assertTrue(probe_json.exists())

            with publish_csv.open('r', encoding='utf-8', newline='') as handle:
                publish_rows = list(csv.DictReader(handle))
            with review_csv.open('r', encoding='utf-8', newline='') as handle:
                review_rows = list(csv.DictReader(handle))
            summary = json.loads(summary_json.read_text(encoding='utf-8'))

            self.assertEqual([row['name'] for row in publish_rows], ['solvaivo'])
            self.assertEqual([row['name'] for row in review_rows], ['equidral'])
            self.assertEqual(summary['publish_count'], 1)
            self.assertEqual(summary['review_count'], 1)

    def test_main_empty_input_writes_empty_outputs_and_returns_zero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            postrank_dir = root / 'postrank'
            postrank_dir.mkdir(parents=True, exist_ok=True)
            input_csv = postrank_dir / 'fused_quality_remote_rank.csv'
            input_csv.write_text('rank,name,fusion_score,recommendation\n', encoding='utf-8')

            argv = [
                'finalize_fused_tmview_publish.py',
                '--input-csv',
                str(input_csv),
                '--out-dir',
                str(root),
                '--top-n',
                '5',
            ]
            with mock.patch('sys.argv', argv), mock.patch.object(fftp, 'probe_names') as mock_probe_names:
                rc = fftp.main()
            self.assertEqual(rc, 0)
            mock_probe_names.assert_not_called()

            publish_csv = postrank_dir / 'fused_publish_final.csv'
            review_csv = postrank_dir / 'fused_review_queue.csv'
            rejected_csv = postrank_dir / 'fused_rejected.csv'
            summary_json = postrank_dir / 'fused_tmview_gate_summary.json'
            probe_json = postrank_dir / 'fused_tmview_probe.json'

            self.assertTrue(publish_csv.exists())
            self.assertTrue(review_csv.exists())
            self.assertTrue(rejected_csv.exists())
            self.assertTrue(summary_json.exists())
            self.assertTrue(probe_json.exists())

            with publish_csv.open('r', encoding='utf-8', newline='') as handle:
                self.assertEqual(list(csv.DictReader(handle)), [])
            with review_csv.open('r', encoding='utf-8', newline='') as handle:
                self.assertEqual(list(csv.DictReader(handle)), [])
            with rejected_csv.open('r', encoding='utf-8', newline='') as handle:
                self.assertEqual(list(csv.DictReader(handle)), [])

            summary = json.loads(summary_json.read_text(encoding='utf-8'))
            probes = json.loads(probe_json.read_text(encoding='utf-8'))
            self.assertEqual(summary['checked_count'], 0)
            self.assertEqual(summary['publish_count'], 0)
            self.assertEqual(summary['review_count'], 0)
            self.assertEqual(summary['rejected_count'], 0)
            self.assertEqual(probes, [])

    def test_main_empty_input_can_fail_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            postrank_dir = root / 'postrank'
            postrank_dir.mkdir(parents=True, exist_ok=True)
            input_csv = postrank_dir / 'fused_quality_remote_rank.csv'
            input_csv.write_text('rank,name,fusion_score,recommendation\n', encoding='utf-8')

            argv = [
                'finalize_fused_tmview_publish.py',
                '--input-csv',
                str(input_csv),
                '--out-dir',
                str(root),
                '--fail-on-empty-publish',
            ]
            with mock.patch('sys.argv', argv):
                rc = fftp.main()
            self.assertEqual(rc, 4)


if __name__ == '__main__':
    unittest.main()
