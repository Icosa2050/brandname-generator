#!/usr/bin/env python3
"""Tests for fuse_postrank_profiles helpers."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import fuse_postrank_profiles as fpp


class FusePostrankProfilesTest(unittest.TestCase):
    def test_derive_weight_from_health_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            postrank_dir = out_dir / 'postrank'
            postrank_dir.mkdir(parents=True, exist_ok=True)
            (postrank_dir / 'health_check.json').write_text(
                json.dumps(
                    {
                        'metrics': {
                            'strong_count': 12,
                            'new_shortlist_count': 33,
                        }
                    }
                ),
                encoding='utf-8',
            )
            weight = fpp.derive_weight_from_out_dir(out_dir, rows=[])
            self.assertAlmostEqual(weight, 12.0 / 33.0, places=4)

    def test_fuse_rankings_combines_overlap(self) -> None:
        quality_rows = [
            {'name': 'Stelarum', 'key': 'stelarum', 'total_score': 97.0, 'recommendation': 'strong'},
            {'name': 'Prismora', 'key': 'prismora', 'total_score': 96.0, 'recommendation': 'strong'},
        ]
        remote_rows = [
            {'name': 'Stelarum', 'key': 'stelarum', 'total_score': 91.0, 'recommendation': 'consider'},
            {'name': 'Velucord', 'key': 'velucord', 'total_score': 95.0, 'recommendation': 'strong'},
        ]
        fused, summary = fpp.fuse_rankings(
            quality_rows=quality_rows,
            remote_rows=remote_rows,
            quality_weight=0.62,
            remote_weight=0.38,
            top_n=10,
            rrf_k=30,
            score_mix=0.5,
        )
        self.assertGreaterEqual(len(fused), 3)
        top = fused[0]
        self.assertEqual(top['name'].lower(), 'stelarum')
        self.assertIn('quality', top['source_profiles'])
        self.assertIn('remote_quality', top['source_profiles'])
        self.assertEqual(summary.get('overlap_count_top_n'), 1)
        self.assertGreater(summary.get('iqr_fusion_score', 0.0), 0.0)

    def test_main_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            quality_dir = root / 'quality'
            remote_dir = root / 'remote_quality'
            out_dir = root / 'fused'
            for campaign_dir in [quality_dir, remote_dir]:
                (campaign_dir / 'postrank').mkdir(parents=True, exist_ok=True)
                (campaign_dir / 'postrank' / 'health_check.json').write_text(
                    json.dumps({'metrics': {'strong_count': 4, 'new_shortlist_count': 10}}),
                    encoding='utf-8',
                )

            with (quality_dir / 'postrank' / 'deterministic_rubric_rank.csv').open(
                'w', encoding='utf-8', newline=''
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=['name', 'total_score', 'recommendation'])
                writer.writeheader()
                writer.writerow({'name': 'Stelarum', 'total_score': '97.2', 'recommendation': 'strong'})
                writer.writerow({'name': 'Prismora', 'total_score': '95.1', 'recommendation': 'consider'})

            with (remote_dir / 'postrank' / 'deterministic_rubric_rank.csv').open(
                'w', encoding='utf-8', newline=''
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=['name', 'total_score', 'recommendation'])
                writer.writeheader()
                writer.writerow({'name': 'Stelarum', 'total_score': '93.4', 'recommendation': 'consider'})
                writer.writerow({'name': 'Velucord', 'total_score': '94.2', 'recommendation': 'strong'})

            argv = [
                'fuse_postrank_profiles.py',
                '--quality-out-dir',
                str(quality_dir),
                '--remote-quality-out-dir',
                str(remote_dir),
                '--out-dir',
                str(out_dir),
                '--top-n',
                '5',
            ]
            with mock.patch('sys.argv', argv):
                rc = fpp.main()
            self.assertEqual(rc, 0)

            out_csv = out_dir / 'postrank' / 'fused_quality_remote_rank.csv'
            out_json = out_dir / 'postrank' / 'fused_quality_remote_summary.json'
            self.assertTrue(out_csv.exists())
            self.assertTrue(out_json.exists())

            with out_csv.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            self.assertGreaterEqual(len(rows), 3)
            self.assertEqual(rows[0]['name'].lower(), 'stelarum')

    def test_main_writes_empty_outputs_for_empty_rank_csvs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            quality_dir = root / 'quality'
            remote_dir = root / 'remote_quality'
            out_dir = root / 'fused'
            for campaign_dir in [quality_dir, remote_dir]:
                (campaign_dir / 'postrank').mkdir(parents=True, exist_ok=True)
                (campaign_dir / 'postrank' / 'health_check.json').write_text(
                    json.dumps({'metrics': {'strong_count': 0, 'new_shortlist_count': 0}}),
                    encoding='utf-8',
                )
                with (campaign_dir / 'postrank' / 'deterministic_rubric_rank.csv').open(
                    'w', encoding='utf-8', newline=''
                ) as handle:
                    writer = csv.DictWriter(handle, fieldnames=['name', 'total_score', 'recommendation'])
                    writer.writeheader()

            argv = [
                'fuse_postrank_profiles.py',
                '--quality-out-dir',
                str(quality_dir),
                '--remote-quality-out-dir',
                str(remote_dir),
                '--out-dir',
                str(out_dir),
                '--top-n',
                '5',
            ]
            with mock.patch('sys.argv', argv):
                rc = fpp.main()
            self.assertEqual(rc, 0)

            out_csv = out_dir / 'postrank' / 'fused_quality_remote_rank.csv'
            out_json = out_dir / 'postrank' / 'fused_quality_remote_summary.json'
            with out_csv.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows, [])
            summary = json.loads(out_json.read_text(encoding='utf-8'))
            self.assertEqual(summary['rows']['fused'], 0)
            self.assertEqual(summary['top_names'], [])


if __name__ == '__main__':
    unittest.main()
