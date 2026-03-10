#!/usr/bin/env python3
"""Tests for rerank_shortlist_deterministic helpers."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

import rerank_shortlist_deterministic as rsd


class RerankShortlistDeterministicTest(unittest.TestCase):
    def test_normalize_name_strips_non_ascii_letters(self) -> None:
        self.assertEqual(rsd.normalize_name(' Veri-Domo 123! '), 'veridomo')

    def test_score_name_penalizes_negative_substring(self) -> None:
        good = rsd.score_name('veridomo', source_shortlist_selected='True', source_recommendation='strong')
        bad = rsd.score_name('assmeter', source_shortlist_selected='True', source_recommendation='strong')
        self.assertGreater(good.total_score, bad.total_score)
        self.assertLess(bad.negative_score, good.negative_score)
        self.assertIn('negative:lexical_hits', ';'.join(bad.reasons))

    def test_score_name_penalizes_lazy_category_suffix(self) -> None:
        good = rsd.score_name('metronim', source_shortlist_selected='True', source_recommendation='strong')
        bad = rsd.score_name('tenantlo', source_shortlist_selected='True', source_recommendation='strong')
        self.assertGreater(good.total_score, bad.total_score)
        self.assertIn('stretch:lazy_category_suffix', ';'.join(bad.reasons))

    def test_score_name_penalizes_generic_compound(self) -> None:
        good = rsd.score_name('velunor', source_shortlist_selected='True', source_recommendation='strong')
        bad = rsd.score_name('smartpayflow', source_shortlist_selected='True', source_recommendation='strong')
        self.assertGreater(good.total_score, bad.total_score)
        self.assertIn('stretch:generic_compound', ';'.join(bad.reasons))

    def test_load_names_filters_shortlist_selected_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'run.csv'
            with path.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['name', 'shortlist_selected', 'recommendation'])
                writer.writeheader()
                writer.writerow({'name': 'veridomo', 'shortlist_selected': 'True', 'recommendation': 'strong'})
                writer.writerow({'name': 'rentflow', 'shortlist_selected': 'False', 'recommendation': 'consider'})
            got = rsd.load_names(path, include_non_shortlist=False)
        self.assertEqual([name for name, _sel, _rec in got], ['veridomo'])

    def test_load_names_include_non_shortlist(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'run.csv'
            with path.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['name', 'shortlist_selected', 'recommendation'])
                writer.writeheader()
                writer.writerow({'name': 'veridomo', 'shortlist_selected': 'True', 'recommendation': 'strong'})
                writer.writerow({'name': 'rentflow', 'shortlist_selected': 'False', 'recommendation': 'consider'})
            got = rsd.load_names(path, include_non_shortlist=True)
        self.assertEqual([name for name, _sel, _rec in got], ['veridomo', 'rentflow'])

    def test_main_prefers_validated_survivors_csv(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / 'campaign'
            runs_dir = out_dir / 'runs'
            postrank_dir = out_dir / 'postrank'
            runs_dir.mkdir(parents=True, exist_ok=True)
            postrank_dir.mkdir(parents=True, exist_ok=True)

            run_csv = runs_dir / 'run_001.csv'
            with run_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['name', 'shortlist_selected', 'recommendation'])
                writer.writeheader()
                writer.writerow({'name': 'metroval', 'shortlist_selected': 'True', 'recommendation': 'strong'})

            survivors_csv = postrank_dir / 'validated_survivors.csv'
            with survivors_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['name', 'shortlist_selected', 'recommendation'])
                writer.writeheader()
                writer.writerow({'name': 'prismora', 'shortlist_selected': 'True', 'recommendation': 'strong'})

            argv = [
                'rerank_shortlist_deterministic.py',
                '--out-dir',
                str(out_dir),
                '--top-n',
                '5',
            ]
            with unittest.mock.patch('sys.argv', argv):
                rc = rsd.main()
            self.assertEqual(rc, 0)

            output_csv = postrank_dir / 'deterministic_rubric_rank.csv'
            output_json = postrank_dir / 'deterministic_rubric_summary.json'
            with output_csv.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual([row['name'] for row in rows], ['prismora'])
            summary = json.loads(output_json.read_text(encoding='utf-8'))
            self.assertEqual(summary['input_mode'], 'validated_survivors')
            self.assertEqual(Path(summary['input_csv']).resolve(), survivors_csv.resolve())

    def test_main_writes_empty_outputs_when_validated_survivors_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / 'campaign'
            runs_dir = out_dir / 'runs'
            postrank_dir = out_dir / 'postrank'
            runs_dir.mkdir(parents=True, exist_ok=True)
            postrank_dir.mkdir(parents=True, exist_ok=True)

            run_csv = runs_dir / 'run_001.csv'
            with run_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['name', 'shortlist_selected', 'recommendation'])
                writer.writeheader()
                writer.writerow({'name': 'metroval', 'shortlist_selected': 'True', 'recommendation': 'strong'})

            survivors_csv = postrank_dir / 'validated_survivors.csv'
            with survivors_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['name', 'shortlist_selected', 'recommendation'])
                writer.writeheader()

            argv = [
                'rerank_shortlist_deterministic.py',
                '--out-dir',
                str(out_dir),
                '--top-n',
                '5',
            ]
            with unittest.mock.patch('sys.argv', argv):
                rc = rsd.main()
            self.assertEqual(rc, 0)

            output_csv = postrank_dir / 'deterministic_rubric_rank.csv'
            output_json = postrank_dir / 'deterministic_rubric_summary.json'
            with output_csv.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows, [])
            summary = json.loads(output_json.read_text(encoding='utf-8'))
            self.assertTrue(summary['empty_input'])
            self.assertEqual(summary['input_mode'], 'validated_survivors')
            self.assertEqual(summary['name_count_scored'], 0)


if __name__ == '__main__':
    unittest.main()
