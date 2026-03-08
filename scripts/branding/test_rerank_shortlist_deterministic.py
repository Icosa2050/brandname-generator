#!/usr/bin/env python3
"""Tests for rerank_shortlist_deterministic helpers."""

from __future__ import annotations

import csv
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


if __name__ == '__main__':
    unittest.main()
