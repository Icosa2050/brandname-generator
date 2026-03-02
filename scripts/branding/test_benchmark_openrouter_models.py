#!/usr/bin/env python3
"""Tests for benchmark_openrouter_models helpers."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

import benchmark_openrouter_models as bom


class BenchmarkOpenRouterModelsTest(unittest.TestCase):
    def test_parse_model_list_deduplicates_and_trims(self) -> None:
        got = bom.parse_model_list(
            '  mistralai/mistral-small-creative , qwen/qwen3-next-80b-a3b-instruct, '
            'mistralai/mistral-small-creative, ,anthropic/claude-sonnet-4.6 '
        )
        self.assertEqual(
            got,
            [
                'mistralai/mistral-small-creative',
                'qwen/qwen3-next-80b-a3b-instruct',
                'anthropic/claude-sonnet-4.6',
            ],
        )

    def test_model_slug_normalizes_path_and_tag(self) -> None:
        self.assertEqual(bom.model_slug('qwen/qwen3-next-80b-a3b-instruct:free'), 'qwen__qwen3-next-80b-a3b-instruct__free')

    def test_read_progress_metrics_returns_fallback_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            got = bom.read_progress_metrics(Path(td))
        self.assertEqual(got.get('llm_stage_status'), 'no_progress')
        self.assertEqual(got.get('new_shortlist_count'), 0)

    def test_read_campaign_summary_parses_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            summary = out_dir / 'campaign_summary.json'
            summary.write_text('{"status":"assert_failed","errors":1}\n', encoding='utf-8')
            got = bom.read_campaign_summary(out_dir)
        self.assertEqual(got.get('status'), 'assert_failed')
        self.assertEqual(got.get('errors'), 1)

    def test_read_progress_metrics_parses_last_row(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td)
            progress = out_dir / 'campaign_progress.csv'
            with progress.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        'llm_stage_status',
                        'llm_candidate_count',
                        'shortlist_count',
                        'new_shortlist_count',
                        'llm_slo_status',
                        'status',
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        'llm_stage_status': 'ok',
                        'llm_candidate_count': '41',
                        'shortlist_count': '12',
                        'new_shortlist_count': '9',
                        'llm_slo_status': 'pass',
                        'status': 'ok',
                    }
                )
            got = bom.read_progress_metrics(out_dir)
        self.assertEqual(got.get('llm_stage_status'), 'ok')
        self.assertEqual(got.get('llm_candidate_count'), 41)
        self.assertEqual(got.get('shortlist_count'), 12)
        self.assertEqual(got.get('new_shortlist_count'), 9)
        self.assertEqual(got.get('llm_slo_status'), 'pass')
        self.assertEqual(got.get('status'), 'ok')

    def test_pick_bundle_models_known_bundle(self) -> None:
        self.assertEqual(bom.pick_bundle_models('a'), bom.MODEL_BUNDLE_A)
        self.assertEqual(bom.pick_bundle_models('flash'), bom.MODEL_BUNDLE_FLASH)

    def test_pick_bundle_models_unknown_bundle_raises(self) -> None:
        with self.assertRaises(ValueError):
            bom.pick_bundle_models('nope')

    def test_select_recommended_models_demotes_timeout(self) -> None:
        ok = bom.BenchmarkResult(
            model='anthropic/claude-sonnet-4.6',
            out_dir='/tmp/a',
            return_code=0,
            duration_s=8.0,
            llm_stage_status='ok',
            llm_candidate_count=42,
            shortlist_count=30,
            new_shortlist_count=30,
            llm_slo_status='pass',
            llm_slo_success_rate=1.0,
            llm_slo_timeout_rate=0.0,
            llm_slo_breaches='',
            status='ok',
            sample_names=['veridomo'],
            log_path='/tmp/a/run.log',
        )
        bad = bom.BenchmarkResult(
            model='qwen/qwen3.5-flash-02-23',
            out_dir='/tmp/b',
            return_code=0,
            duration_s=120.0,
            llm_stage_status='stage_timeout',
            llm_candidate_count=14,
            shortlist_count=8,
            new_shortlist_count=8,
            llm_slo_status='breach',
            llm_slo_success_rate=0.5,
            llm_slo_timeout_rate=0.5,
            llm_slo_breaches='[\"timeout_rate\"]',
            status='ok',
            sample_names=['rentflow'],
            log_path='/tmp/b/run.log',
        )
        recommended, reasons = bom.select_recommended_models(results=[bad, ok], timeout_threshold=0.35)
        self.assertEqual(recommended, ['anthropic/claude-sonnet-4.6'])
        self.assertEqual(reasons.get('qwen/qwen3.5-flash-02-23'), 'stage_timeout')

    def test_select_recommended_models_forces_best_when_all_demoted(self) -> None:
        only = bom.BenchmarkResult(
            model='qwen/qwen3.5-flash-02-23',
            out_dir='/tmp/b',
            return_code=0,
            duration_s=120.0,
            llm_stage_status='stage_timeout',
            llm_candidate_count=14,
            shortlist_count=8,
            new_shortlist_count=8,
            llm_slo_status='breach',
            llm_slo_success_rate=0.5,
            llm_slo_timeout_rate=0.5,
            llm_slo_breaches='[\"timeout_rate\"]',
            status='ok',
            sample_names=['rentflow'],
            log_path='/tmp/b/run.log',
        )
        recommended, reasons = bom.select_recommended_models(results=[only], timeout_threshold=0.35)
        self.assertEqual(recommended, ['qwen/qwen3.5-flash-02-23'])
        self.assertEqual(
            reasons.get('qwen/qwen3.5-flash-02-23'),
            'forced_keep_no_models_passed_demote_rule',
        )

    def test_model_demote_reason_demotes_non_ok_status(self) -> None:
        result = bom.BenchmarkResult(
            model='anthropic/claude-sonnet-4.6',
            out_dir='/tmp/a',
            return_code=0,
            duration_s=3.0,
            llm_stage_status='ok',
            llm_candidate_count=12,
            shortlist_count=10,
            new_shortlist_count=10,
            llm_slo_status='pass',
            llm_slo_success_rate=1.0,
            llm_slo_timeout_rate=0.0,
            llm_slo_breaches='',
            status='assert_failed',
            sample_names=['verodomo'],
            log_path='/tmp/a/run.log',
        )
        self.assertEqual(
            bom.model_demote_reason(result=result, timeout_threshold=0.35),
            'status=assert_failed',
        )


if __name__ == '__main__':
    unittest.main()
