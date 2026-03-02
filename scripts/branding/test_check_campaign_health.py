#!/usr/bin/env python3
"""Tests for check_campaign_health helpers."""

from __future__ import annotations

import unittest

import check_campaign_health as cch


class CheckCampaignHealthTest(unittest.TestCase):
    def test_evaluate_health_passes_with_good_metrics(self) -> None:
        cfg = cch.HealthConfig(
            min_new_shortlist=10,
            min_postrank_strong=6,
            min_iqr=10.0,
            max_ceiling_share=0.20,
            require_llm_stage_ok=True,
        )
        progress_row = {
            'llm_stage_status': 'ok',
            'new_shortlist_count': '18',
        }
        postrank_summary = {
            'iqr_total_score': 14.5,
            'score_ceiling_share': 0.08,
            'recommendation_counts': {'strong': 9},
        }
        result = cch.evaluate_health(progress_row=progress_row, postrank_summary=postrank_summary, cfg=cfg)
        self.assertTrue(result['healthy'])
        self.assertEqual(result['violations'], [])

    def test_evaluate_health_flags_missing_postrank(self) -> None:
        cfg = cch.HealthConfig(
            min_new_shortlist=5,
            min_postrank_strong=3,
            min_iqr=8.0,
            max_ceiling_share=0.25,
            require_llm_stage_ok=True,
        )
        progress_row = {
            'llm_stage_status': 'ok',
            'new_shortlist_count': '12',
        }
        result = cch.evaluate_health(progress_row=progress_row, postrank_summary={}, cfg=cfg)
        self.assertFalse(result['healthy'])
        self.assertIn('postrank_summary_missing', result['violations'])

    def test_evaluate_health_flags_low_discrimination(self) -> None:
        cfg = cch.HealthConfig(
            min_new_shortlist=5,
            min_postrank_strong=3,
            min_iqr=10.0,
            max_ceiling_share=0.20,
            require_llm_stage_ok=True,
        )
        progress_row = {
            'llm_stage_status': 'ok',
            'new_shortlist_count': '15',
        }
        postrank_summary = {
            'iqr_total_score': 3.2,
            'score_ceiling_share': 0.45,
            'recommendation_counts': {'strong': 6},
        }
        result = cch.evaluate_health(progress_row=progress_row, postrank_summary=postrank_summary, cfg=cfg)
        self.assertFalse(result['healthy'])
        self.assertIn('iqr_below_threshold:3.20<10.00', result['violations'])
        self.assertIn('ceiling_share_above_threshold:0.4500>0.2000', result['violations'])

    def test_evaluate_health_allows_non_ok_when_configured(self) -> None:
        cfg = cch.HealthConfig(
            min_new_shortlist=5,
            min_postrank_strong=2,
            min_iqr=8.0,
            max_ceiling_share=0.30,
            require_llm_stage_ok=False,
        )
        progress_row = {
            'llm_stage_status': 'ok_llm_degraded_empty',
            'new_shortlist_count': '7',
        }
        postrank_summary = {
            'iqr_total_score': 9.0,
            'score_ceiling_share': 0.10,
            'recommendation_counts': {'strong': 2},
        }
        result = cch.evaluate_health(progress_row=progress_row, postrank_summary=postrank_summary, cfg=cfg)
        self.assertTrue(result['healthy'])


if __name__ == '__main__':
    unittest.main()
