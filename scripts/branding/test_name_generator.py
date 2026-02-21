#!/usr/bin/env python3
"""Focused regression tests for name_generator helpers."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import name_generator as ng


class _PassthroughFilter:
    filter_id = 'passthrough'

    def apply(self, request: ng.FilterRequest) -> list[ng.GeneratedCandidate]:
        return list(request.generated)


class NameGeneratorTest(unittest.TestCase):
    def test_load_llm_fallback_candidates_rereads_file_on_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'llm_candidates.json'
            path.write_text('', encoding='utf-8')

            with (
                mock.patch.object(
                    ng.Path,
                    'read_text',
                    side_effect=['', '{"candidates":[{"name":"Verodomo"},{"name":"Clarivio"}]}'],
                ) as mock_read,
                mock.patch('name_generator.time.sleep') as mock_sleep,
            ):
                names = ng.load_llm_fallback_candidates(
                    path=str(path),
                    max_attempts=2,
                    backoff_ms=5,
                    allow_text_fallback=False,
                )

            self.assertEqual(names, ['clarivio', 'verodomo'])
            self.assertEqual(mock_read.call_count, 2)
            self.assertEqual(mock_sleep.call_count, 1)

    def test_quality_first_template_gate_is_opt_in(self) -> None:
        item = ng.GeneratedCandidate(name='verodomo', generator_family='seed', lineage_atoms=['vero', 'domo'])
        with (
            mock.patch('name_generator.challenge_risk', return_value=(12, 9, 'objego', 0, 0)),
            mock.patch('name_generator.adversarial_similarity_signal', return_value=(0, '')),
            mock.patch('name_generator.gibberish_signal', return_value=(0, '')),
            mock.patch('name_generator.false_friend_signal', return_value=(0, '')),
            mock.patch('name_generator.template_likeness_signal', return_value=(24, 'templatic')),
            mock.patch('name_generator.psych_spelling_risk', return_value=3),
            mock.patch('name_generator.psych_trust_proxy_score', return_value=78),
        ):
            no_gate = ng.evaluate_candidates(
                scope='global',
                generated_items=[item],
                similarity_fail_threshold=95,
                false_friend_fail_threshold=95,
                gibberish_fail_threshold=95,
                false_friend_rules={},
                quality_first=False,
            )[0]
            with_gate = ng.evaluate_candidates(
                scope='global',
                generated_items=[item],
                similarity_fail_threshold=95,
                false_friend_fail_threshold=95,
                gibberish_fail_threshold=95,
                false_friend_rules={},
                quality_first=True,
                quality_max_template_penalty=18,
            )[0]

        self.assertFalse(no_gate.hard_fail)
        self.assertTrue(with_gate.hard_fail)
        self.assertEqual(with_gate.fail_reason, 'quality_template_like')

    def test_quality_first_trust_proxy_gate(self) -> None:
        item = ng.GeneratedCandidate(name='clarivo', generator_family='seed', lineage_atoms=['clari', 'vo'])
        with (
            mock.patch('name_generator.challenge_risk', return_value=(12, 9, 'objego', 0, 0)),
            mock.patch('name_generator.adversarial_similarity_signal', return_value=(0, '')),
            mock.patch('name_generator.gibberish_signal', return_value=(0, '')),
            mock.patch('name_generator.false_friend_signal', return_value=(0, '')),
            mock.patch('name_generator.template_likeness_signal', return_value=(0, '')),
            mock.patch('name_generator.psych_spelling_risk', return_value=3),
            mock.patch('name_generator.psych_trust_proxy_score', return_value=52),
        ):
            got = ng.evaluate_candidates(
                scope='global',
                generated_items=[item],
                similarity_fail_threshold=95,
                false_friend_fail_threshold=95,
                gibberish_fail_threshold=95,
                false_friend_rules={},
                quality_first=True,
                quality_min_trust_proxy=64,
            )[0]
        self.assertTrue(got.hard_fail)
        self.assertEqual(got.fail_reason, 'quality_trust_proxy_low')

    def test_generate_candidates_seed_family_respects_quota(self) -> None:
        generated = ng.generate_candidates(
            scope='global',
            seeds=['clarity'],
            min_len=5,
            max_len=12,
            variation_profile='expanded',
            generator_families=['seed', 'coined'],
            family_quotas={'seed': 3, 'coined': 0},
            source_atoms=[],
            source_influence_share=0.25,
            max_per_prefix2=99,
            max_per_suffix2=99,
            max_per_shape=99,
            max_per_family=99,
            filter_engine=_PassthroughFilter(),
        )
        self.assertLessEqual(len(generated), 3)
        self.assertTrue(all(item.generator_family == 'seed' for item in generated))
        self.assertIn('clarity', {item.name for item in generated})

    def test_rebalance_family_quotas_for_source_influence(self) -> None:
        out = ng.rebalance_family_quotas_for_source_influence(
            active_families=['coined', 'source_pool', 'blend'],
            family_quotas={'coined': 200, 'source_pool': 200, 'blend': 200},
            source_influence_share=0.20,
        )
        self.assertEqual(out['coined'], 200)
        self.assertLess(out['source_pool'], 200)
        self.assertLess(out['blend'], 200)


if __name__ == '__main__':
    unittest.main()
