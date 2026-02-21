#!/usr/bin/env python3
"""Fixture-driven tests for naming_ideation_stage helpers."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest import mock
from urllib import error

import naming_ideation_stage as nide


class _FakeHTTPResponse:
    def __init__(self, text: str) -> None:
        self._blob = text.encode('utf-8')

    def __enter__(self) -> '_FakeHTTPResponse':
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    def read(self) -> bytes:
        return self._blob


class NamingIdeationStageTest(unittest.TestCase):
    def test_parse_candidate_payload_valid_json(self) -> None:
        raw = json.dumps({'candidates': [{'name': 'Veribill'}, {'name': 'Nexum1'}, {'name': 'x1'}]})
        got = nide.parse_candidate_payload(raw)
        self.assertIn('veribill', got)
        self.assertNotIn('nexum1', got)
        self.assertNotIn('x1', got)

    def test_parse_candidate_payload_wrapped_json(self) -> None:
        raw = 'model output\n{"candidates":[{"name":"Dividis"},{"name":"Exactis"}]}\nthanks'
        got = nide.parse_candidate_payload(raw)
        self.assertEqual(got, ['dividis', 'exactis'])

    def test_load_fixture_candidates_line_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'fixture.txt'
            path.write_text('fairbill\n***\nveribill\nnex-bill\n', encoding='utf-8')
            got = nide.load_fixture_candidates(str(path))
            self.assertIn('fairbill', got)
            self.assertIn('veribill', got)
            self.assertIn('nexbill', got)  # punctuation is stripped by normalize_alpha_name

    def test_parse_candidate_payload_empty(self) -> None:
        self.assertEqual(nide.parse_candidate_payload(''), [])

    def test_load_context_packet_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'ctx.json'
            path.write_text(
                json.dumps(
                    {
                        'llm_context': {
                            'product_core': '  Trusted settlement engine for landlords and tenants   ',
                            'target_users': ['landlords', 'property managers', 'landlords'],
                            'trust_signals': 'clarity,accuracy,compliance',
                            'forbidden_directions': ['crypto', 'casino', 'emoji-heavy'],
                            'language_market': 'DACH + EU',
                            'tone_mix': {'trust': 0.6, 'modern': 0.3, 'playful': 0.1},
                            'good_examples': ['verodomo', 'settlewise'],
                            'bad_examples': ['fairbill123', 'x'],
                            'seed_roots': ['ratio', 'tenant', 'ledger', 'trusted'],
                            'notes': 'Avoid legal-risk signaling words.',
                        }
                    }
                )
                + '\n',
                encoding='utf-8',
            )
            got = nide.load_context_packet(str(path))
            self.assertEqual(got['product_core'], 'Trusted settlement engine for landlords and tenants')
            self.assertEqual(got['target_users'], ['landlords', 'property managers'])
            self.assertEqual(got['trust_signals'], ['clarity', 'accuracy', 'compliance'])
            self.assertIn('tone_mix', got)
            self.assertEqual(got['good_examples'], ['verodomo', 'settlewise'])
            self.assertEqual(got['bad_examples'], ['fairbill'])
            self.assertIn('seed_roots', got)

    def test_load_context_packet_missing_file(self) -> None:
        with self.assertRaisesRegex(ValueError, 'context_file_not_found'):
            nide.load_context_packet('/tmp/does-not-exist-ctx-packet.json')

    def test_build_prompt_includes_context_block(self) -> None:
        context = {
            'product_core': 'settlement automation for landlords and tenants',
            'target_users': ['landlords', 'property managers'],
            'trust_signals': ['clarity', 'reliability'],
            'seed_roots': ['ratio', 'tenant'],
        }
        prompt, _mode = nide.build_prompt(
            scope='global',
            round_index=0,
            target_count=10,
            constraints={'banned_tokens': ['fair'], 'banned_prefixes': ['fai']},
            context_packet=context,
        )
        self.assertIn('Context packet:', prompt)
        self.assertIn('product_core: settlement automation for landlords and tenants', prompt)
        self.assertIn('target_users: landlords, property managers', prompt)
        self.assertIn('align with context packet priorities when provided', prompt)

    @mock.patch('naming_ideation_stage.request.urlopen', side_effect=TimeoutError())
    def test_call_openrouter_candidates_timeout(self, _mock_urlopen: mock.Mock) -> None:
        names, usage, err = nide.call_openrouter_candidates(
            api_key='k',
            model='mistralai/mistral-small-creative',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
        )
        self.assertEqual(names, [])
        self.assertEqual(usage, {})
        self.assertEqual(err, 'timeout')

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_call_openrouter_candidates_fallback_after_http_404(self, mock_urlopen: mock.Mock) -> None:
        first_error = error.HTTPError(
            url='https://openrouter.ai/api/v1/chat/completions',
            code=404,
            msg='not found',
            hdrs=None,
            fp=BytesIO(b'{"error":"model not found"}'),
        )
        second_payload = json.dumps(
            {
                'choices': [
                    {
                        'message': {
                            'content': '{"candidates":[{"name":"Verodomo"},{"name":"Fairbill"}]}'
                        }
                    }
                ],
                'usage': {'prompt_tokens': 12, 'completion_tokens': 34},
            }
        )
        mock_urlopen.side_effect = [first_error, _FakeHTTPResponse(second_payload)]

        names, usage, err = nide.call_openrouter_candidates(
            api_key='k',
            model='mistralai/mistral-small-creative',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
        )
        self.assertEqual(err, '')
        self.assertEqual(sorted(names), ['fairbill', 'verodomo'])
        self.assertEqual(usage.get('prompt_tokens'), 12)

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_call_openrouter_candidates_sets_attribution_headers(self, mock_urlopen: mock.Mock) -> None:
        captured_headers: dict[str, str] = {}

        def _fake_urlopen(req: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            if hasattr(req, 'header_items'):
                captured_headers.update({k.lower(): v for k, v in req.header_items()})
            payload = json.dumps(
                {
                    'choices': [{'message': {'content': '{"candidates":[{"name":"Verodomo"}]}'}}],
                    'usage': {'cost': 0.0123},
                }
            )
            return _FakeHTTPResponse(payload)

        mock_urlopen.side_effect = _fake_urlopen
        names, usage, err = nide.call_openrouter_candidates(
            api_key='k',
            model='mistralai/mistral-small-creative',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
            http_referer='https://example.com/app',
            x_title='Kostula Naming Pipeline',
        )
        self.assertEqual(err, '')
        self.assertEqual(names, ['verodomo'])
        self.assertEqual(usage.get('cost'), 0.0123)
        self.assertEqual(captured_headers.get('http-referer'), 'https://example.com/app')
        self.assertEqual(captured_headers.get('x-title'), 'Kostula Naming Pipeline')

    def test_estimate_usage_cost_prefers_direct_cost(self) -> None:
        got = nide.estimate_usage_cost_usd(
            usage={'prompt_tokens': 1000, 'completion_tokens': 1000, 'cost': 0.0205},
            in_price_per_1k=0.0006,
            out_price_per_1k=0.0006,
        )
        self.assertAlmostEqual(got, 0.0205, places=6)

    def test_compute_dynamic_constraints_fail_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runs = Path(td)
            csv_path = runs / 'run_001.csv'
            headers = ['name', 'hard_fail', 'fail_reason', 'shortlist_selected']
            rows = [
                {'name': 'fairbill', 'hard_fail': 'true', 'fail_reason': 'quality_template_like', 'shortlist_selected': ''},
                {'name': 'faircost', 'hard_fail': 'true', 'fail_reason': 'quality_template_like', 'shortlist_selected': ''},
                {'name': 'ratefair', 'hard_fail': 'true', 'fail_reason': 'quality_template_like', 'shortlist_selected': ''},
                {'name': 'veribill', 'hard_fail': '', 'fail_reason': '', 'shortlist_selected': 'true'},
                {'name': 'dividis', 'hard_fail': '', 'fail_reason': '', 'shortlist_selected': 'true'},
            ]
            with csv_path.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)

            constraints = nide.compute_dynamic_constraints(
                runs_dir=runs,
                seen_shortlist=set(),
                window_runs=5,
                fail_threshold=0.20,
            )
            self.assertIn('quality_template_like', constraints['selected_reasons'])
            self.assertTrue(constraints['banned_prefixes'])

    def test_compute_dynamic_constraints_entropy_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runs = Path(td)
            constraints = nide.compute_dynamic_constraints(
                runs_dir=runs,
                seen_shortlist={'fairox', 'fairly', 'fairgo', 'fairza', 'fairtu', 'fairna'},
                window_runs=5,
                entropy_threshold=2.5,
            )
            self.assertIn('fai', constraints['banned_prefixes'])

    def test_compute_dynamic_constraints_pruning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runs = Path(td)
            old = runs / 'run_001_dynamic_constraints.json'
            old.write_text(
                json.dumps(
                    {
                        'banned_tokens': ['tokenaa', 'tokenbb', 'tokencc', 'tokendd', 'tokenee', 'tokenff'],
                        'banned_prefixes': ['prefaa', 'prefbb', 'prefcc', 'prefdd', 'prefee'],
                    }
                )
                + '\n',
                encoding='utf-8',
            )
            constraints = nide.compute_dynamic_constraints(
                runs_dir=runs,
                seen_shortlist=set(),
                max_token_ban=3,
                max_prefix_ban=2,
            )
            self.assertLessEqual(len(constraints['banned_tokens']), 3)
            self.assertLessEqual(len(constraints['banned_prefixes']), 2)


if __name__ == '__main__':
    unittest.main()
