#!/usr/bin/env python3
"""Fixture-driven tests for naming_ideation_stage helpers."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
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
    def test_is_valid_candidate_name_uses_6_to_14_range(self) -> None:
        self.assertFalse(nide.is_valid_candidate_name('abcde'))
        self.assertTrue(nide.is_valid_candidate_name('abcdef'))
        self.assertTrue(nide.is_valid_candidate_name('abcdefghijklmn'))
        self.assertFalse(nide.is_valid_candidate_name('abcdefghijklmno'))
        self.assertFalse(nide.is_valid_candidate_name('abc123'))

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

    def test_load_fixture_candidates_with_usage_openrouter_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'fixture_openrouter.json'
            path.write_text(
                json.dumps(
                    {
                        'choices': [
                            {'message': {'content': '{"candidates":[{"name":"Verodomo"},{"name":"Fairbill"}]}'}}
                        ],
                        'usage': {'prompt_tokens': 9, 'completion_tokens': 11, 'cost': 0.0021},
                    }
                )
                + '\n',
                encoding='utf-8',
            )
            names, usage, err = nide.load_fixture_candidates_with_usage(str(path))
            self.assertEqual(err, '')
            self.assertEqual(sorted(names), ['fairbill', 'verodomo'])
            self.assertEqual(usage.get('prompt_tokens'), 9)

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

    def test_load_prompt_template_reads_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'prompt.txt'
            path.write_text('name ideation template', encoding='utf-8')
            got = nide.load_prompt_template(str(path))
        self.assertEqual(got, 'name ideation template')

    def test_load_prompt_template_missing_file(self) -> None:
        with self.assertRaisesRegex(ValueError, 'prompt_template_not_found'):
            nide.load_prompt_template('/tmp/does-not-exist-prompt-template.txt')

    def test_build_prompt_uses_template_placeholders(self) -> None:
        template = (
            'scope={scope}\n'
            'round={round_index}\n'
            'target={target_count}\n'
            'mode={phonetic}/{morphology}/{semantic}\n'
            'banned={banned_tokens}\n'
            'prefixes={banned_prefixes}\n'
            '{context_block}'
            'json={"candidates":[{"name":"string"}]}'
        )
        prompt, _mode = nide.build_prompt(
            scope='eu',
            round_index=1,
            target_count=12,
            constraints={'banned_tokens': ['fair'], 'banned_prefixes': ['fai']},
            context_packet={'product_core': 'utility settlement'},
            prompt_template=template,
        )
        self.assertIn('scope=eu', prompt)
        self.assertIn('round=2', prompt)
        self.assertIn('target=12', prompt)
        self.assertIn('banned=fair', prompt)
        self.assertIn('prefixes=fai', prompt)
        self.assertIn('Context packet:', prompt)
        self.assertIn('json={"candidates":[{"name":"string"}]}', prompt)

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

    @mock.patch('naming_ideation_stage.request.urlopen', side_effect=TimeoutError())
    def test_call_openai_compat_candidates_timeout(self, _mock_urlopen: mock.Mock) -> None:
        names, usage, err = nide.call_openai_compat_candidates(
            api_key='k',
            base_url='http://localhost:11434/v1',
            model='qwen2.5:14b',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
        )
        self.assertEqual(names, [])
        self.assertEqual(usage, {})
        self.assertEqual(err, 'timeout')

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_call_openai_compat_candidates_respects_base_url(self, mock_urlopen: mock.Mock) -> None:
        captured: dict[str, object] = {}

        def _fake_urlopen(req: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            if hasattr(req, 'full_url'):
                captured['url'] = getattr(req, 'full_url')
            if hasattr(req, 'header_items'):
                captured['headers'] = {k.lower(): v for k, v in req.header_items()}
            payload = json.dumps(
                {
                    'choices': [{'message': {'content': '{"candidates":[{"name":"verodomo"},{"name":"short"}]}'}}],
                    'usage': {'cost': 0.0011},
                }
            )
            return _FakeHTTPResponse(payload)

        mock_urlopen.side_effect = _fake_urlopen
        names, usage, err = nide.call_openai_compat_candidates(
            api_key='ollama',
            base_url='http://localhost:11434/v1',
            model='qwen2.5:14b',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
        )
        headers = captured.get('headers') if isinstance(captured.get('headers'), dict) else {}
        self.assertEqual(err, '')
        self.assertEqual(names, ['verodomo'])
        self.assertEqual(usage.get('cost'), 0.0011)
        self.assertEqual(captured.get('url'), 'http://localhost:11434/v1/chat/completions')
        self.assertEqual(headers.get('authorization'), 'Bearer ollama')

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_call_openai_compat_candidates_includes_request_extras(self, mock_urlopen: mock.Mock) -> None:
        captured_body: dict[str, object] = {}

        def _fake_urlopen(req: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            data = getattr(req, 'data', b'') if hasattr(req, 'data') else b''
            if isinstance(data, (bytes, bytearray)):
                captured_body.update(json.loads(bytes(data).decode('utf-8')))
            payload = json.dumps(
                {
                    'choices': [{'message': {'content': '{"candidates":[{"name":"verodomo"}]}'}}],
                    'usage': {'prompt_tokens': 10, 'completion_tokens': 5},
                }
            )
            return _FakeHTTPResponse(payload)

        mock_urlopen.side_effect = _fake_urlopen
        names, _usage, err = nide.call_openai_compat_candidates(
            api_key='ollama',
            base_url='http://localhost:11434/v1',
            model='qwen2.5:14b',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
            request_extras={'ttl': 1800, 'keep_alive': '20m'},
        )
        self.assertEqual(err, '')
        self.assertEqual(names, ['verodomo'])
        self.assertEqual(captured_body.get('ttl'), 1800)
        self.assertEqual(captured_body.get('keep_alive'), '20m')

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_call_openai_compat_candidates_clamps_temperature(self, mock_urlopen: mock.Mock) -> None:
        captured_body: dict[str, object] = {}

        def _fake_urlopen(req: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            data = getattr(req, 'data', b'') if hasattr(req, 'data') else b''
            if isinstance(data, (bytes, bytearray)):
                captured_body.update(json.loads(bytes(data).decode('utf-8')))
            payload = json.dumps({'choices': [{'message': {'content': '{"candidates":[{"name":"verodomo"}]}'}}]})
            return _FakeHTTPResponse(payload)

        mock_urlopen.side_effect = _fake_urlopen
        names, _usage, err = nide.call_openai_compat_candidates(
            api_key='ollama',
            base_url='http://localhost:11434/v1',
            model='qwen2.5:14b',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
            temperature=9.9,
        )
        self.assertEqual(err, '')
        self.assertEqual(names, ['verodomo'])
        self.assertEqual(captured_body.get('temperature'), 2.0)

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_list_openai_models_reads_ids(self, mock_urlopen: mock.Mock) -> None:
        mock_urlopen.return_value = _FakeHTTPResponse(
            json.dumps({'data': [{'id': 'qwen2.5:14b'}, {'id': 'gemma3:12b'}]})
        )
        got = nide.list_openai_models(
            api_key='ollama',
            base_url='http://localhost:11434/v1',
            timeout_ms=800,
        )
        self.assertEqual(got, {'qwen2.5:14b', 'gemma3:12b'})

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_call_openrouter_candidates_fallback_after_http_404(self, mock_urlopen: mock.Mock) -> None:
        first_error = error.HTTPError(
            url='https://openrouter.ai/api/v1/chat/completions',
            code=404,
            msg='not found',
            hdrs=None,
            fp=None,
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
        try:
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
        finally:
            first_error.close()

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

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_call_openrouter_candidates_uses_configured_temperature(self, mock_urlopen: mock.Mock) -> None:
        captured_body: dict[str, object] = {}

        def _fake_urlopen(req: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            data = getattr(req, 'data', b'') if hasattr(req, 'data') else b''
            if isinstance(data, (bytes, bytearray)):
                captured_body.update(json.loads(bytes(data).decode('utf-8')))
            payload = json.dumps({'choices': [{'message': {'content': '{"candidates":[{"name":"Verodomo"}]}'}}]})
            return _FakeHTTPResponse(payload)

        mock_urlopen.side_effect = _fake_urlopen
        names, _usage, err = nide.call_openrouter_candidates(
            api_key='k',
            model='mistralai/mistral-small-creative',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
            temperature=1.1,
        )
        self.assertEqual(err, '')
        self.assertEqual(names, ['verodomo'])
        self.assertEqual(captured_body.get('temperature'), 1.1)

    @mock.patch('naming_ideation_stage.request.urlopen')
    def test_call_openrouter_candidates_normalizes_non_url_referer(self, mock_urlopen: mock.Mock) -> None:
        captured_headers: dict[str, str] = {}

        def _fake_urlopen(req: object, timeout: float) -> _FakeHTTPResponse:
            del timeout
            if hasattr(req, 'header_items'):
                captured_headers.update({k.lower(): v for k, v in req.header_items()})
            payload = json.dumps({'choices': [{'message': {'content': '{"candidates":[{"name":"Verodomo"}]}'}}]})
            return _FakeHTTPResponse(payload)

        mock_urlopen.side_effect = _fake_urlopen
        names, _usage, err = nide.call_openrouter_candidates(
            api_key='k',
            model='mistralai/mistral-small-creative',
            prompt='hello',
            timeout_ms=500,
            strict_json=True,
            http_referer='brand-name-generator',
            x_title='brand-name-generator',
        )
        self.assertEqual(err, '')
        self.assertEqual(names, ['verodomo'])
        self.assertEqual(captured_headers.get('http-referer'), 'https://brand-name-generator')
        self.assertEqual(captured_headers.get('x-title'), 'brand-name-generator')

    def test_extract_openrouter_response_content_missing_choices(self) -> None:
        content, usage, err = nide.extract_openrouter_response_content({'usage': {'cost': 0.01}})
        self.assertEqual(content, '')
        self.assertEqual(usage.get('cost'), 0.01)
        self.assertEqual(err, 'missing_choices')

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

    def test_evaluate_ideation_slo_breach(self) -> None:
        got = nide.evaluate_ideation_slo(
            attempted_rounds=5,
            successful_rounds=2,
            timeout_rounds=2,
            empty_rounds=1,
            min_success_rate=0.6,
            max_timeout_rate=0.2,
            max_empty_rate=0.1,
            min_samples=3,
        )
        self.assertEqual(got['status'], 'breach')
        self.assertIn('success_rate', got['breaches'])
        self.assertIn('timeout_rate', got['breaches'])
        self.assertIn('empty_rate', got['breaches'])


if __name__ == '__main__':
    unittest.main()
