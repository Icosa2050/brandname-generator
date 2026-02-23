#!/usr/bin/env python3
"""Unit tests for campaign shard scheduling helpers."""

from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import naming_campaign_runner as ncr


class NamingCampaignRunnerShardSchedulingTest(unittest.TestCase):
    def test_load_combo_duration_history_averages_ok_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'campaign_progress.csv'
            headers = [
                'source_influence_share',
                'scope',
                'gate',
                'quota_profile',
                'duration_s',
                'status',
            ]
            rows = [
                {
                    'source_influence_share': '0.25',
                    'scope': 'global',
                    'gate': 'balanced',
                    'quota_profile': 'a:1,b:1',
                    'duration_s': '100',
                    'status': 'ok',
                },
                {
                    'source_influence_share': '0.25',
                    'scope': 'global',
                    'gate': 'balanced',
                    'quota_profile': 'a:1,b:1',
                    'duration_s': '80',
                    'status': 'ok',
                },
                {
                    'source_influence_share': '0.25',
                    'scope': 'global',
                    'gate': 'balanced',
                    'quota_profile': 'a:1,b:1',
                    'duration_s': '999',
                    'status': 'generator_failed',
                },
                {
                    'source_influence_share': '0.40',
                    'scope': 'eu',
                    'gate': 'strict',
                    'quota_profile': 'a:1,b:1',
                    'duration_s': '0',
                    'status': 'ok',
                },
            ]
            with path.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)

            got = ncr.load_combo_duration_history(path)
            key = ncr.combo_history_key(
                share=0.25,
                scope='global',
                gate='balanced',
                quota_profile='a:1,b:1',
            )
            self.assertEqual(got, {key: 90.0})

    def test_assign_sweep_combos_to_shards_weighted_balances_load(self) -> None:
        combos: list[ncr.SweepCombo] = [
            (0.10, 'global', 'balanced', 'q1'),
            (0.20, 'global', 'balanced', 'q1'),
            (0.30, 'global', 'balanced', 'q1'),
            (0.40, 'global', 'balanced', 'q1'),
        ]
        history = {
            ncr.combo_history_key(share=0.10, scope='global', gate='balanced', quota_profile='q1'): 40.0,
            ncr.combo_history_key(share=0.20, scope='global', gate='balanced', quota_profile='q1'): 30.0,
            ncr.combo_history_key(share=0.30, scope='global', gate='balanced', quota_profile='q1'): 20.0,
            ncr.combo_history_key(share=0.40, scope='global', gate='balanced', quota_profile='q1'): 10.0,
        }
        assignments, meta = ncr.assign_sweep_combos_to_shards(
            sweep_combos=combos,
            shard_count=2,
            scheduling='weighted',
            history_seconds_by_combo=history,
        )
        self.assertEqual(meta.get('mode'), 'weighted')
        self.assertEqual(meta.get('history_matches'), 4)
        self.assertEqual(len(assignments), 2)
        self.assertEqual(sorted(meta.get('predicted_load_s', [])), [50.0, 50.0])
        assigned = [combo for shard in assignments for combo in shard]
        self.assertEqual(sorted(assigned), sorted(combos))

    def test_assign_sweep_combos_to_shards_falls_back_to_slice_without_history(self) -> None:
        combos: list[ncr.SweepCombo] = [
            (0.10, 'global', 'balanced', 'q1'),
            (0.20, 'global', 'strict', 'q1'),
            (0.30, 'eu', 'balanced', 'q2'),
            (0.40, 'eu', 'strict', 'q2'),
            (0.50, 'dach', 'balanced', 'q3'),
        ]
        assignments, meta = ncr.assign_sweep_combos_to_shards(
            sweep_combos=combos,
            shard_count=2,
            scheduling='weighted',
            history_seconds_by_combo={},
        )
        self.assertEqual(meta.get('mode'), 'slice_fallback_no_history')
        self.assertEqual(assignments[0], combos[0::2])
        self.assertEqual(assignments[1], combos[1::2])

    def test_extract_generator_history_skip_reads_stage_event(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'generator.log'
            events = [
                {'event': 'naming_pipeline_stage', 'stage': 'cheap_gate', 'evaluated_count': 10},
                {
                    'event': 'naming_pipeline_stage',
                    'stage': 'history_skip',
                    'skipped_count': 3,
                    'skipped_names_sample': ['verodomo', 'clarivio'],
                },
            ]
            path.write_text(
                '\n'.join(f"stage_event={json.dumps(event, ensure_ascii=False)}" for event in events) + '\n',
                encoding='utf-8',
            )
            got = ncr.extract_generator_history_skip(path)
            self.assertEqual(got.get('skipped_count'), 3)
            self.assertEqual(got.get('skipped_generated_count'), 0)
            self.assertEqual(got.get('skipped_finalist_count'), 0)
            self.assertEqual(got.get('skipped_names_sample'), ['verodomo', 'clarivio'])

    def test_extract_generator_history_skip_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            got = ncr.extract_generator_history_skip(Path(td) / 'missing.log')
        self.assertEqual(got.get('skipped_count'), 0)
        self.assertEqual(got.get('skipped_generated_count'), 0)
        self.assertEqual(got.get('skipped_finalist_count'), 0)

    def test_extract_generator_history_skip_aggregates_phased_events(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'generator.log'
            events = [
                {
                    'event': 'naming_pipeline_stage',
                    'stage': 'history_skip',
                    'phase': 'generated',
                    'skipped_count': 2,
                    'skipped_names_sample': ['tenant', 'saldo'],
                },
                {
                    'event': 'naming_pipeline_stage',
                    'stage': 'history_skip',
                    'phase': 'finalist',
                    'skipped_count': 1,
                    'skipped_names_sample': ['claritya'],
                },
            ]
            path.write_text(
                '\n'.join(f"stage_event={json.dumps(event, ensure_ascii=False)}" for event in events) + '\n',
                encoding='utf-8',
            )
            got = ncr.extract_generator_history_skip(path)
            self.assertEqual(got.get('skipped_generated_count'), 2)
            self.assertEqual(got.get('skipped_finalist_count'), 1)
            self.assertEqual(got.get('skipped_count'), 3)
            self.assertEqual(got.get('skipped_names_sample'), ['tenant', 'saldo'])


class NamingCampaignRunnerValidatorRuntimeTest(unittest.TestCase):
    def test_derive_validator_runtime_settings_clamps_values(self) -> None:
        got = ncr.derive_validator_runtime_settings(
            requested_concurrency=64,
            requested_min_concurrency=7,
            requested_max_concurrency=5,
            requested_timeout_s=0.1,
        )
        self.assertEqual(got['min_concurrency'], 7)
        self.assertEqual(got['max_concurrency'], 7)
        self.assertEqual(got['concurrency'], 7)
        self.assertEqual(got['timeout_s'], 0.5)

    def test_parse_args_accepts_validator_runtime_flags(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--validator-concurrency',
            '8',
            '--validator-min-concurrency',
            '3',
            '--validator-max-concurrency',
            '12',
            '--validator-timeout-s',
            '4.5',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        self.assertEqual(args.validator_concurrency, 8)
        self.assertEqual(args.validator_min_concurrency, 3)
        self.assertEqual(args.validator_max_concurrency, 12)
        self.assertAlmostEqual(args.validator_timeout_s, 4.5, places=6)

    def test_parse_args_accepts_openai_compat_provider_flags(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--llm-provider',
            'openai_compat',
            '--llm-openai-base-url',
            'http://localhost:11434/v1',
            '--llm-openai-api-key-env',
            'LOCAL_LLM_KEY',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        self.assertEqual(args.llm_provider, 'openai_compat')
        self.assertEqual(args.llm_openai_base_url, 'http://localhost:11434/v1')
        self.assertEqual(args.llm_openai_api_key_env, 'LOCAL_LLM_KEY')

    def test_parse_args_accepts_prompt_template_file(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--llm-prompt-template-file',
            'docs/branding/llm_prompt.utility_split_v1.txt',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        self.assertEqual(args.llm_prompt_template_file, 'docs/branding/llm_prompt.utility_split_v1.txt')

    def test_load_llm_model_config_parses_json_provider_map(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'models.json'
            path.write_text(
                json.dumps(
                    {
                        'openai_compat': ['gemma3:27b', 'qwen3-vl30b'],
                        'openrouter_http': {'models': ['mistralai/mistral-small-creative']},
                    }
                )
                + '\n',
                encoding='utf-8',
            )
            got = ncr.load_llm_model_config(str(path))
        self.assertEqual(got.get('openai_compat'), ['gemma3:27b', 'qwen3-vl30b'])
        self.assertEqual(got.get('openrouter_http'), ['mistralai/mistral-small-creative'])

    def test_load_llm_model_config_parses_txt_provider_map(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'models.txt'
            path.write_text(
                '\n'.join(
                    [
                        '# local models',
                        'openai_compat=gemma3:27b,qwen3-vl30b',
                        'openrouter_http=mistralai/mistral-small-creative,openai/gpt-4o-mini',
                    ]
                )
                + '\n',
                encoding='utf-8',
            )
            got = ncr.load_llm_model_config(str(path))
        self.assertEqual(got.get('openai_compat'), ['gemma3:27b', 'qwen3-vl30b'])
        self.assertEqual(got.get('openrouter_http'), ['mistralai/mistral-small-creative', 'openai/gpt-4o-mini'])

    def test_load_llm_model_config_parses_toml_provider_map(self) -> None:
        if ncr.tomllib is None:
            self.skipTest('tomllib unavailable on this Python version')
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'models.toml'
            path.write_text(
                '\n'.join(
                    [
                        '[providers]',
                        'openai_compat = ["gemma3:27b", "qwen3-vl30b"]',
                        'openrouter_http = ["mistralai/mistral-small-creative"]',
                    ]
                )
                + '\n',
                encoding='utf-8',
            )
            got = ncr.load_llm_model_config(str(path))
        self.assertEqual(got.get('openai_compat'), ['gemma3:27b', 'qwen3-vl30b'])
        self.assertEqual(got.get('openrouter_http'), ['mistralai/mistral-small-creative'])

    def test_resolve_llm_models_prefers_cli_models_over_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'models.json'
            path.write_text(
                json.dumps(
                    {
                        'openai_compat': ['gemma3:27b', 'qwen3-vl30b'],
                    }
                )
                + '\n',
                encoding='utf-8',
            )
            argv = [
                'naming_campaign_runner.py',
                '--llm-model-config',
                str(path),
                '--llm-models',
                'override-one,override-two',
            ]
            with mock.patch.object(sys, 'argv', argv):
                args = ncr.parse_args()
        models, source, err = ncr.resolve_llm_models(args=args, provider='openai_compat')
        self.assertEqual(models, ['override-one', 'override-two'])
        self.assertEqual(source, 'cli_models')
        self.assertEqual(err, '')

    @mock.patch('naming_campaign_runner.nide.call_openai_compat_candidates')
    @mock.patch('naming_campaign_runner.nide.list_openai_models')
    def test_run_active_llm_ideation_openai_compat_enforces_length_filter(
        self,
        mock_list_models: mock.Mock,
        mock_call_openai: mock.Mock,
    ) -> None:
        mock_list_models.return_value = {'qwen2.5:14b'}
        mock_call_openai.return_value = (
            ['short', 'verodomo', 'waytoolongcandidatehere', 'tenantia'],
            {'cost': 0.01},
            '',
        )
        argv = [
            'naming_campaign_runner.py',
            '--llm-ideation-enabled',
            '--llm-provider',
            'openai_compat',
            '--llm-model',
            'qwen2.5:14b',
            '--llm-openai-base-url',
            'http://localhost:11434/v1',
            '--llm-rounds',
            '1',
            '--llm-candidates-per-round',
            '8',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_dir = root / 'runs'
            logs_dir = root / 'logs'
            runs_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            artifact_path, report = ncr.run_active_llm_ideation(
                args=args,
                runs_dir=runs_dir,
                logs_dir=logs_dir,
                run_id='run_001_test',
                run_index=1,
                scope='global',
                seen_shortlist=set(),
                context_packet={},
            )
            self.assertEqual(report.get('status'), 'ok')
            self.assertEqual(report.get('candidate_count'), 2)
            self.assertIsNotNone(artifact_path)
            assert artifact_path is not None
            payload = json.loads(artifact_path.read_text(encoding='utf-8'))
            got = sorted(item.get('name') for item in payload.get('candidates', []))
            self.assertEqual(got, ['tenantia', 'verodomo'])

    @mock.patch('naming_campaign_runner.nide.call_openrouter_candidates')
    @mock.patch('naming_campaign_runner.nide.list_openrouter_models')
    def test_run_active_llm_ideation_openrouter_rotates_models_from_config(
        self,
        mock_list_models: mock.Mock,
        mock_call_openrouter: mock.Mock,
    ) -> None:
        model_a = 'mistralai/mistral-small-creative'
        model_b = 'openai/gpt-4o-mini'
        mock_list_models.return_value = {model_a, model_b}

        def _fake_call(*, model: str, **kwargs: object) -> tuple[list[str], dict[str, float], str]:
            del kwargs
            if model == model_a:
                return ['verodomo'], {'cost': 0.001}, ''
            if model == model_b:
                return ['tenantia'], {'cost': 0.001}, ''
            return [], {}, 'unexpected_model'

        mock_call_openrouter.side_effect = _fake_call

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = root / 'llm_models.json'
            config_path.write_text(
                json.dumps({'openrouter_http': [model_a, model_b]}) + '\n',
                encoding='utf-8',
            )
            argv = [
                'naming_campaign_runner.py',
                '--llm-ideation-enabled',
                '--llm-provider',
                'openrouter_http',
                '--llm-model-config',
                str(config_path),
                '--llm-rounds',
                '2',
                '--llm-candidates-per-round',
                '1',
                '--llm-api-key-env',
                'OPENROUTER_API_KEY',
            ]
            with mock.patch.object(sys, 'argv', argv):
                args = ncr.parse_args()

            runs_dir = root / 'runs'
            logs_dir = root / 'logs'
            runs_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            with mock.patch.dict('os.environ', {'OPENROUTER_API_KEY': 'test-key'}, clear=False):
                artifact_path, report = ncr.run_active_llm_ideation(
                    args=args,
                    runs_dir=runs_dir,
                    logs_dir=logs_dir,
                    run_id='run_002_test',
                    run_index=2,
                    scope='global',
                    seen_shortlist=set(),
                    context_packet={},
                )

            self.assertEqual(report.get('status'), 'ok')
            self.assertEqual(report.get('models_requested'), [model_a, model_b])
            self.assertEqual(report.get('models_used'), {model_a: 1, model_b: 1})
            self.assertIsNotNone(artifact_path)
            assert artifact_path is not None
            payload = json.loads(artifact_path.read_text(encoding='utf-8'))
            got = sorted(item.get('name') for item in payload.get('candidates', []))
            self.assertEqual(got, ['tenantia', 'verodomo'])
            called_models = [call.kwargs.get('model') for call in mock_call_openrouter.call_args_list]
            self.assertEqual(called_models, [model_a, model_b])


if __name__ == '__main__':
    unittest.main()
