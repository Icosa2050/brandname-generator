#!/usr/bin/env python3
"""Unit tests for campaign shard scheduling helpers."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
for candidate in (SCRIPT_DIR, ROOT_DIR):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

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

    def test_load_combo_duration_history_accepts_ok_degraded_status(self) -> None:
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
                    'status': 'ok_llm_degraded_empty',
                },
                {
                    'source_influence_share': '0.25',
                    'scope': 'global',
                    'gate': 'balanced',
                    'quota_profile': 'a:1,b:1',
                    'duration_s': '50',
                    'status': 'completed',
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
            self.assertEqual(got, {key: 75.0})

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

    def test_infer_combo_start_offset_returns_zero_when_progress_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            progress_csv = Path(td) / 'missing_campaign_progress.csv'
            got = ncr.infer_combo_start_offset(
                progress_csv=progress_csv,
                shard_id=0,
                shard_count=1,
            )
            self.assertEqual(got, 0)

    def test_infer_combo_start_offset_sharded_requires_matching_numeric_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            progress_csv = Path(td) / 'campaign_progress.csv'
            headers = ['run_id', 'shard_id', 'shard_count', 'status']
            rows = [
                {'run_id': '1', 'shard_id': '', 'shard_count': '', 'status': 'ok'},
                {'run_id': '2', 'shard_id': 'x', 'shard_count': 'y', 'status': 'ok'},
                {'run_id': '3', 'shard_id': '1', 'shard_count': '2', 'status': 'ok'},
                {'run_id': '4', 'shard_id': '0', 'shard_count': '2', 'status': 'ok'},
                {'run_id': '5', 'shard_id': '1', 'shard_count': '3', 'status': 'ok'},
            ]
            with progress_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)

            got = ncr.infer_combo_start_offset(
                progress_csv=progress_csv,
                shard_id=1,
                shard_count=2,
            )
            self.assertEqual(got, 1)

    def test_infer_combo_start_offset_unsharded_accepts_rows_without_shard_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            progress_csv = Path(td) / 'campaign_progress.csv'
            headers = ['run_id', 'shard_id', 'shard_count', 'status']
            rows = [
                {'run_id': '1', 'shard_id': '', 'shard_count': '', 'status': 'ok'},
                {'run_id': '2', 'shard_id': '0', 'shard_count': '1', 'status': 'ok'},
                {'run_id': '3', 'shard_id': '1', 'shard_count': '1', 'status': 'ok'},
                {'run_id': '4', 'shard_id': '0', 'shard_count': '2', 'status': 'ok'},
            ]
            with progress_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)

            got = ncr.infer_combo_start_offset(
                progress_csv=progress_csv,
                shard_id=0,
                shard_count=1,
            )
            # Rows 1 and 2 count; rows 3 and 4 declare mismatching shard metadata.
            self.assertEqual(got, 2)

    def test_default_collision_first_validator_checks_exclude_tmview_probe(self) -> None:
        self.assertNotIn('tmview_probe', ncr.DEFAULT_COLLISION_FIRST_VALIDATOR_CHECKS)
        checks = ncr.ensure_collision_first_validator_checks('')
        self.assertNotIn('tmview_probe', checks.split(','))

    def test_build_hybrid_provider_round_schedule_respects_targets(self) -> None:
        schedule = ncr.build_hybrid_provider_round_schedule(
            total_rounds=4,
            local_rounds=3,
            remote_rounds=1,
        )
        self.assertEqual(schedule, ['openai_compat', 'openrouter_http', 'openai_compat', 'openai_compat'])

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

    def test_parse_args_accepts_collision_first_flags(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--no-collision-first-mode',
            '--collision-policy-version',
            'policy_x',
            '--collision-class-profile',
            '9,42,35',
            '--collision-market-scope',
            'eu,ch,de',
            '--validator-cheap-trademark-blocklist-file',
            'resources/branding/inputs/cheap_tm_collision_blocklist_v1.txt',
            '--prefix-audit-csv',
            'test_outputs/branding/prefix_collision_audit/latest_prefix_audit.csv',
            '--prefix-audit-top-n',
            '15',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        self.assertFalse(args.collision_first_mode)
        self.assertEqual(args.collision_policy_version, 'policy_x')
        self.assertEqual(args.collision_class_profile, '9,42,35')
        self.assertEqual(args.collision_market_scope, 'eu,ch,de')
        self.assertEqual(
            args.validator_cheap_trademark_blocklist_file,
            'resources/branding/inputs/cheap_tm_collision_blocklist_v1.txt',
        )
        self.assertEqual(args.prefix_audit_csv, 'test_outputs/branding/prefix_collision_audit/latest_prefix_audit.csv')
        self.assertEqual(args.prefix_audit_top_n, 15)

    def test_ensure_collision_first_validator_checks_normalizes_to_brandpipe_surface(self) -> None:
        got = ncr.ensure_collision_first_validator_checks('web,app_store')
        checks = [part.strip() for part in got.split(',') if part.strip()]
        self.assertIn('domain', checks)
        self.assertIn('package', checks)
        self.assertIn('company', checks)
        self.assertIn('web', checks)
        self.assertIn('app_store', checks)
        self.assertIn('social', checks)
        self.assertIn('tm', checks)
        self.assertEqual(len(checks), len(set(checks)))

    def test_load_prefixes_from_audit_csv_filters_by_risk_and_pronounceability(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / 'prefix_audit.csv'
            headers = ['prefix', 'pronounce_score', 'risk_score']
            rows = [
                {'prefix': 'verano', 'pronounce_score': '100', 'risk_score': '0.0'},
                {'prefix': 'zzqpt', 'pronounce_score': '50', 'risk_score': '0.0'},
                {'prefix': 'takenx', 'pronounce_score': '95', 'risk_score': '30.0'},
                {'prefix': 'solvra', 'pronounce_score': '92', 'risk_score': '8.0'},
            ]
            with csv_path.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
            got = ncr.load_prefixes_from_audit_csv(csv_path=csv_path, top_n=10)
        self.assertEqual(got, ['verano', 'solvra'])

    def test_parse_args_accepts_openai_compat_provider_flags(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--llm-provider',
            'openai_compat',
            '--llm-openai-base-url',
            'http://localhost:11434/v1',
            '--llm-openai-api-key-env',
            'LOCAL_LLM_KEY',
            '--llm-openai-ttl-s',
            '1800',
            '--llm-openai-keep-alive',
            '30m',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        self.assertEqual(args.llm_provider, 'openai_compat')
        self.assertEqual(args.llm_openai_base_url, 'http://localhost:11434/v1')
        self.assertEqual(args.llm_openai_api_key_env, 'LOCAL_LLM_KEY')
        self.assertEqual(args.llm_openai_ttl_s, 1800)
        self.assertEqual(args.llm_openai_keep_alive, '30m')

    def test_parse_args_accepts_hybrid_provider_flags(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--llm-provider',
            'hybrid',
            '--llm-hybrid-local-share',
            '0.8',
            '--llm-hybrid-local-models',
            'qwen3-vl-30b-a3b-instruct-mlx',
            '--llm-hybrid-remote-models',
            'mistralai/mistral-small-creative',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        self.assertEqual(args.llm_provider, 'hybrid')
        self.assertAlmostEqual(args.llm_hybrid_local_share, 0.8, places=6)
        self.assertEqual(args.llm_hybrid_local_models, 'qwen3-vl-30b-a3b-instruct-mlx')
        self.assertEqual(args.llm_hybrid_remote_models, 'mistralai/mistral-small-creative')

    def test_is_remote_llm_requested_flags(self) -> None:
        self.assertFalse(
            ncr.is_remote_llm_requested(
                argparse.Namespace(
                    llm_ideation_enabled=False,
                    llm_provider='openrouter_http',
                    llm_hybrid_local_share=0.75,
                )
            )
        )
        self.assertTrue(
            ncr.is_remote_llm_requested(
                argparse.Namespace(
                    llm_ideation_enabled=True,
                    llm_provider='openrouter_http',
                    llm_hybrid_local_share=0.75,
                )
            )
        )
        self.assertFalse(
            ncr.is_remote_llm_requested(
                argparse.Namespace(
                    llm_ideation_enabled=True,
                    llm_provider='hybrid',
                    llm_hybrid_local_share=1.0,
                )
            )
        )
        self.assertTrue(
            ncr.is_remote_llm_requested(
                argparse.Namespace(
                    llm_ideation_enabled=True,
                    llm_provider='hybrid',
                    llm_hybrid_local_share=0.6,
                )
            )
        )

    def test_missing_remote_llm_api_key_env(self) -> None:
        args = argparse.Namespace(
            llm_ideation_enabled=True,
            llm_provider='hybrid',
            llm_hybrid_local_share=0.4,
            llm_api_key_env='OPENROUTER_API_KEY',
        )
        with mock.patch.dict('os.environ', {}, clear=True):
            self.assertEqual(ncr.missing_remote_llm_api_key_env(args), 'OPENROUTER_API_KEY')
        with mock.patch.dict('os.environ', {'OPENROUTER_API_KEY': 'test-key'}, clear=True):
            self.assertEqual(ncr.missing_remote_llm_api_key_env(args), '')

    def test_main_fails_fast_when_remote_key_missing(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--llm-ideation-enabled',
            '--llm-provider',
            'openrouter_http',
            '--llm-api-key-env',
            'OPENROUTER_API_KEY',
        ]
        with mock.patch.object(sys, 'argv', argv):
            with mock.patch.dict('os.environ', {}, clear=True):
                with mock.patch('builtins.print') as mock_print:
                    code = ncr.main()
        self.assertEqual(code, 1)
        printed = ' '.join(str(call.args[0]) for call in mock_print.call_args_list if call.args)
        self.assertIn('missing required environment variable OPENROUTER_API_KEY', printed)

    def test_parse_args_accepts_prompt_template_file(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--llm-prompt-template-file',
            'resources/branding/llm/llm_prompt.utility_split_v1.txt',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        self.assertEqual(args.llm_prompt_template_file, 'resources/branding/llm/llm_prompt.utility_split_v1.txt')

    def test_parse_args_accepts_source_filter_flags(self) -> None:
        argv = [
            'naming_campaign_runner.py',
            '--source-input-files',
            'resources/branding/inputs/source_inputs_core_v3.csv,resources/branding/inputs/source_inputs_expansion_v3.csv',
            '--source-exclusion-files',
            'resources/branding/inputs/source_exclusions_seed_v1.txt',
            '--source-zipf-min',
            '1.0',
            '--source-zipf-max',
            '5.8',
            '--source-zipf-language',
            'en',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        self.assertEqual(
            args.source_input_files,
            'resources/branding/inputs/source_inputs_core_v3.csv,resources/branding/inputs/source_inputs_expansion_v3.csv',
        )
        self.assertEqual(args.source_exclusion_files, 'resources/branding/inputs/source_exclusions_seed_v1.txt')
        self.assertAlmostEqual(args.source_zipf_min, 1.0, places=6)
        self.assertAlmostEqual(args.source_zipf_max, 5.8, places=6)
        self.assertEqual(args.source_zipf_language, 'en')

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
            '--llm-openai-ttl-s',
            '3600',
            '--llm-openai-keep-alive',
            '20m',
            '--llm-rounds',
            '1',
            '--llm-candidates-per-round',
            '8',
            '--llm-temperature',
            '0.65',
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
            mock_call_openai.assert_called_once()
            kwargs = mock_call_openai.call_args.kwargs
            self.assertEqual(kwargs.get('request_extras'), {'ttl': 3600, 'keep_alive': '20m'})
            self.assertAlmostEqual(float(kwargs.get('temperature') or 0.0), 0.65, places=6)

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
                '--llm-temperature',
                '0.95',
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
            called_temps = [call.kwargs.get('temperature') for call in mock_call_openrouter.call_args_list]
            self.assertEqual(called_temps, [0.95, 0.95])

    @mock.patch('naming_campaign_runner.nide.call_openrouter_candidates')
    @mock.patch('naming_campaign_runner.nide.list_openrouter_models')
    @mock.patch('naming_campaign_runner.nide.call_openai_compat_candidates')
    @mock.patch('naming_campaign_runner.nide.list_openai_models')
    def test_run_active_llm_ideation_hybrid_uses_both_providers(
        self,
        mock_list_openai_models: mock.Mock,
        mock_call_openai: mock.Mock,
        mock_list_openrouter_models: mock.Mock,
        mock_call_openrouter: mock.Mock,
    ) -> None:
        local_model = 'qwen3-vl-30b-a3b-instruct-mlx'
        remote_model = 'mistralai/mistral-small-creative'
        mock_list_openai_models.return_value = {local_model}
        mock_list_openrouter_models.return_value = {remote_model}
        mock_call_openai.return_value = (['verodomo'], {'cost': 0.001}, '')
        mock_call_openrouter.return_value = (['tenantia'], {'cost': 0.002}, '')

        argv = [
            'naming_campaign_runner.py',
            '--llm-ideation-enabled',
            '--llm-provider',
            'hybrid',
            '--llm-hybrid-local-model',
            local_model,
            '--llm-hybrid-remote-model',
            remote_model,
            '--llm-hybrid-local-share',
            '0.50',
            '--llm-rounds',
            '2',
            '--llm-candidates-per-round',
            '1',
            '--llm-api-key-env',
            'OPENROUTER_API_KEY',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_dir = root / 'runs'
            logs_dir = root / 'logs'
            runs_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            with mock.patch.dict('os.environ', {'OPENROUTER_API_KEY': 'test-key'}, clear=False):
                artifact_path, report = ncr.run_active_llm_ideation(
                    args=args,
                    runs_dir=runs_dir,
                    logs_dir=logs_dir,
                    run_id='run_003_hybrid_test',
                    run_index=3,
                    scope='global',
                    seen_shortlist=set(),
                    context_packet={},
                )

            self.assertEqual(report.get('status'), 'ok')
            self.assertEqual(report.get('candidate_count'), 2)
            self.assertEqual(
                report.get('models_used_by_provider'),
                {
                    'openai_compat': {local_model: 1},
                    'openrouter_http': {remote_model: 1},
                },
            )
            self.assertIsNotNone(artifact_path)
            assert artifact_path is not None
            payload = json.loads(artifact_path.read_text(encoding='utf-8'))
            got = sorted(item.get('name') for item in payload.get('candidates', []))
            self.assertEqual(got, ['tenantia', 'verodomo'])
            metadata = payload.get('metadata', {})
            self.assertEqual(metadata.get('provider'), 'hybrid')
            self.assertEqual(
                metadata.get('models_used_by_provider'),
                {
                    'openai_compat': {local_model: 1},
                    'openrouter_http': {remote_model: 1},
                },
            )
            mock_call_openai.assert_called_once()
            mock_call_openrouter.assert_called_once()

    @mock.patch('naming_campaign_runner.nide.call_openai_compat_candidates')
    @mock.patch('naming_campaign_runner.nide.list_openai_models')
    def test_run_active_llm_ideation_openai_compat_reports_timeout_error(
        self,
        mock_list_models: mock.Mock,
        mock_call_openai: mock.Mock,
    ) -> None:
        model = 'qwen3:latest'
        mock_list_models.return_value = {model}
        mock_call_openai.return_value = ([], {}, 'timeout')
        argv = [
            'naming_campaign_runner.py',
            '--llm-ideation-enabled',
            '--llm-provider',
            'openai_compat',
            '--llm-model',
            model,
            '--llm-rounds',
            '1',
            '--llm-candidates-per-round',
            '1',
            '--llm-max-retries',
            '0',
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
                run_id='run_timeout_test',
                run_index=1,
                scope='global',
                seen_shortlist=set(),
                context_packet={},
            )
        self.assertIsNone(artifact_path)
        self.assertEqual(report.get('status'), 'empty_with_errors')
        self.assertEqual(report.get('candidate_count'), 0)
        self.assertIn('round=1:timeout', report.get('errors', []))

    @mock.patch('naming_campaign_runner.nide.list_openrouter_models')
    @mock.patch('naming_campaign_runner.nide.list_openai_models')
    def test_run_active_llm_ideation_hybrid_unavailable_without_provider_context(
        self,
        mock_list_openai_models: mock.Mock,
        mock_list_openrouter_models: mock.Mock,
    ) -> None:
        del mock_list_openrouter_models
        mock_list_openai_models.return_value = set()
        argv = [
            'naming_campaign_runner.py',
            '--llm-ideation-enabled',
            '--llm-provider',
            'hybrid',
            '--llm-hybrid-local-model',
            'qwen3:latest',
            '--llm-hybrid-remote-model',
            'mistralai/mistral-small-creative',
            '--llm-hybrid-local-share',
            '0.5',
            '--llm-rounds',
            '2',
            '--llm-candidates-per-round',
            '1',
            '--llm-api-key-env',
            'OPENROUTER_API_KEY',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_dir = root / 'runs'
            logs_dir = root / 'logs'
            runs_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            with mock.patch.dict('os.environ', {}, clear=True):
                artifact_path, report = ncr.run_active_llm_ideation(
                    args=args,
                    runs_dir=runs_dir,
                    logs_dir=logs_dir,
                    run_id='run_hybrid_unavailable',
                    run_index=1,
                    scope='global',
                    seen_shortlist=set(),
                    context_packet={},
                )
        self.assertIsNone(artifact_path)
        self.assertEqual(report.get('status'), 'hybrid_unavailable')
        self.assertEqual(report.get('candidate_count'), 0)
        self.assertIn('openrouter_http:missing env OPENROUTER_API_KEY', report.get('errors', []))

    @mock.patch('naming_campaign_runner.nide.call_openai_compat_candidates')
    @mock.patch('naming_campaign_runner.nide.list_openrouter_models')
    @mock.patch('naming_campaign_runner.nide.list_openai_models')
    def test_run_active_llm_ideation_hybrid_local_only_still_generates_candidates(
        self,
        mock_list_openai_models: mock.Mock,
        mock_list_openrouter_models: mock.Mock,
        mock_call_openai: mock.Mock,
    ) -> None:
        del mock_list_openrouter_models
        local_model = 'qwen3:latest'
        mock_list_openai_models.return_value = {local_model}
        mock_call_openai.return_value = (['verodomo'], {'cost': 0.001}, '')
        argv = [
            'naming_campaign_runner.py',
            '--llm-ideation-enabled',
            '--llm-provider',
            'hybrid',
            '--llm-hybrid-local-model',
            local_model,
            '--llm-hybrid-remote-model',
            'mistralai/mistral-small-creative',
            '--llm-hybrid-local-share',
            '0.5',
            '--llm-rounds',
            '1',
            '--llm-candidates-per-round',
            '1',
            '--llm-api-key-env',
            'OPENROUTER_API_KEY',
        ]
        with mock.patch.object(sys, 'argv', argv):
            args = ncr.parse_args()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_dir = root / 'runs'
            logs_dir = root / 'logs'
            runs_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)
            with mock.patch.dict('os.environ', {}, clear=True):
                artifact_path, report = ncr.run_active_llm_ideation(
                    args=args,
                    runs_dir=runs_dir,
                    logs_dir=logs_dir,
                    run_id='run_hybrid_local_only',
                    run_index=1,
                    scope='global',
                    seen_shortlist=set(),
                    context_packet={},
                )
        self.assertIsNotNone(artifact_path)
        self.assertEqual(report.get('status'), 'ok')
        self.assertEqual(report.get('candidate_count'), 1)
        self.assertIn('openrouter_http:missing env OPENROUTER_API_KEY', report.get('errors', []))


if __name__ == '__main__':
    unittest.main()
