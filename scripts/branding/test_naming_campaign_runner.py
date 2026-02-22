#!/usr/bin/env python3
"""Unit tests for campaign shard scheduling helpers."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

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
            self.assertEqual(got.get('skipped_names_sample'), ['verodomo', 'clarivio'])

    def test_extract_generator_history_skip_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            got = ncr.extract_generator_history_skip(Path(td) / 'missing.log')
        self.assertEqual(got.get('skipped_count'), 0)


if __name__ == '__main__':
    unittest.main()
