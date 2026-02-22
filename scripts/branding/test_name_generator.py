#!/usr/bin/env python3
"""Focused regression tests for name_generator helpers."""

from __future__ import annotations

import csv
import json
import sqlite3
import subprocess
import sys
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
    def test_load_failed_history_names_detects_rejected_and_validator_hard_fail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'history.db'
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE candidates (
                      id INTEGER PRIMARY KEY,
                      name_normalized TEXT NOT NULL UNIQUE,
                      status TEXT NOT NULL,
                      state TEXT NOT NULL,
                      rejection_reason TEXT NOT NULL
                    );
                    CREATE TABLE validation_results (
                      id INTEGER PRIMARY KEY,
                      candidate_id INTEGER NOT NULL,
                      hard_fail INTEGER NOT NULL
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT INTO candidates(id, name_normalized, status, state, rejection_reason)
                    VALUES(1, 'verodomo', 'rejected', 'scored', '')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO candidates(id, name_normalized, status, state, rejection_reason)
                    VALUES(2, 'clarivio', 'checked', 'checked', '')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO candidates(id, name_normalized, status, state, rejection_reason)
                    VALUES(3, 'novanta', 'checked', 'checked', '')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO validation_results(candidate_id, hard_fail)
                    VALUES(2, 1)
                    """
                )
                conn.commit()

            got = ng.load_failed_history_names(
                db_path=db_path,
                candidate_names=['Verodomo', 'Clarivio', 'Novanta', 'Unknown'],
            )
            self.assertEqual(got, {'verodomo', 'clarivio'})

    def test_load_failed_history_names_missing_db_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            got = ng.load_failed_history_names(
                db_path=Path(td) / 'does_not_exist.db',
                candidate_names=['verodomo'],
            )
        self.assertEqual(got, set())

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

    def test_main_skips_failed_history_during_generation_phase(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'history.db'
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE candidates (
                      id INTEGER PRIMARY KEY,
                      name_normalized TEXT NOT NULL UNIQUE,
                      status TEXT NOT NULL,
                      state TEXT NOT NULL,
                      rejection_reason TEXT NOT NULL
                    );
                    CREATE TABLE validation_results (
                      id INTEGER PRIMARY KEY,
                      candidate_id INTEGER NOT NULL,
                      hard_fail INTEGER NOT NULL
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT INTO candidates(id, name_normalized, status, state, rejection_reason)
                    VALUES(1, 'tenant', 'rejected', 'scored', 'manual_test')
                    """
                )
                conn.commit()

            output_csv = Path(td) / 'out.csv'
            output_json = Path(td) / 'out.json'
            run_log = Path(td) / 'run.jsonl'

            cmd = [
                sys.executable,
                str(Path(ng.__file__).resolve()),
                '--scope=global',
                '--gate=balanced',
                '--generator-families=seed',
                '--family-quotas=seed:5',
                '--seeds=tenant,clarity',
                '--source-influence-share=0',
                '--pool-size=40',
                '--check-limit=20',
                '--shortlist-size=10',
                f'--db={db_path}',
                f'--output={output_csv}',
                f'--json-output={output_json}',
                f'--run-log={run_log}',
                '--degraded-network-mode',
                '--no-domain-check',
                '--no-store-check',
                '--no-web-check',
                '--no-package-check',
                '--no-social-check',
                '--no-progress',
            ]
            completed = subprocess.run(cmd, capture_output=True, text=True, check=True)

            generated_history_events: list[dict[str, object]] = []
            for raw in completed.stdout.splitlines():
                line = raw.strip()
                if not line.startswith('stage_event='):
                    continue
                payload = json.loads(line[len('stage_event=') :])
                if not isinstance(payload, dict):
                    continue
                if payload.get('stage') != 'history_skip':
                    continue
                if payload.get('phase') != 'generated':
                    continue
                generated_history_events.append(payload)

            self.assertTrue(generated_history_events)
            final_event = generated_history_events[-1]
            self.assertGreaterEqual(int(final_event.get('skipped_count') or 0), 1)
            self.assertIn('tenant', list(final_event.get('skipped_names_sample') or []))

            with output_csv.open('r', encoding='utf-8', newline='') as handle:
                reader = csv.DictReader(handle)
                names = {
                    (str(row.get('name_normalized') or row.get('name') or '')).strip().lower()
                    for row in reader
                }
            self.assertNotIn('tenant', names)


if __name__ == '__main__':
    unittest.main()
