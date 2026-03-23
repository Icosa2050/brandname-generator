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
from argparse import Namespace
from pathlib import Path
from unittest import mock

import name_generator as ng


class _PassthroughFilter:
    filter_id = 'passthrough'

    def apply(self, request: ng.FilterRequest) -> list[ng.GeneratedCandidate]:
        return list(request.generated)


class NameGeneratorTest(unittest.TestCase):
    def _default_args(self) -> Namespace:
        with mock.patch.object(sys, 'argv', ['name_generator.py']):
            return ng.parse_args()

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

    def test_trademark_search_urls_uses_filtered_tmview_results_url(self) -> None:
        _dpma, _swissreg, tmview = ng.trademark_search_urls('siglumen')
        self.assertIn('https://www.tmdn.org/tmview/#/tmview/results?', tmview)
        self.assertIn('basicSearch=%20siglumen', tmview)
        self.assertIn('niceClass=9,OR,42,OR,EMPTY', tmview)
        self.assertIn('tmStatus=Filed,Registered', tmview)

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

    def test_lazy_category_template_flag_is_hard_fail_even_without_quality_first(self) -> None:
        item = ng.GeneratedCandidate(name='tenantlo', generator_family='seed', lineage_atoms=['tenant', 'lo'])
        with (
            mock.patch('name_generator.challenge_risk', return_value=(12, 9, 'objego', 0, 0)),
            mock.patch('name_generator.adversarial_similarity_signal', return_value=(0, '')),
            mock.patch('name_generator.gibberish_signal', return_value=(0, '')),
            mock.patch('name_generator.false_friend_signal', return_value=(0, '')),
            mock.patch('name_generator.template_likeness_signal', return_value=(30, 'lazy_category_suffix')),
            mock.patch('name_generator.psych_spelling_risk', return_value=3),
            mock.patch('name_generator.psych_trust_proxy_score', return_value=78),
        ):
            got = ng.evaluate_candidates(
                scope='global',
                generated_items=[item],
                similarity_fail_threshold=95,
                false_friend_fail_threshold=95,
                gibberish_fail_threshold=95,
                false_friend_rules={},
                quality_first=False,
            )[0]
        self.assertTrue(got.hard_fail)
        self.assertEqual(got.fail_reason, 'lazy_category_suffix')

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
            max_per_primary_atom=99,
            filter_engine=_PassthroughFilter(),
        )
        self.assertLessEqual(len(generated), 3)
        self.assertTrue(all(item.generator_family == 'seed' for item in generated))
        self.assertIn('clarity', {item.name for item in generated})

    def test_generate_candidates_lattice_family_surfaces_new_structures(self) -> None:
        source_atoms = [
            {'atom_display': 'clarity', 'confidence_weight': 0.82},
            {'atom_display': 'beacon', 'confidence_weight': 0.78},
            {'atom_display': 'serein', 'confidence_weight': 0.75},
            {'atom_display': 'meridi', 'confidence_weight': 0.72},
        ]
        generated = ng.generate_candidates(
            scope='global',
            seeds=[],
            min_len=6,
            max_len=14,
            variation_profile='expanded',
            generator_families=['lattice'],
            family_quotas={'lattice': 18},
            source_atoms=source_atoms,
            source_influence_share=0.35,
            max_per_prefix2=99,
            max_per_suffix2=99,
            max_per_shape=99,
            max_per_family=99,
            max_per_primary_atom=99,
            filter_engine=_PassthroughFilter(),
        )
        self.assertTrue(generated)
        self.assertTrue(all(item.generator_family == 'lattice' for item in generated))
        self.assertTrue(any(len(item.lineage_atoms) >= 3 for item in generated))
        self.assertGreaterEqual(len({item.name[:3] for item in generated}), 3)

    def test_diversity_filter_limits_primary_atom_repetition(self) -> None:
        generated = [
            ng.GeneratedCandidate(name='vantaten', generator_family='coined', lineage_atoms=['vanta', 'ten'], source_confidence=0.9),
            ng.GeneratedCandidate(name='vantabal', generator_family='coined', lineage_atoms=['vanta', 'bal'], source_confidence=0.8),
            ng.GeneratedCandidate(name='vantaro', generator_family='coined', lineage_atoms=['vanta', 'ro'], source_confidence=0.7),
            ng.GeneratedCandidate(name='solvara', generator_family='coined', lineage_atoms=['solva', 'ra'], source_confidence=0.6),
        ]

        out = ng.diversity_filter(
            generated,
            max_per_prefix2=99,
            max_per_suffix2=99,
            max_per_shape=99,
            max_per_family=99,
            max_per_primary_atom=2,
        )

        self.assertEqual([item.name for item in out], ['vantaten', 'vantabal', 'solvara'])

    def test_rerank_with_diversity_limits_primary_atom_repetition(self) -> None:
        candidates = [
            ng.Candidate(name='vantaten', generator_family='coined', lineage_atoms='vanta;ten', source_confidence=0.9, quality_score=80, challenge_risk=10, total_score=90, descriptive_risk=0, similarity_risk=0, closest_mark='', scope_penalty=0),
            ng.Candidate(name='vantabal', generator_family='coined', lineage_atoms='vanta;bal', source_confidence=0.8, quality_score=79, challenge_risk=11, total_score=89, descriptive_risk=0, similarity_risk=0, closest_mark='', scope_penalty=0),
            ng.Candidate(name='solvara', generator_family='coined', lineage_atoms='solva;ra', source_confidence=0.7, quality_score=78, challenge_risk=12, total_score=88, descriptive_risk=0, similarity_risk=0, closest_mark='', scope_penalty=0),
        ]

        ranked = ng.rerank_with_diversity(
            candidates,
            gate='strict',
            shortlist_size=3,
            max_per_bucket=3,
            max_per_prefix3=3,
            max_per_phonetic=3,
            max_per_primary_atom=1,
            max_per_seed_base=1,
        )

        selected = [item.name for item in ranked if item.shortlist_selected]
        deferred = {item.name: item.shortlist_reason for item in ranked if not item.shortlist_selected}
        self.assertEqual(selected, ['vantaten', 'solvara'])
        self.assertEqual(deferred['vantabal'], 'primary_atom_quota_reached:vanta')

    def test_rerank_with_diversity_limits_seed_base_variants(self) -> None:
        candidates = [
            ng.Candidate(name='clarity', generator_family='seed', lineage_atoms='clarity', source_confidence=0.9, quality_score=80, challenge_risk=10, total_score=90, descriptive_risk=0, similarity_risk=0, closest_mark='', scope_penalty=0),
            ng.Candidate(name='clarityon', generator_family='seed', lineage_atoms='clarity;on', source_confidence=0.8, quality_score=79, challenge_risk=11, total_score=89, descriptive_risk=0, similarity_risk=0, closest_mark='', scope_penalty=0),
            ng.Candidate(name='solvara', generator_family='coined', lineage_atoms='solva;ra', source_confidence=0.7, quality_score=78, challenge_risk=12, total_score=88, descriptive_risk=0, similarity_risk=0, closest_mark='', scope_penalty=0),
        ]

        ranked = ng.rerank_with_diversity(
            candidates,
            gate='strict',
            shortlist_size=3,
            max_per_bucket=3,
            max_per_prefix3=3,
            max_per_phonetic=3,
            max_per_primary_atom=3,
            max_per_seed_base=1,
        )

        selected = [item.name for item in ranked if item.shortlist_selected]
        deferred = {item.name: item.shortlist_reason for item in ranked if not item.shortlist_selected}
        self.assertEqual(selected, ['clarity', 'solvara'])
        self.assertEqual(deferred['clarityon'], 'seed_base_quota_reached:clarity')

    def test_web_collision_signal_ignores_social_handle_exact_hits(self) -> None:
        quoted = [
            ('https://www.tiktok.com/@billevis', 'BiL LeviS (@billevis) | TikTok'),
        ]
        plain = [
            ('https://www.instagram.com/billevis/', 'Tina Christian (@billevis) • Instagram photos and videos'),
            ('https://www.facebook.com/some.user', 'Billevi Halaluva - Facebook'),
        ]
        with mock.patch(
            'name_generator.fetch_search_matches',
            side_effect=[(quoted, True, 'ddg'), (plain, True, 'ddg')],
        ):
            exact, near, total, sample_domains, ok, source = ng.web_collision_signal('billevis', top_n=8)
        self.assertTrue(ok)
        self.assertEqual(source, 'ddg+ddg')
        self.assertEqual(exact, 0)
        self.assertEqual(near, 0)
        self.assertEqual(total, 3)
        self.assertIn('instagram.com', sample_domains)

    def test_web_collision_signal_counts_exact_domain_label_match(self) -> None:
        quoted: list[tuple[str, str]] = []
        plain = [
            ('https://ratefixe.ro/servicii', 'Rate Fixe - Credite rapide'),
            ('https://example.com', 'Unrelated title'),
        ]
        with mock.patch(
            'name_generator.fetch_search_matches',
            side_effect=[(quoted, True, 'ddg'), (plain, True, 'ddg')],
        ):
            exact, near, total, sample_domains, ok, source = ng.web_collision_signal('ratefixe', top_n=8)
        self.assertTrue(ok)
        self.assertEqual(source, 'ddg+ddg')
        self.assertEqual(exact, 1)
        self.assertEqual(near, 0)
        self.assertEqual(total, 2)
        self.assertIn('ratefixe.ro', sample_domains)

    def test_fetch_search_matches_prefers_serpapi(self) -> None:
        with (
            mock.patch('name_generator.fetch_serpapi_matches', return_value=([('https://example.com', 'Example')], True, 'serpapi')),
            mock.patch('name_generator.fetch_text') as mock_fetch_text,
        ):
            rows, ok, source = ng.fetch_search_matches('freshpass')
        self.assertTrue(ok)
        self.assertEqual(source, 'serpapi')
        self.assertEqual(rows, [('https://example.com', 'Example')])
        mock_fetch_text.assert_not_called()

    def test_rebalance_family_quotas_for_source_influence(self) -> None:
        out = ng.rebalance_family_quotas_for_source_influence(
            active_families=['coined', 'source_pool', 'blend', 'lattice'],
            family_quotas={'coined': 200, 'source_pool': 200, 'blend': 200, 'lattice': 200},
            source_influence_share=0.20,
        )
        self.assertEqual(out['coined'], 200)
        self.assertLess(out['source_pool'], 200)
        self.assertLess(out['blend'], 200)
        self.assertLess(out['lattice'], 200)

    def test_template_likeness_signal_penalizes_lazy_category_suffix(self) -> None:
        penalty, flags = ng.template_likeness_signal('tenantlo')
        self.assertGreaterEqual(penalty, 24)
        self.assertIn('lazy_category_suffix', flags)

    def test_template_likeness_signal_does_not_flag_unrelated_name(self) -> None:
        penalty, flags = ng.template_likeness_signal('clarodus')
        self.assertLess(penalty, 16)
        self.assertNotIn('lazy_category_suffix', flags)

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

    def test_persist_to_db_records_atom_budget_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'naming.db'
            args = self._default_args()
            args.seeds = 'balance,clarity'
            args.max_per_primary_atom = 5
            args.shortlist_max_primary_atom = 1
            args.shortlist_max_seed_base = 1

            candidate = ng.Candidate(
                name='solvara',
                generator_family='coined',
                lineage_atoms='solva;ra',
                source_confidence=0.7,
                quality_score=78,
                challenge_risk=12,
                total_score=88,
                descriptive_risk=0,
                similarity_risk=0,
                closest_mark='',
                scope_penalty=0,
                shortlist_selected=True,
                shortlist_rank=1,
                shortlist_bucket='brandable',
                shortlist_reason='diversity_accept',
            )

            run_id, _ = ng.persist_to_db(
                db_path=db_path,
                scope='global',
                gate='strict',
                variation_profile='expanded',
                args=args,
                candidates=[candidate],
            )

            with sqlite3.connect(db_path) as conn:
                row = conn.execute('SELECT config_json FROM naming_runs WHERE id = ?', (run_id,)).fetchone()

            self.assertIsNotNone(row)
            config = json.loads(str(row[0]))
            self.assertEqual(config['max_per_primary_atom'], 5)
            self.assertEqual(config['shortlist_max_primary_atom'], 1)
            self.assertEqual(config['shortlist_max_seed_base'], 1)


if __name__ == '__main__':
    unittest.main()
