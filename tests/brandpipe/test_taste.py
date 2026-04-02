# ruff: noqa: E402
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.naming_policy import build_naming_policy
from brandpipe.models import LexiconBundle, SeedCandidate
from brandpipe.taste import build_blocked_fragments, evaluate_name, filter_names, filter_seed_candidates


class TasteTests(unittest.TestCase):
    def test_evaluate_name_rejects_recent_ugly_survivors(self) -> None:
        blocked_fragments = ("parcel", "ledger", "tenant", "lease", "utility", "private")

        for raw_name in (
            "krelixen",
            "porthvenix",
            "deptrixen",
            "blentrex",
            "tenurblen",
            "beacparcel",
            "clarcivic",
            "clarven",
            "vexlaris",
            "parclen",
            "privardal",
            "preceral",
            "coraline",
            "primanel",
            "statica",
            "string",
            "render",
        ):
            decision = evaluate_name(raw_name, blocked_fragments=blocked_fragments)
            self.assertFalse(decision.accepted, raw_name)
            self.assertTrue(decision.reasons, raw_name)

    def test_evaluate_name_accepts_cleaner_shapes(self) -> None:
        blocked_fragments = ("parcel", "ledger", "tenant", "lease", "utility")

        for raw_name in ("baltera", "covendel", "solvian", "marcera"):
            decision = evaluate_name(raw_name, blocked_fragments=blocked_fragments)
            self.assertTrue(decision.accepted, raw_name)

    def test_filter_helpers_annotate_kept_items_and_drop_domain_fragment_mashups(self) -> None:
        bundle = LexiconBundle(
            core_terms=("utility", "settlement", "tenant"),
            modifiers=("clarity", "reliable"),
            avoid_terms=("rent", "ledger"),
            associative_terms=("parcel", "beacon", "anchor"),
            morphemes=("clar", "beac", "parcel"),
        )
        blocked_fragments = build_blocked_fragments(bundle)

        filtered_seeds, seed_report = filter_seed_candidates(
            [
                SeedCandidate(name="beacparcel", archetype="blend"),
                SeedCandidate(name="baltera", archetype="blend"),
            ],
            blocked_fragments=blocked_fragments,
        )
        self.assertEqual([item.name for item in filtered_seeds], ["baltera"])
        self.assertIn("direct_domain_fragment", seed_report["dropped"])
        self.assertIsInstance(filtered_seeds[0].taste_reasons, tuple)

        filtered_names, name_report = filter_names(
            ["beacparcel", "baltera"],
            blocked_fragments=blocked_fragments,
        )
        self.assertEqual(filtered_names, ["baltera"])
        self.assertIn("direct_domain_fragment", name_report["dropped"])

    def test_external_fragment_hints_block_recent_crowded_neighborhoods(self) -> None:
        blocked_fragments = build_blocked_fragments(
            None,
            extra_fragments=("samis", "parcl", "tenv"),
        )

        filtered_names, name_report = filter_names(
            ["samistra", "parclex", "baltera"],
            blocked_fragments=blocked_fragments,
        )

        self.assertEqual(filtered_names, ["baltera"])
        self.assertIn("clipped_literal_fragment", name_report["dropped"])

    def test_evaluate_name_respects_relaxed_policy_overrides(self) -> None:
        policy = build_naming_policy(
            {
                "shape": {
                    "min_length": 5,
                    "max_length": 16,
                    "reject_repeated_char_run": False,
                },
                "taste": {
                    "banned_morphemes": [],
                    "generic_safe_openings": [],
                    "exact_generic_words": [],
                    "reject_codes": [],
                    "min_vowel_ratio": 0.0,
                    "min_open_syllable_ratio": 0.0,
                    "reject_penalty_threshold": 99.0,
                },
            }
        )

        decision = evaluate_name("prexa", blocked_fragments=(), policy=policy)

        self.assertTrue(decision.accepted)


if __name__ == "__main__":
    unittest.main()
