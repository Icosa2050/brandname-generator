# ruff: noqa: E402
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.diversity import (
    filter_names,
    filter_local_collisions,
    filter_seed_candidates,
    phonetic_key,
    root_key,
    salvage_names,
)
from brandpipe.models import SeedCandidate
from brandpipe.naming_policy import build_naming_policy


class DiversityTests(unittest.TestCase):
    def test_root_and_phonetic_keys_collapse_simple_families(self) -> None:
        self.assertEqual(root_key("clarify"), root_key("clarily"))
        self.assertEqual(phonetic_key("lift"), phonetic_key("lyft"))

    def test_filter_names_drops_close_variants_and_avoid_terms(self) -> None:
        names, report = filter_names(
            ["clarify", "clarily", "lyft", "lift", "immotion", "beacon"],
            avoid_terms=("immo",),
            saturation_limit=1,
        )

        self.assertEqual(names, ["clarify", "lyft", "beacon"])
        self.assertIn("suffix_family", report["dropped"])
        self.assertIn("phonetic_duplicate", report["dropped"])
        self.assertIn("avoid_term", report["dropped"])
        self.assertEqual(report["input_count"], 6)
        self.assertLess(float(report["compression_ratio"]), 1.0)

    def test_filter_seed_candidates_keeps_only_diverse_items(self) -> None:
        candidates = [
            SeedCandidate(name="clarify", archetype="compound"),
            SeedCandidate(name="clarily", archetype="blend"),
            SeedCandidate(name="beacon", archetype="coined"),
        ]

        kept, report = filter_seed_candidates(
            candidates,
            avoid_terms=("immo",),
            saturation_limit=1,
        )

        self.assertEqual([item.name for item in kept], ["clarify", "beacon"])
        self.assertEqual(report["kept"], 2)
        self.assertEqual(report["input_count"], 3)
        self.assertGreaterEqual(report["unique_root_count"], 2)

    def test_filter_names_supports_lead_fragment_and_skeleton_quotas(self) -> None:
        names, report = filter_names(
            ["lattanel", "lattimer", "mekitor", "moketar", "bekitor"],
            avoid_terms=(),
            saturation_limit=2,
            lead_fragment_limit=1,
            lead_fragment_length=4,
            lead_skeleton_limit=1,
        )

        self.assertEqual(names, ["lattanel", "mekitor", "bekitor"])
        self.assertIn("lead_fragment_quota", report["dropped"])
        self.assertIn("lead_skeleton_quota", report["dropped"])
        self.assertEqual(report["lead_fragment_limit"], 1)
        self.assertEqual(report["lead_skeleton_limit"], 1)

    def test_salvage_names_keeps_small_exact_deduped_non_avoid_set(self) -> None:
        names, report = salvage_names(
            ["clarify", "clarify", "immotion", "beacon", "lift"],
            avoid_terms=("immo",),
            limit=2,
        )

        self.assertEqual(names, ["clarify", "beacon"])
        self.assertEqual(report["mode"], "salvage_exact_only")
        self.assertEqual(report["kept"], 2)
        self.assertIn("exact_duplicate", report["dropped"])
        self.assertIn("avoid_term", report["dropped"])

    def test_filter_local_collisions_drops_phonetic_trigram_and_terminal_repetition(self) -> None:
        names, report = filter_local_collisions(
            ["pryndx", "prynde", "varkten", "quintlex", "flendex"],
            recent_corpus=[
                {"name": "pryndex", "decision": "blocked"},
                {"name": "varkton", "decision": "blocked"},
            ],
            terminal_bigram_quota=1,
            trigram_threshold=0.55,
        )

        self.assertEqual(names, ["quintlex"])
        self.assertIn("phonetic_corpus_collision", report["dropped"])
        self.assertIn("trigram_corpus_collision", report["dropped"])
        self.assertIn("terminal_quota", report["dropped"])
        self.assertEqual(report["corpus_size"], 2)

    def test_filter_local_collisions_salvages_lowest_collision_name_when_all_dropped(self) -> None:
        names, report = filter_local_collisions(
            ["quintlex", "flendex"],
            recent_corpus=[
                {"name": "quintlez", "decision": "blocked"},
                {"name": "flendax", "decision": "blocked"},
            ],
            terminal_bigram_quota=1,
            trigram_threshold=0.55,
        )

        self.assertEqual(len(names), 1)
        self.assertTrue(report["relaxed"])
        self.assertEqual(report["salvage"]["mode"], "retain_lowest_local_collision")

    def test_filter_local_collisions_drops_crowded_terminal_skeletons(self) -> None:
        names, report = filter_local_collisions(
            ["candix", "veldix", "planchiv"],
            recent_corpus=[],
            crowded_terminal_skeletons=("dx",),
        )

        self.assertEqual(names, ["planchiv"])
        self.assertIn("crowded_terminal_skeleton", report["dropped"])
        self.assertEqual(report["crowded_terminal_skeletons"], ["dx"])

    def test_filter_local_collisions_drops_crowded_terminal_families(self) -> None:
        names, report = filter_local_collisions(
            ["serevela", "hathera", "latimen"],
            recent_corpus=[],
            crowded_terminal_families=("la",),
        )

        self.assertEqual(names, ["hathera", "latimen"])
        self.assertIn("crowded_terminal_family", report["dropped"])
        self.assertEqual(report["crowded_terminal_families"], ["la"])

    def test_filter_local_collisions_drops_reason_specific_lead_and_tail_hints(self) -> None:
        names, report = filter_local_collisions(
            ["serevala", "housan", "meridel"],
            recent_corpus=[],
            avoid_lead_fragments=("serev",),
            avoid_tail_fragments=("san",),
        )

        self.assertEqual(names, ["meridel"])
        self.assertIn("lead_fragment_collision", report["dropped"])
        self.assertIn("tail_fragment_collision", report["dropped"])
        self.assertEqual(report["avoid_lead_fragments"], ["serev"])
        self.assertEqual(report["avoid_tail_fragments"], ["san"])

    def test_filter_local_collisions_drops_reason_specific_lead_skeletons(self) -> None:
        names, report = filter_local_collisions(
            ["lumenda", "meridel"],
            recent_corpus=[],
            avoid_lead_skeletons=("lmn",),
        )

        self.assertEqual(names, ["meridel"])
        self.assertIn("lead_skeleton_collision", report["dropped"])
        self.assertEqual(report["avoid_lead_skeletons"], ["lmn"])

    def test_filter_local_collisions_uses_policy_default_thresholds(self) -> None:
        policy = build_naming_policy(
            {
                "local_collision": {
                    "terminal_bigram_quota": 3,
                    "trigram_threshold": 0.95,
                    "salvage_keep_count": 2,
                }
            }
        )

        names, report = filter_local_collisions(
            ["flendex", "trandex"],
            recent_corpus=[],
            policy=policy,
        )

        self.assertEqual(len(names), 2)
        self.assertEqual(report["terminal_bigram_quota"], 3)
        self.assertEqual(report["salvage_keep_count"], 2)


if __name__ == "__main__":
    unittest.main()
