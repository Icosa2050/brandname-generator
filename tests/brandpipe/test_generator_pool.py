# ruff: noqa: E402
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.generator_pool import generate_seed_pool, select_round_seed_candidates
from brandpipe.models import LexiconBundle


class GeneratorPoolTests(unittest.TestCase):
    def test_generate_seed_pool_returns_multiple_archetypes(self) -> None:
        bundle = LexiconBundle(
            core_terms=("property", "settlement", "tenant"),
            modifiers=("clarity", "reliability", "private"),
            avoid_terms=("immo", "rent"),
            associative_terms=("anchor", "beacon", "parcel", "keystone", "signal"),
            morphemes=("anchor", "beacon", "parcel", "signal", "tenant", "clar", "steady"),
            language_bias="neutral",
        )

        seeds, report = generate_seed_pool(
            bundle,
            pseudowords=["zelnar", "trivex", "mirest", "cavlin"],
            total_limit=24,
        )

        self.assertTrue(seeds)
        archetypes = {seed.archetype for seed in seeds}
        self.assertIn("transmute", archetypes)
        self.assertIn("compound", archetypes)
        self.assertGreaterEqual(len(archetypes), 3)
        self.assertLessEqual(len(seeds), 24)
        self.assertEqual(report["total"], len(seeds))
        self.assertIn("diversity_score", report)
        self.assertGreaterEqual(float(report["source_score_avg"]), 0.0)

    def test_generate_seed_pool_can_surface_blends_for_cleaner_bundle(self) -> None:
        bundle = LexiconBundle(
            core_terms=("solace", "lumen", "harbor"),
            modifiers=("steady", "candid", "serein"),
            avoid_terms=("rent",),
            associative_terms=("auren", "marin", "talen", "brisa"),
            morphemes=("sola", "lume", "hara", "sere", "mari", "tale"),
            language_bias="neutral",
        )

        seeds, _report = generate_seed_pool(
            bundle,
            pseudowords=["zelnar", "mirest", "cavlin"],
            total_limit=24,
        )

        archetypes = {seed.archetype for seed in seeds}
        self.assertIn("blend", archetypes)

    def test_generate_seed_pool_filters_ugly_suffix_families(self) -> None:
        bundle = LexiconBundle(
            core_terms=("property", "settlement", "tenant"),
            modifiers=("clarity", "reliability", "private"),
            avoid_terms=("immo", "rent"),
            associative_terms=("anchor", "beacon", "parcel", "keystone", "signal"),
            morphemes=("anchor", "beacon", "parcel", "signal", "tenant", "clar", "steady"),
            language_bias="neutral",
        )

        seeds, report = generate_seed_pool(
            bundle,
            pseudowords=["krelixen", "deptrixen", "blentrex", "clarien", "covendel"],
            total_limit=24,
        )

        names = {seed.name for seed in seeds}
        self.assertNotIn("krelixen", names)
        self.assertNotIn("deptrixen", names)
        self.assertNotIn("blentrex", names)
        self.assertIn("taste_filter", report)

    def test_generate_seed_pool_rejects_harsh_source_units(self) -> None:
        bundle = LexiconBundle(
            core_terms=("property", "settlement"),
            modifiers=("clarity", "quickzone"),
            avoid_terms=("rent",),
            associative_terms=("quartz", "zendel", "harbor", "signal"),
            morphemes=("quar", "zend", "hara", "sign"),
            language_bias="neutral",
        )

        seeds, _report = generate_seed_pool(
            bundle,
            pseudowords=["quenar", "zendal", "harena", "solarin"],
            total_limit=24,
        )

        names = {seed.name for seed in seeds}
        self.assertNotIn("quenar", names)
        self.assertNotIn("zendal", names)
        self.assertTrue(any(name in names for name in {"haraan", "harbora", "signala"}))

    def test_select_round_seed_candidates_round_robins_archetypes(self) -> None:
        bundle = LexiconBundle(
            core_terms=("property", "settlement", "tenant"),
            modifiers=("clarity", "reliability", "private"),
            avoid_terms=("immo", "rent"),
            associative_terms=("anchor", "beacon", "parcel", "keystone", "signal"),
            morphemes=("anchor", "beacon", "parcel", "signal", "tenant", "clar", "steady"),
            language_bias="neutral",
        )
        seeds, _report = generate_seed_pool(
            bundle,
            pseudowords=["zelnar", "trivex", "mirest", "cavlin", "bralen", "tarken"],
            total_limit=24,
        )

        selected = select_round_seed_candidates(seed_pool=seeds, round_index=0, max_count=4)

        self.assertEqual(len(selected), 4)
        self.assertEqual(len({item.archetype for item in selected}), 4)

    def test_generate_seed_pool_demotes_crowded_terminal_families(self) -> None:
        bundle = LexiconBundle(
            core_terms=("solace", "lumen", "harbor"),
            modifiers=("steady", "candid", "serein"),
            avoid_terms=("rent",),
            associative_terms=("auren", "marin", "talen", "brisa"),
            morphemes=("sola", "lume", "hara", "sere", "mari", "tale"),
            language_bias="neutral",
        )

        seeds, report = generate_seed_pool(
            bundle,
            pseudowords=["serevala", "harelan", "lumanel", "marvela"],
            total_limit=16,
            crowded_terminal_families=("la",),
        )

        self.assertEqual(report["crowded_terminal_families"], ["la"])
        self.assertTrue(seeds)
        self.assertNotEqual(seeds[0].name[-2:], "la")

    def test_generate_seed_pool_uses_external_avoid_names_in_seed_diversity(self) -> None:
        bundle = LexiconBundle(
            core_terms=("solace", "lumen", "harbor"),
            modifiers=("steady", "candid", "serein"),
            avoid_terms=("rent",),
            associative_terms=("auren", "marin", "talen", "brisa"),
            morphemes=("sola", "lume", "hara", "sere", "mari", "tale"),
            language_bias="neutral",
        )

        seeds, report = generate_seed_pool(
            bundle,
            pseudowords=["meridel", "lumanel", "harbela"],
            total_limit=16,
            avoid_terms_extra=("meridel",),
        )

        self.assertIn("meridel", report["avoid_terms_extra"])
        self.assertTrue(all(seed.name != "meridel" for seed in seeds))


if __name__ == "__main__":
    unittest.main()
