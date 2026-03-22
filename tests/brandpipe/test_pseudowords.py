# ruff: noqa: E402
from __future__ import annotations

import sys
import types
import unittest
from unittest import mock

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.models import Brief, PseudowordConfig
from brandpipe.pseudowords import derive_seed_words, generate_pseudoword_pool, select_round_seed_names


class _FakeWuggyGenerator:
    supported_official_language_plugin_names = ["orthographic_english", "orthographic_german"]

    def __init__(self) -> None:
        self.loaded_plugin = ""
        self.download_calls: list[tuple[str, bool]] = []

    def load(self, language_plugin: str) -> None:
        self.loaded_plugin = language_plugin

    def download_language_plugin(self, language_plugin: str, auto_download: bool = False) -> None:
        self.download_calls.append((language_plugin, auto_download))

    def generate_classic(self, input_sequences, ncandidates_per_sequence=10, output_mode="plain"):  # type: ignore[no-untyped-def]
        assert input_sequences
        seed = input_sequences[0]
        return [
            {"plain": f"{seed}or"},   # ends with o -> filtered
            {"plain": f"{seed}ix"},
            {"plain": f"{seed}en"},
            {"plain": seed},          # same as source seed -> filtered
            {"plain": "bad!"},        # invalid chars -> filtered
        ]


class _PluginAwareWuggyGenerator(_FakeWuggyGenerator):
    def generate_classic(self, input_sequences, ncandidates_per_sequence=10, output_mode="plain"):  # type: ignore[no-untyped-def]
        assert input_sequences
        seed = input_sequences[0]
        ending = "ix" if self.loaded_plugin.endswith("english") else "al"
        return [
            {"plain": f"{seed}{ending}"},
            {"plain": f"{seed}en"},
        ]


class PseudowordTests(unittest.TestCase):
    def test_derive_seed_words_prefers_brief_content(self) -> None:
        brief = Brief(
            product_core="utility-cost settlement software for landlords",
            target_users=["property managers", "private landlords"],
            trust_signals=["clarity", "reliability", "legal defensibility"],
            notes="lowercase latin letters only",
        )

        seeds = derive_seed_words(brief)

        self.assertIn("landlord", seeds)
        self.assertIn("clarity", seeds)
        self.assertIn("property", seeds)
        self.assertIn("reliable", seeds)
        self.assertNotIn("utility", seeds)
        self.assertNotIn("software", seeds)

    def test_derive_seed_words_adds_lightweight_normalized_forms(self) -> None:
        brief = Brief(trust_signals=["reliability", "legal defensibility"])

        seeds = derive_seed_words(brief)

        self.assertIn("reliable", seeds)
        self.assertIn("defensible", seeds)

    def test_generate_pseudoword_pool_uses_wuggy_when_available(self) -> None:
        fake_module = types.SimpleNamespace(WuggyGenerator=_FakeWuggyGenerator)
        brief = Brief(
            product_core="tenant balance clarity ledger",
            trust_signals=["steady"],
        )

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            names, report = generate_pseudoword_pool(
                brief=brief,
                config=PseudowordConfig(language_plugin="orthographic_english", seed_count=4),
            )

        self.assertEqual(len(names), 4)
        self.assertTrue(all(6 <= len(name) <= 14 for name in names))
        self.assertTrue(all(not name.endswith("o") for name in names))
        self.assertEqual(report["engine"], "wuggy")
        self.assertEqual(report["generated_count"], 4)
        self.assertEqual(report["warning"], "")
        self.assertGreaterEqual(int(report["successful_seed_count"]), 1)

    def test_generate_pseudoword_pool_auto_downloads_missing_plugin(self) -> None:
        fake_instance = _FakeWuggyGenerator()
        fake_module = types.SimpleNamespace(WuggyGenerator=lambda: fake_instance)
        brief = Brief(product_core="tenant balance clarity ledger")

        def fake_exists(path: str) -> bool:
            return False if path.endswith("orthographic_english") else True

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            with mock.patch("brandpipe.pseudowords.inspect.getfile", return_value="/tmp/wuggy/generators/fake.py"):
                with mock.patch("brandpipe.pseudowords.os.path.exists", side_effect=fake_exists):
                    names, report = generate_pseudoword_pool(
                        brief=brief,
                        config=PseudowordConfig(language_plugin="orthographic_english", seed_count=2),
                    )

        self.assertEqual(len(names), 2)
        self.assertEqual(fake_instance.download_calls, [("orthographic_english", True)])
        self.assertTrue(report["downloaded_plugin"])

    def test_generate_pseudoword_pool_skips_missing_lexicon_seeds_when_pool_is_healthy(self) -> None:
        class _PartialWuggy(_FakeWuggyGenerator):
            def generate_classic(self, input_sequences, ncandidates_per_sequence=10, output_mode="plain"):  # type: ignore[no-untyped-def]
                seed = input_sequences[0]
                if seed == "defensible":
                    raise RuntimeError("Sequence defensible was not found in lexicon orthographic_english")
                return [{"plain": f"{seed}ix"}]

        fake_module = types.SimpleNamespace(WuggyGenerator=_PartialWuggy)
        brief = Brief(
            product_core="tenant balance clarity ledger",
            trust_signals=["reliability", "legal defensibility"],
        )

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            names, report = generate_pseudoword_pool(
                brief=brief,
                config=PseudowordConfig(language_plugin="orthographic_english", seed_count=8),
            )

        self.assertGreaterEqual(len(names), 6)
        self.assertEqual(report["warning"], "")
        self.assertGreaterEqual(int(report["successful_seed_count"]), 6)
        self.assertEqual(report["dropped_seeds"], [{"seed": "defensible", "reason": "Sequence defensible was not found in lexicon orthographic_english"}])

    def test_generate_pseudoword_pool_fails_when_output_is_too_small(self) -> None:
        class _TinyWuggy(_FakeWuggyGenerator):
            def generate_classic(self, input_sequences, ncandidates_per_sequence=10, output_mode="plain"):  # type: ignore[no-untyped-def]
                seed = input_sequences[0]
                return [{"plain": f"{seed}ix"}]

        fake_module = types.SimpleNamespace(WuggyGenerator=_TinyWuggy)
        brief = Brief(product_core="ledger clarity")

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            names, report = generate_pseudoword_pool(
                brief=brief,
                config=PseudowordConfig(language_plugin="orthographic_english", seed_count=10),
            )

        self.assertGreater(len(names), 0)
        self.assertEqual(report["warning"], "insufficient_pseudoword_yield")

    def test_generate_pseudoword_pool_merges_multiple_language_plugins(self) -> None:
        fake_module = types.SimpleNamespace(WuggyGenerator=_PluginAwareWuggyGenerator)
        brief = Brief(product_core="tenant balance clarity ledger")

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            names, report = generate_pseudoword_pool(
                brief=brief,
                config=PseudowordConfig(
                    language_plugin="orthographic_english",
                    language_plugins=("orthographic_english", "orthographic_german"),
                    seed_count=6,
                ),
            )

        self.assertGreaterEqual(len(names), 6)
        self.assertEqual(report["warning"], "")
        self.assertEqual(
            report["language_plugins"],
            ["orthographic_english", "orthographic_german"],
        )
        self.assertEqual(len(report["plugin_reports"]), 2)
        self.assertTrue(any(name.endswith("ix") for name in names))
        self.assertTrue(any(name.endswith("al") for name in names))

    def test_generate_pseudoword_pool_continues_when_one_plugin_is_unsupported(self) -> None:
        fake_module = types.SimpleNamespace(WuggyGenerator=_PluginAwareWuggyGenerator)
        brief = Brief(product_core="tenant balance clarity ledger")

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            names, report = generate_pseudoword_pool(
                brief=brief,
                config=PseudowordConfig(
                    language_plugin="orthographic_english",
                    language_plugins=("orthographic_english", "orthographic_spanish"),
                    seed_count=4,
                ),
            )

        self.assertEqual(report["warning"], "")
        self.assertGreaterEqual(len(names), 4)
        self.assertGreaterEqual(len(report["plugin_reports"]), 1)
        self.assertEqual(report["plugin_reports"][0]["language_plugin"], "orthographic_english")

    def test_select_round_seed_names_rotates_pool(self) -> None:
        pool = ["alpha", "bravo", "charly", "delta", "echo"]

        self.assertEqual(select_round_seed_names(seed_pool=pool, round_index=0, max_count=3), ["alpha", "bravo", "charly"])
        self.assertEqual(select_round_seed_names(seed_pool=pool, round_index=1, max_count=3), ["delta", "echo", "alpha"])


if __name__ == "__main__":
    unittest.main()
