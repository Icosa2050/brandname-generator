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

import brandpipe.pseudowords as MODULE
from brandpipe.models import Brief, LexiconBundle, PseudowordConfig
from brandpipe.pseudowords import (
    _blocked_lexical_fragments,
    _extract_plain_candidate,
    _generate_rare_pronounceable_pool,
    _generate_wuggy_pseudowords,
    _is_low_collision_shape,
    _plugin_target_counts,
    _seed_forms,
    derive_seed_words,
    derive_seed_words_from_lexicon,
    generate_pseudoword_pool,
    select_round_seed_names,
)


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

    def test_lexicon_seed_helpers_and_candidate_extraction_cover_normalization_edges(self) -> None:
        bundle = LexiconBundle(
            core_terms=("stories", "reliability"),
            modifiers=("flexibility", "ledgers"),
            associative_terms=("stories", "about"),
        )

        seeds = derive_seed_words_from_lexicon(bundle)

        self.assertIn("story", seeds)
        self.assertIn("reliable", seeds)
        self.assertIn("flexible", seeds)
        self.assertIn("ledger", seeds)
        self.assertNotIn("about", seeds)
        self.assertEqual(_seed_forms("trust"), [])
        self.assertEqual(_extract_plain_candidate("zivora"), "zivora")
        self.assertEqual(_extract_plain_candidate({"plain": 3, "pseudoword": "valtor"}), "valtor")
        self.assertIn("fallback", _extract_plain_candidate({"other": "fallback"}))

    def test_seed_round_and_plugin_count_helpers_cover_empty_inputs(self) -> None:
        self.assertEqual(select_round_seed_names(seed_pool=[], round_index=3, max_count=2), [])
        self.assertEqual(select_round_seed_names(seed_pool=["alpha"], round_index=0, max_count=0), [])
        self.assertEqual(_plugin_target_counts(5, 0), [])

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

    def test_generate_pseudoword_pool_adds_low_collision_phase1_names(self) -> None:
        fake_module = types.SimpleNamespace(WuggyGenerator=_FakeWuggyGenerator)
        brief = Brief(product_core="tenant balance clarity ledger")

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            names, report = generate_pseudoword_pool(
                brief=brief,
                config=PseudowordConfig(
                    language_plugin="orthographic_english",
                    seed_count=4,
                    rare_seed_count=8,
                    rare_profile="aggressive",
                ),
            )

        self.assertGreaterEqual(len(names), 8)
        self.assertIn("rare_pronounceable", report["engines"])
        self.assertEqual(report["rare_pronounceable"]["profile"], "aggressive")
        self.assertGreaterEqual(int(report["rare_pronounceable"]["generated_count"]), 4)
        self.assertTrue(any(name[:2] in {"zk", "zv", "vr", "kv", "tv", "zl", "xr", "xl"} for name in names))

    def test_generate_pseudoword_pool_rare_phase_can_cover_small_wuggy_yield(self) -> None:
        class _TinyWuggy(_FakeWuggyGenerator):
            def generate_classic(self, input_sequences, ncandidates_per_sequence=10, output_mode="plain"):  # type: ignore[no-untyped-def]
                seed = input_sequences[0]
                return [{"plain": f"{seed}ix"}]

        fake_module = types.SimpleNamespace(WuggyGenerator=_TinyWuggy)
        brief = Brief(product_core="tenant balance clarity ledger")

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            names, report = generate_pseudoword_pool(
                brief=brief,
                config=PseudowordConfig(
                    language_plugin="orthographic_english",
                    seed_count=10,
                    rare_seed_count=8,
                    rare_profile="aggressive",
                ),
            )

        self.assertGreaterEqual(len(names), 10)
        self.assertEqual(report["warning"], "")
        self.assertGreaterEqual(int(report["rare_pronounceable"]["generated_count"]), 4)

    def test_blocked_fragment_and_low_collision_helpers_cover_rejection_paths(self) -> None:
        brief = Brief(
            product_core="tenant signal ledger",
            forbidden_directions=["clarity"],
            notes="about",
        )
        lexicon = LexiconBundle(
            core_terms=("signal",),
            modifiers=("about",),
            avoid_terms=("trust",),
            associative_terms=("clarity",),
        )

        blocked = _blocked_lexical_fragments(brief=brief, lexicon=lexicon, seed_words=["settlor"])

        self.assertIn("signal", blocked)
        self.assertIn("clarity", blocked)
        self.assertIn("settlor", blocked)
        self.assertNotIn("about", blocked)
        self.assertFalse(
            _is_low_collision_shape("bad", blocked_fragments=(), initial_clusters=("zv",), internal_clusters=("vr",))
        )
        self.assertFalse(
            _is_low_collision_shape("aaaezx", blocked_fragments=(), initial_clusters=("zv",), internal_clusters=("vr",))
        )
        self.assertFalse(
            _is_low_collision_shape("abclmn", blocked_fragments=(), initial_clusters=("zv",), internal_clusters=("vr",))
        )
        self.assertFalse(
            _is_low_collision_shape("stralp", blocked_fragments=(), initial_clusters=("zv",), internal_clusters=("vr",))
        )
        self.assertFalse(
            _is_low_collision_shape("tanaka", blocked_fragments=(), initial_clusters=("zv",), internal_clusters=("vr",))
        )
        self.assertFalse(
            _is_low_collision_shape("valeria", blocked_fragments=(), initial_clusters=("zv",), internal_clusters=("vr",))
        )

    def test_rare_generator_reports_unsupported_profiles_and_empty_output(self) -> None:
        names, report = _generate_rare_pronounceable_pool(
            brief=Brief(product_core="tenant balance clarity ledger"),
            config=PseudowordConfig(rare_seed_count=2, rare_profile="mystery"),
            lexicon=None,
            seed_words=["ledger"],
            seen=set(),
        )

        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "unsupported_rare_profile")

        with mock.patch("brandpipe.pseudowords._is_low_collision_shape", return_value=False):
            names, report = _generate_rare_pronounceable_pool(
                brief=Brief(product_core="tenant balance clarity ledger"),
                config=PseudowordConfig(rare_seed_count=2, rare_profile="balanced"),
                lexicon=None,
                seed_words=["ledger"],
                seen=set(),
            )

        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "rare_pronounceable_empty")

    def test_generate_wuggy_helper_handles_zero_seedless_and_import_fail_paths(self) -> None:
        names, report = _generate_wuggy_pseudowords(
            seed_words=["ledger"],
            config=PseudowordConfig(seed_count=0),
        )
        self.assertEqual(names, [])
        self.assertEqual(report["requested_count"], 0)

        names, report = _generate_wuggy_pseudowords(
            seed_words=[],
            config=PseudowordConfig(seed_count=3),
        )
        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "no_seed_words")

        with mock.patch.dict(sys.modules, {"wuggy": None}):
            names, report = _generate_wuggy_pseudowords(
                seed_words=["ledger"],
                config=PseudowordConfig(seed_count=2),
            )

        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "wuggy_unavailable")
        self.assertTrue(report["error_class"])

    def test_generate_wuggy_helper_reports_unsupported_plugin_and_download_failures(self) -> None:
        fake_module = types.SimpleNamespace(WuggyGenerator=_FakeWuggyGenerator)

        with mock.patch.dict(sys.modules, {"wuggy": fake_module}):
            names, report = _generate_wuggy_pseudowords(
                seed_words=["ledger"],
                config=PseudowordConfig(
                    language_plugin="orthographic_spanish",
                    language_plugins=("orthographic_spanish",),
                    seed_count=2,
                ),
            )

        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "unsupported_language_plugin")
        self.assertEqual(report["supported_plugins"], ["orthographic_english", "orthographic_german"])

        class _DownloadFailWuggy(_FakeWuggyGenerator):
            def download_language_plugin(self, language_plugin: str, auto_download: bool = False) -> None:
                del language_plugin, auto_download
                raise RuntimeError("download boom")

        failing_module = types.SimpleNamespace(WuggyGenerator=_DownloadFailWuggy)
        with mock.patch.dict(sys.modules, {"wuggy": failing_module}):
            with mock.patch("brandpipe.pseudowords.inspect.getfile", return_value="/tmp/wuggy/generators/fake.py"):
                with mock.patch("brandpipe.pseudowords.os.path.exists", return_value=False):
                    names, report = _generate_wuggy_pseudowords(
                        seed_words=["ledger"],
                        config=PseudowordConfig(seed_count=2),
                    )

        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "language_plugin_download_failed")
        self.assertEqual(report["error_class"], "RuntimeError")

    def test_generate_wuggy_helper_reports_load_and_generation_failures(self) -> None:
        class _LoadFailWuggy(_FakeWuggyGenerator):
            def load(self, language_plugin: str) -> None:
                del language_plugin
                raise RuntimeError("load boom")

        load_module = types.SimpleNamespace(WuggyGenerator=_LoadFailWuggy)
        with mock.patch.dict(sys.modules, {"wuggy": load_module}):
            names, report = _generate_wuggy_pseudowords(
                seed_words=["ledger"],
                config=PseudowordConfig(seed_count=2),
            )

        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "language_plugin_load_failed")
        self.assertEqual(report["error_class"], "RuntimeError")

        class _GenerationFailWuggy(_FakeWuggyGenerator):
            def generate_classic(self, input_sequences, ncandidates_per_sequence=10, output_mode="plain"):  # type: ignore[no-untyped-def]
                del input_sequences, ncandidates_per_sequence, output_mode
                raise RuntimeError("generation boom")

        generation_module = types.SimpleNamespace(WuggyGenerator=_GenerationFailWuggy)
        with mock.patch.dict(sys.modules, {"wuggy": generation_module}):
            names, report = _generate_wuggy_pseudowords(
                seed_words=["ledger"],
                config=PseudowordConfig(seed_count=2),
            )

        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "generation_failed")
        self.assertEqual(report["error_class"], "RuntimeError")

    def test_generate_pseudoword_pool_preserves_top_level_no_name_warnings(self) -> None:
        brief = Brief(product_core="tenant balance clarity ledger")
        config = PseudowordConfig(seed_count=2)

        with (
            mock.patch.object(MODULE, "derive_seed_words", return_value=["ledger"]),
            mock.patch.object(
                MODULE,
                "_generate_wuggy_pseudowords",
                return_value=(
                    [],
                    {
                        "language_plugin": "orthographic_english",
                        "language_plugins": ["orthographic_english"],
                        "attempted_seed_count": 1,
                        "successful_seed_count": 0,
                        "dropped_seeds": [],
                        "plugin_reports": [],
                        "warning": "wuggy_unavailable",
                        "downloaded_plugin": True,
                        "supported_plugins": ["orthographic_english"],
                        "error_class": "ModuleNotFoundError",
                        "error_message": "missing",
                    },
                ),
            ),
            mock.patch.object(MODULE, "_generate_rare_pronounceable_pool", return_value=([], {"warning": ""})),
        ):
            names, report = generate_pseudoword_pool(brief=brief, config=config)

        self.assertEqual(names, [])
        self.assertEqual(report["warning"], "wuggy_unavailable")
        self.assertTrue(report["downloaded_plugin"])
        self.assertEqual(report["supported_plugins"], ["orthographic_english"])
        self.assertEqual(report["error_class"], "ModuleNotFoundError")

    def test_select_round_seed_names_rotates_pool(self) -> None:
        pool = ["alpha", "bravo", "charly", "delta", "echo"]

        self.assertEqual(select_round_seed_names(seed_pool=pool, round_index=0, max_count=3), ["alpha", "bravo", "charly"])
        self.assertEqual(select_round_seed_names(seed_pool=pool, round_index=1, max_count=3), ["delta", "echo", "alpha"])


if __name__ == "__main__":
    unittest.main()
