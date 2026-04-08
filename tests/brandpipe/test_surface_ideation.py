# ruff: noqa: E402
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.family_llm import _accept_candidate, generate_family_candidates
from brandpipe.models import Brief, IdeationConfig, NameFamily, SurfacedCandidate, SurfacePolicy
from brandpipe.naming_policy import build_naming_policy
from brandpipe.surface_ideation import (
    _acronym_pool,
    _alpha_tokens,
    _blend_halves,
    _candidate,
    _family_order,
    _family_quota_map,
    _generate_brutalist_utility_family,
    _generate_contrarian_dictionary_family,
    _generate_literal_tld_hack_family,
    _generate_mascot_mutation_family,
    _generate_smooth_blend_family,
    _llm_family_candidates,
    _mascot_variants,
    _root_pool,
    _top_family_candidates,
    generate_candidate_surfaces,
    infer_surface_policy,
    normalize_comparison,
    normalize_surface,
)


class SurfaceIdeationTests(unittest.TestCase):
    def test_surface_helper_normalization_and_policy_inference(self) -> None:
        self.assertEqual(normalize_surface("  XnView MP  "), "xnview mp")
        self.assertEqual(normalize_comparison("VÆRMON"), "vaermon")
        self.assertEqual(infer_surface_policy("brand.example"), SurfacePolicy.DOTTED_LOWER)
        self.assertEqual(infer_surface_policy("brand-name"), SurfacePolicy.HYPHENATED_LOWER)
        self.assertEqual(infer_surface_policy("XnView MP"), SurfacePolicy.TITLE_SPACED_ACRONYM)
        self.assertEqual(infer_surface_policy("Meridel"), SurfacePolicy.MIXED_CASE_ALPHA)
        self.assertEqual(infer_surface_policy("meridel"), SurfacePolicy.ALPHA_LOWER)

    def test_family_order_and_quota_map_follow_policy_and_explicit_overrides(self) -> None:
        config = IdeationConfig(
            provider="fixture",
            rounds=2,
            candidates_per_round=6,
            family_quotas={"runic_forge": 2, "smooth_blend": 3},
            naming_policy=build_naming_policy(
                {
                    "surface": {
                        "family_order": ["runic_forge", "invalid_family", "smooth_blend", "runic_forge"],
                    }
                }
            ),
        )

        ordered = _family_order(config.naming_policy)
        quotas = _family_quota_map(config)

        self.assertEqual(ordered, (NameFamily.RUNIC_FORGE, NameFamily.SMOOTH_BLEND))
        self.assertEqual(quotas, {NameFamily.RUNIC_FORGE: 2, NameFamily.SMOOTH_BLEND: 3})

        default_config = IdeationConfig(provider="fixture", rounds=1, candidates_per_round=7)
        default_quotas = _family_quota_map(default_config)
        self.assertEqual(sum(default_quotas.values()), 7)
        self.assertEqual(len(default_quotas), 6)

    def test_alpha_root_and_acronym_pools_filter_noise(self) -> None:
        brief = Brief(
            product_core="Signal signal settlement tools",
            target_users=["operators", "operators"],
            trust_signals=["clarity"],
            notes="steady utility utility",
        )

        alpha_tokens = _alpha_tokens(brief)
        roots = _root_pool(brief)
        acronyms = _acronym_pool(brief)

        self.assertIn("operators", alpha_tokens)
        self.assertIn("clarity", alpha_tokens)
        self.assertEqual(alpha_tokens.count("signal"), 1)
        self.assertEqual(alpha_tokens.count("operators"), 1)
        self.assertIn("operators", roots)
        self.assertIn("clarity", roots)
        self.assertGreaterEqual(len(acronyms[0]), 2)
        self.assertIn("TSX", acronyms)

    def test_candidate_and_helper_generators_cover_edge_cases(self) -> None:
        candidate = _candidate(
            "Tool Grid",
            NameFamily.BRUTALIST_UTILITY,
            source_kind="family_lane_llm",
            source_detail={"source": "test"},
        )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.name_normalized, "toolgrid")
        self.assertEqual(candidate.surface_policy, SurfacePolicy.TITLE_SPACED_ACRONYM)
        self.assertIn('"source": "test"', candidate.source_detail)
        self.assertIsNone(
            _candidate(
                "a",
                NameFamily.SMOOTH_BLEND,
                source_kind="family_lane_llm",
                source_detail={},
            )
        )

        self.assertEqual(_blend_halves("anchor", "beacon"), "anccon")
        self.assertEqual(_blend_halves("same", "same"), "")
        self.assertEqual(_mascot_variants("otter")[:3], ["Otter", "Ootter", "Ottero"])

    def test_top_family_candidates_and_llm_family_candidates_dedupe_and_rank(self) -> None:
        seeded = [
            SurfacedCandidate(
                display_name="Beta",
                name_normalized="beta",
                family=NameFamily.SMOOTH_BLEND,
                surface_policy=SurfacePolicy.MIXED_CASE_ALPHA,
                family_score=4.0,
                family_rank=0,
            ),
            SurfacedCandidate(
                display_name="Alpha",
                name_normalized="alpha",
                family=NameFamily.SMOOTH_BLEND,
                surface_policy=SurfacePolicy.MIXED_CASE_ALPHA,
                family_score=9.0,
                family_rank=0,
            ),
        ]

        ranked = _top_family_candidates(seeded, 1)
        self.assertEqual([(item.display_name, item.family_rank) for item in ranked], [("Alpha", 1)])

        config = IdeationConfig(provider="fixture")
        with mock.patch(
            "brandpipe.surface_ideation.generate_family_candidates",
            return_value=(["Tool Grid", "Tool Grid", ""], {"family": "brutalist_utility", "attempts": 1}),
        ):
            candidates, report = _llm_family_candidates(
                family=NameFamily.BRUTALIST_UTILITY,
                brief=Brief(product_core="tooling"),
                config=config,
                quota=2,
                success_context=None,
                avoidance_context=None,
            )

        self.assertEqual([candidate.display_name for candidate in candidates], ["Tool Grid"])
        self.assertEqual(report["attempts"], 1)

    def test_deterministic_family_generators_emit_expected_shapes(self) -> None:
        brief = Brief(product_core="settlement utility signal", target_users=["operators"], trust_signals=["clarity"])

        literal = _generate_literal_tld_hack_family(brief, 2)
        smooth = _generate_smooth_blend_family(brief=brief, quota=2)
        mascot = _generate_mascot_mutation_family(brief, 2)
        contrarian = _generate_contrarian_dictionary_family(brief, 2)
        brutalist = _generate_brutalist_utility_family(brief, 2)

        self.assertTrue(any(candidate.display_name.endswith((".io", ".app", ".hq", ".cloud")) for candidate in literal))
        self.assertTrue(all(candidate.family == NameFamily.SMOOTH_BLEND for candidate in smooth))
        self.assertTrue(all(candidate.family == NameFamily.MASCOT_MUTATION for candidate in mascot))
        self.assertTrue(all(candidate.family == NameFamily.CONTRARIAN_DICTIONARY for candidate in contrarian))
        self.assertTrue(all(" " in candidate.display_name for candidate in brutalist))

    def test_smooth_blend_generator_uses_root_fallback_when_no_blends_exist(self) -> None:
        with mock.patch("brandpipe.surface_ideation._root_pool", return_value=["anchor"]):
            generated = _generate_smooth_blend_family(brief=Brief(product_core="anchor"), quota=1)

        self.assertEqual([candidate.display_name for candidate in generated], ["Anchor"])

    def test_family_default_uses_family_llm_and_deterministic_fallback(self) -> None:
        brief = Brief(
            product_core="incident response signal coordination software",
            target_users=["operators", "responders"],
            trust_signals=["clarity", "speed"],
        )
        config = IdeationConfig(
            provider="fixture",
            family_mix_profile="family_default",
            family_quotas={
                "literal_tld_hack": 1,
                "smooth_blend": 1,
                "mascot_mutation": 1,
                "runic_forge": 1,
                "contrarian_dictionary": 1,
                "brutalist_utility": 1,
            },
        )

        def fake_generate_family_candidates(*, family, **kwargs):  # type: ignore[no-untyped-def]
            outputs = {
                "literal_tld_hack": [],
                "smooth_blend": ["brandnamic"],
                "mascot_mutation": ["Ollama"],
                "runic_forge": ["VÆRMON"],
                "contrarian_dictionary": ["Discord"],
                "brutalist_utility": ["Royal TSX"],
            }
            return outputs[family.value], {"family": family.value, "attempts": 2}

        with mock.patch("brandpipe.surface_ideation.generate_family_candidates", side_effect=fake_generate_family_candidates):
            candidates, report = generate_candidate_surfaces(brief=brief, config=config)

        by_family = {candidate.family.value: candidate for candidate in candidates}
        self.assertEqual(by_family["smooth_blend"].display_name, "brandnamic")
        self.assertEqual(by_family["smooth_blend"].source_kind, "family_lane_llm")
        self.assertEqual(by_family["literal_tld_hack"].source_kind, "family_lane_deterministic")
        self.assertEqual(by_family["runic_forge"].display_name, "VÆRMON")
        self.assertGreaterEqual(int(report["family_reports"]["literal_tld_hack"]["fallback_count"]), 1)

    def test_smooth_blend_fallback_is_native_not_legacy(self) -> None:
        brief = Brief(
            product_core="incident response coordination software",
            target_users=["operators", "responders"],
            trust_signals=["clarity", "speed"],
        )
        config = IdeationConfig(
            provider="fixture",
            family_mix_profile="family_default",
            family_quotas={"smooth_blend": 1},
        )

        def fake_generate_family_candidates(*, family, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            if family == NameFamily.SMOOTH_BLEND:
                return [], {"family": family.value, "attempts": 1}
            return ["placeholder"], {"family": family.value, "attempts": 1}

        with mock.patch("brandpipe.surface_ideation.generate_family_candidates", side_effect=fake_generate_family_candidates):
            candidates, report = generate_candidate_surfaces(brief=brief, config=config)

        smooth_blends = [candidate for candidate in candidates if candidate.family == NameFamily.SMOOTH_BLEND]
        self.assertEqual(len(smooth_blends), 1)
        self.assertEqual(smooth_blends[0].source_kind, "family_lane_deterministic")
        self.assertNotIn("legacy", smooth_blends[0].source_detail)
        self.assertEqual(int(report["family_reports"]["smooth_blend"]["fallback_count"]), 1)

    def test_family_default_preserves_surface_forms_from_llm(self) -> None:
        brief = Brief(product_core="incident response tooling")
        config = IdeationConfig(
            provider="fixture",
            family_mix_profile="family_default",
            family_quotas={
                "literal_tld_hack": 1,
                "smooth_blend": 1,
                "mascot_mutation": 1,
                "runic_forge": 1,
                "contrarian_dictionary": 1,
                "brutalist_utility": 1,
            },
        )

        def fake_generate_family_candidates(*, family, **kwargs):  # type: ignore[no-untyped-def]
            outputs = {
                "literal_tld_hack": ["incident.io"],
                "smooth_blend": ["nimbalyst"],
                "mascot_mutation": ["Ollama"],
                "runic_forge": ["VÆRMON"],
                "contrarian_dictionary": ["Discord"],
                "brutalist_utility": ["XnView MP"],
            }
            return outputs[family.value], {"family": family.value, "attempts": 1}

        with mock.patch("brandpipe.surface_ideation.generate_family_candidates", side_effect=fake_generate_family_candidates):
            candidates, _report = generate_candidate_surfaces(brief=brief, config=config)

        display_names = {candidate.display_name: candidate for candidate in candidates}
        self.assertEqual(display_names["incident.io"].name_normalized, "incidentio")
        self.assertEqual(display_names["XnView MP"].name_normalized, "xnviewmp")
        self.assertEqual(display_names["VÆRMON"].name_normalized, "vaermon")
        self.assertEqual(display_names["XnView MP"].source_kind, "family_lane_llm")

    def test_family_default_prefers_higher_quality_family_candidates_when_quota_is_tight(self) -> None:
        brief = Brief(product_core="incident response tooling")
        config = IdeationConfig(
            provider="fixture",
            family_mix_profile="family_default",
            family_quotas={
                "literal_tld_hack": 1,
                "smooth_blend": 1,
                "mascot_mutation": 1,
                "runic_forge": 1,
                "contrarian_dictionary": 1,
                "brutalist_utility": 1,
            },
        )

        def fake_generate_family_candidates(*, family, **kwargs):  # type: ignore[no-untyped-def]
            outputs = {
                "literal_tld_hack": ["incident.io"],
                "smooth_blend": ["nimbalyst"],
                "mascot_mutation": ["Ollama"],
                "runic_forge": ["QYLDAR", "VÆRMON"],
                "contrarian_dictionary": ["Discord"],
                "brutalist_utility": ["XnView MP"],
            }
            return outputs[family.value], {"family": family.value, "attempts": 1}

        with mock.patch("brandpipe.surface_ideation.generate_family_candidates", side_effect=fake_generate_family_candidates):
            candidates, _report = generate_candidate_surfaces(brief=brief, config=config)

        by_family = {candidate.family.value: candidate for candidate in candidates}
        self.assertEqual(by_family["runic_forge"].display_name, "VÆRMON")
        self.assertEqual(by_family["runic_forge"].family_rank, 1)
        self.assertGreater(by_family["runic_forge"].family_score, 0.0)

    def test_normalize_comparison_transliterates_runic_glyphs(self) -> None:
        self.assertEqual(normalize_comparison("VÆRMON"), "vaermon")
        self.assertEqual(normalize_comparison("SØLKRIN"), "soelkrin")

    def test_runic_forge_accepts_structural_corridor_names(self) -> None:
        self.assertEqual(_accept_candidate(NameFamily.RUNIC_FORGE, "VÆRMON"), (True, ""))
        self.assertEqual(_accept_candidate(NameFamily.RUNIC_FORGE, "SØLKRIN"), (True, ""))
        self.assertEqual(_accept_candidate(NameFamily.RUNIC_FORGE, "NAQEL"), (False, "crowded_neighbor_pattern"))
        self.assertEqual(_accept_candidate(NameFamily.RUNIC_FORGE, "KYLRAX"), (False, "fantasy_sludge_tail"))

    def test_runic_forge_generation_requests_one_name_per_call(self) -> None:
        brief = Brief(product_core="premium software")
        config = IdeationConfig(
            provider="openai_compat",
            model="mock-model",
            family_llm_retry_limit=1,
        )
        call_targets: list[int] = []
        produced = iter(
            [
                (["KYLRAX"], {}, ""),
                (["VÆRMON"], {}, ""),
                (["TRÆNVOR"], {}, ""),
            ]
        )

        def fake_provider_call(**kwargs):  # type: ignore[no-untyped-def]
            call_targets.append(int(kwargs["target_count"]))
            return next(produced)

        with mock.patch("brandpipe.family_llm._call_provider_for_family", side_effect=fake_provider_call):
            accepted, report = generate_family_candidates(
                family=NameFamily.RUNIC_FORGE,
                brief=brief,
                config=config,
                quota=2,
            )

        self.assertEqual(call_targets, [1, 1, 1])
        self.assertEqual(accepted, ["VÆRMON", "TRÆNVOR"])
        self.assertEqual(int(report["accepted"]), 2)

    def test_surface_generation_uses_custom_fallback_pools(self) -> None:
        brief = Brief(product_core="incident response tooling")
        config = IdeationConfig(
            provider="fixture",
            family_quotas={"runic_forge": 1},
            naming_policy=build_naming_policy(
                {
                    "surface": {
                        "family_order": ["runic_forge"],
                        "runic_fallbacks": ["ALTVOR"],
                    }
                }
            ),
        )

        def fake_generate_family_candidates(*, family, **kwargs):  # type: ignore[no-untyped-def]
            self.assertEqual(family, NameFamily.RUNIC_FORGE)
            return [], {"family": family.value, "attempts": 1}

        with mock.patch("brandpipe.surface_ideation.generate_family_candidates", side_effect=fake_generate_family_candidates):
            candidates, report = generate_candidate_surfaces(brief=brief, config=config)

        self.assertEqual([candidate.display_name for candidate in candidates], ["ALTVOR"])
        self.assertEqual(int(report["family_reports"]["runic_forge"]["fallback_count"]), 1)


if __name__ == "__main__":
    unittest.main()
