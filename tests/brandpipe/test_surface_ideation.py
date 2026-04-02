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
from brandpipe.models import Brief, IdeationConfig
from brandpipe.models import NameFamily
from brandpipe.naming_policy import build_naming_policy
from brandpipe.surface_ideation import generate_candidate_surfaces, normalize_comparison


class SurfaceIdeationTests(unittest.TestCase):
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
