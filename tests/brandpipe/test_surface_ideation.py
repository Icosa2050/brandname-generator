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

from brandpipe.models import Brief, IdeationConfig
from brandpipe.surface_ideation import generate_candidate_surfaces


class SurfaceIdeationTests(unittest.TestCase):
    def test_surface_diverse_v2_uses_family_llm_and_deterministic_fallback(self) -> None:
        brief = Brief(
            product_core="incident response signal coordination software",
            target_users=["operators", "responders"],
            trust_signals=["clarity", "speed"],
        )
        config = IdeationConfig(
            provider="fixture",
            family_mix_profile="surface_diverse_v2",
            family_quotas={
                "literal_tld_hack": 1,
                "smooth_blend": 1,
                "mascot_mutation": 1,
                "contrarian_dictionary": 1,
                "brutalist_utility": 1,
            },
        )

        def fake_generate_family_candidates(*, family, **kwargs):  # type: ignore[no-untyped-def]
            outputs = {
                "literal_tld_hack": [],
                "smooth_blend": ["brandnamic"],
                "mascot_mutation": ["Ollama"],
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
        self.assertGreaterEqual(int(report["family_reports"]["literal_tld_hack"]["fallback_count"]), 1)

    def test_surface_diverse_v2_preserves_surface_forms_from_llm(self) -> None:
        brief = Brief(product_core="incident response tooling")
        config = IdeationConfig(
            provider="fixture",
            family_mix_profile="surface_diverse_v2",
            family_quotas={
                "literal_tld_hack": 1,
                "smooth_blend": 1,
                "mascot_mutation": 1,
                "contrarian_dictionary": 1,
                "brutalist_utility": 1,
            },
        )

        def fake_generate_family_candidates(*, family, **kwargs):  # type: ignore[no-untyped-def]
            outputs = {
                "literal_tld_hack": ["incident.io"],
                "smooth_blend": ["nimbalyst"],
                "mascot_mutation": ["Ollama"],
                "contrarian_dictionary": ["Discord"],
                "brutalist_utility": ["XnView MP"],
            }
            return outputs[family.value], {"family": family.value, "attempts": 1}

        with mock.patch("brandpipe.surface_ideation.generate_family_candidates", side_effect=fake_generate_family_candidates):
            candidates, _report = generate_candidate_surfaces(brief=brief, config=config)

        display_names = {candidate.display_name: candidate for candidate in candidates}
        self.assertEqual(display_names["incident.io"].name_normalized, "incidentio")
        self.assertEqual(display_names["XnView MP"].name_normalized, "xnviewmp")
        self.assertEqual(display_names["XnView MP"].source_kind, "family_lane_llm")


if __name__ == "__main__":
    unittest.main()
