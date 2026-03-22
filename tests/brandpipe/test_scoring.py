# ruff: noqa: E402
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.ranking import rank_candidates
from brandpipe.scoring import build_attractiveness_result, score_name_attractiveness


class ScoringTests(unittest.TestCase):
    def test_attractiveness_prefers_smoother_names(self) -> None:
        smooth = score_name_attractiveness("baltera")
        rough = score_name_attractiveness("vexlaris")

        self.assertEqual(smooth.status, "pass")
        self.assertGreater(smooth.score_delta, rough.score_delta)

    def test_attractiveness_warns_closed_heavy_name(self) -> None:
        result = score_name_attractiveness("chardlen")
        self.assertEqual(result.status, "warn")
        self.assertLess(result.score_delta, 7.5)

    def test_attractiveness_warns_heavy_flat_name_even_if_numeric_score_clears(self) -> None:
        result = score_name_attractiveness("cordastin")
        self.assertEqual(result.status, "warn")
        self.assertIn("closed_syllables_heavy", result.reasons)

    def test_attractiveness_warns_lexical_seam_shape(self) -> None:
        result = score_name_attractiveness("precela")
        self.assertEqual(result.status, "warn")
        self.assertIn("lexical_seam", result.reasons)

    def test_attractiveness_warns_generic_safe_opening_name(self) -> None:
        result = score_name_attractiveness("preceral")
        self.assertEqual(result.status, "warn")
        self.assertIn("generic_safe_opening", result.reasons)

    def test_attractiveness_keeps_compact_clean_name(self) -> None:
        result = score_name_attractiveness("habitan")
        self.assertEqual(result.status, "pass")

    def test_attractiveness_warns_leading_harsh_name(self) -> None:
        result = score_name_attractiveness("quenar")
        self.assertEqual(result.status, "warn")
        self.assertIn("leading_harsh", result.reasons)

    def test_attractiveness_warns_literal_signal_fragment_name(self) -> None:
        result = score_name_attractiveness("clarcivic")
        self.assertEqual(result.status, "warn")
        self.assertIn("literal_signal_fragment", result.reasons)

    def test_build_attractiveness_result_uses_candidate_result_contract(self) -> None:
        result = build_attractiveness_result("sollaren")

        self.assertEqual(result.check_name, "attractiveness")
        self.assertIn(result.reason, {"attractiveness_pass", "attractiveness_warn"})
        self.assertIn("reasons", result.details)

    def test_ranking_uses_attractiveness_to_split_clean_candidates(self) -> None:
        ranked = rank_candidates(
            {
                "baltera": [build_attractiveness_result("baltera")],
                "chardlen": [build_attractiveness_result("chardlen")],
            }
        )

        self.assertEqual(ranked[0].name, "baltera")
        self.assertEqual(ranked[0].decision, "candidate")
        self.assertEqual(ranked[1].name, "chardlen")
        self.assertEqual(ranked[1].decision, "watch")


if __name__ == "__main__":
    unittest.main()
