# ruff: noqa: E402
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.blend import best_blend, blend_candidates


class BlendTests(unittest.TestCase):
    def test_blend_candidates_prefers_cleaner_overlap_based_splice(self) -> None:
        candidates = blend_candidates("clarity", "beacon", limit=5)

        self.assertTrue(candidates)
        self.assertNotIn("claritybeacon", candidates)
        self.assertIn(candidates[0], {"clacon", "clarcon", "claricon", "claracon"})

    def test_blend_candidates_is_deterministic(self) -> None:
        first = blend_candidates("property", "manager", limit=5)
        second = blend_candidates("property", "manager", limit=5)

        self.assertEqual(first, second)
        self.assertTrue(all(not item.startswith("prope") for item in first))

    def test_best_blend_handles_tight_pairs_without_junk(self) -> None:
        self.assertIn(best_blend("tenant", "anchor"), {"tenhor", "tenahor", "tenchor"})
        self.assertIsNone(best_blend("aaa", "aaa"))


if __name__ == "__main__":
    unittest.main()
