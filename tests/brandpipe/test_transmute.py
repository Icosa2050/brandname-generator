# ruff: noqa: E402
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.lexicon import build_lexicon
from brandpipe.models import Brief
from brandpipe.transmute import generate_transmute_candidates


class TransmuteTests(unittest.TestCase):
    def test_generate_transmute_candidates_is_deterministic(self) -> None:
        brief = Brief(
            product_core="utility-cost settlement software",
            target_users=["property managers"],
            trust_signals=["clarity", "reliability"],
        )
        bundle, _report = build_lexicon(brief)

        first = [seed.name for seed in generate_transmute_candidates(bundle, limit=12)]
        second = [seed.name for seed in generate_transmute_candidates(bundle, limit=12)]

        self.assertEqual(first, second)

    def test_generate_transmute_candidates_avoids_literal_domain_words(self) -> None:
        brief = Brief(
            product_core="utility-cost settlement software",
            target_users=["property managers"],
            trust_signals=["clarity", "reliability"],
        )
        bundle, _report = build_lexicon(brief)

        names = [seed.name for seed in generate_transmute_candidates(bundle, limit=20)]

        self.assertTrue(names)
        self.assertTrue(any(name.startswith(prefix) for prefix in {"anch", "harb", "luce"} for name in names))
        self.assertFalse(any(name.startswith("clar") for name in names))
        self.assertNotIn("property", names)
        self.assertNotIn("manager", names)
        self.assertNotIn("managers", names)
        self.assertNotIn("tenant", names)
        self.assertTrue(all(len(name) <= 14 for name in names))


if __name__ == "__main__":
    unittest.main()
