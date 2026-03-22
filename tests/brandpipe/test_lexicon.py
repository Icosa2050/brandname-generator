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


class LexiconTests(unittest.TestCase):
    def test_build_lexicon_creates_associative_terms_and_morphemes(self) -> None:
        bundle, report = build_lexicon(
            Brief(
                product_core="utility-cost settlement software for landlords and property managers",
                target_users=["private landlords", "property managers"],
                trust_signals=["reliability", "clarity", "legal defensibility"],
                forbidden_directions=["descriptive compounds", "immo clones"],
                language_market="de-ch-en",
            )
        )

        self.assertIn("settlement", bundle.core_terms)
        self.assertIn("property", bundle.core_terms)
        self.assertNotIn("reliability", bundle.modifiers)
        self.assertNotIn("clarity", bundle.modifiers)
        self.assertIn("anchor", bundle.associative_terms)
        self.assertIn("lucent", bundle.associative_terms)
        self.assertIn("harbor", bundle.associative_terms)
        self.assertIn("immo", bundle.avoid_terms)
        self.assertIn("clarity", bundle.avoid_terms)
        self.assertIn("trust", bundle.avoid_terms)
        self.assertTrue(bundle.morphemes)
        self.assertEqual(bundle.language_bias, "neutral")
        self.assertGreater(report["morpheme_count"], 0)


if __name__ == "__main__":
    unittest.main()
