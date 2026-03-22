# ruff: noqa: E402
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.tmview import (
    TmviewProbeResult,
    _title_exact_or_near,
    build_tmview_url,
    normalize_alpha,
    probe_names,
    write_results_json,
)


class TmviewTests(unittest.TestCase):
    def test_normalize_alpha_strips_non_letters(self) -> None:
        self.assertEqual(normalize_alpha("Cord-Nix 42"), "cordnix")
        self.assertEqual(normalize_alpha("Andalé"), "andale")

    def test_build_tmview_url_embeds_basic_search(self) -> None:
        url = build_tmview_url("cordnix")
        self.assertIn("basicSearch=%20cordnix", url)
        self.assertIn("criteria=F", url)
        self.assertIn("niceClass=9,OR,42,OR,EMPTY", url)
        self.assertIn("tmStatus=Filed,Registered", url)

    def test_probe_names_normalizes_and_deduplicates(self) -> None:
        fake_results = [
            TmviewProbeResult(
                name="cordnix",
                url="https://example.test",
                query_ok=True,
                source="tmview_playwright",
                exact_hits=0,
                near_hits=1,
                result_count=2,
                sample_text="Cordix",
            )
        ]
        with mock.patch("brandpipe.tmview.TmviewProbe") as probe_cls:
            probe = probe_cls.return_value.__enter__.return_value
            probe.probe_name.side_effect = list(fake_results)
            results = probe_names(names=["Cordnix", "cordnix"], profile_dir="/tmp/tmview-profile")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, "cordnix")
        probe.probe_name.assert_called_once_with("cordnix")

    def test_title_exact_or_near_matches_tmview_fuzzy_neighbors(self) -> None:
        self.assertEqual(_title_exact_or_near("samistra", "SAWISTRA"), (False, True))
        self.assertEqual(_title_exact_or_near("tenvrik", "Tendrik"), (False, True))
        self.assertEqual(_title_exact_or_near("hearthvex", "HEALTHDEX THE BUSINESS HEALTH INDEX"), (False, True))
        self.assertEqual(_title_exact_or_near("cirani", "CIRANO"), (False, True))
        self.assertEqual(_title_exact_or_near("andalen", "Andalé"), (False, True))

    def test_write_results_json_persists_payload(self) -> None:
        result = TmviewProbeResult(
            name="cordnix",
            url="https://example.test",
            query_ok=True,
            source="tmview_playwright",
            exact_hits=0,
            near_hits=1,
            result_count=2,
            sample_text="Cordix",
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_path = Path(tmp_dir) / "tmview.json"
            written = write_results_json(out_path, [result])
            self.assertEqual(written, out_path.resolve())
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(payload[0]["name"], "cordnix")
