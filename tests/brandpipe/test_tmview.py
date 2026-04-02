# ruff: noqa: E402
from __future__ import annotations

import json
import sys
import tempfile
import unittest
import warnings
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.tmview import (
    TmviewProbeResult,
    _parse_result_count,
    _title_exact_or_near,
    _title_match_mode,
    build_tmview_url,
    clone_tmview_runtime_profile,
    normalize_alpha,
    probe_names,
    TmviewProbe,
    write_results_json,
)


class TmviewTests(unittest.TestCase):
    def test_normalize_alpha_preserves_digits(self) -> None:
        self.assertEqual(normalize_alpha("Cord-Nix 42"), "cordnix42")
        self.assertEqual(normalize_alpha("Set 4 You"), "set4you")
        self.assertEqual(normalize_alpha("Andalé"), "andale")
        self.assertEqual(normalize_alpha("VÆRMON"), "vaermon")
        self.assertEqual(normalize_alpha("SØLKRIN"), "soelkrin")

    def test_build_tmview_url_embeds_basic_search(self) -> None:
        url = build_tmview_url("cordnix")
        self.assertIn("basicSearch=%20cordnix", url)
        self.assertIn("criteria=F", url)
        self.assertIn("niceClass=9,OR,42", url)
        self.assertIn("tmStatus=Filed,Registered", url)

    def test_build_tmview_url_accepts_explicit_nice_class(self) -> None:
        url = build_tmview_url("cordnix", nice_class="9,OR,42,OR,EMPTY")
        self.assertIn("niceClass=9,OR,42,OR,EMPTY", url)

    def test_parse_result_count_handles_tmview_summary_formats(self) -> None:
        self.assertEqual(_parse_result_count("1-11 of 11"), 11)
        self.assertEqual(_parse_result_count("Show all 16 results"), 16)
        self.assertEqual(_parse_result_count("No rows found"), 0)
        self.assertIsNone(_parse_result_count("<missing>"))

    def test_probe_names_normalizes_and_deduplicates(self) -> None:
        fake_results = [
            TmviewProbeResult(
                name="Cordnix",
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
        self.assertEqual(results[0].name, "Cordnix")
        probe.probe_name.assert_called_once_with("Cordnix", normalized_name="cordnix")

    def test_probe_names_passes_explicit_nice_class_to_probe(self) -> None:
        fake_results = [
            TmviewProbeResult(
                name="cordnix",
                url="https://example.test",
                query_ok=True,
                source="tmview_playwright",
                exact_hits=0,
                near_hits=0,
                result_count=0,
                sample_text="",
                query_nice_class="9,OR,42",
            )
        ]
        with mock.patch("brandpipe.tmview.TmviewProbe") as probe_cls:
            probe = probe_cls.return_value.__enter__.return_value
            probe.probe_name.side_effect = list(fake_results)
            results = probe_names(
                names=["Cordnix"],
                profile_dir="/tmp/tmview-profile",
                nice_class="9,OR,42",
            )

        self.assertEqual(len(results), 1)
        probe_cls.assert_called_once()
        _, kwargs = probe_cls.call_args
        self.assertEqual(kwargs["nice_class"], "9,OR,42")

    def test_title_exact_or_near_matches_tmview_fuzzy_neighbors(self) -> None:
        self.assertEqual(_title_exact_or_near("samistra", "SAWISTRA"), (False, True))
        self.assertEqual(_title_exact_or_near("tenvrik", "Tendrik"), (False, True))
        self.assertEqual(_title_exact_or_near("hearthvex", "HEALTHDEX THE BUSINESS HEALTH INDEX"), (False, True))
        self.assertEqual(_title_exact_or_near("cirani", "CIRANO"), (False, True))
        self.assertEqual(_title_exact_or_near("andalen", "Andalé"), (False, True))

    def test_title_match_mode_distinguishes_surface_and_normalized_exact(self) -> None:
        self.assertEqual(_title_match_mode("incident.io", "incidentio", "INCIDENT.IO"), ("surface_exact", False))
        self.assertEqual(_title_match_mode("incident.io", "incidentio", "Incidentio"), ("normalized_exact", False))

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

    def test_clone_tmview_runtime_profile_ignores_lock_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source-profile"
            source.mkdir(parents=True)
            (source / "SingletonLock").write_text("locked", encoding="utf-8")
            (source / "SingletonCookie").write_text("cookie", encoding="utf-8")
            (source / "SingletonSocket").write_text("socket", encoding="utf-8")
            (source / "keep.txt").write_text("ok", encoding="utf-8")

            temp_root, runtime_dir = clone_tmview_runtime_profile(source)
            try:
                self.assertTrue((runtime_dir / "keep.txt").exists())
                self.assertFalse((runtime_dir / "SingletonLock").exists())
                self.assertFalse((runtime_dir / "SingletonCookie").exists())
                self.assertFalse((runtime_dir / "SingletonSocket").exists())
            finally:
                if temp_root.exists():
                    import shutil

                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_tmview_probe_uses_temp_runtime_profile_and_cleans_it_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "source-profile"
            source.mkdir(parents=True)
            (source / "keep.txt").write_text("ok", encoding="utf-8")
            browser_path = Path(tmp_dir) / "edge"
            browser_path.write_text("", encoding="utf-8")

            launch_calls: list[dict[str, object]] = []

            class FakeContext:
                def close(self) -> None:
                    return None

            class FakeChromium:
                def launch_persistent_context(self, **kwargs):
                    launch_calls.append(kwargs)
                    return FakeContext()

            class FakePlaywright:
                chromium = FakeChromium()

                def stop(self) -> None:
                    return None

            class FakeStarter:
                def start(self):
                    return FakePlaywright()

            with mock.patch("brandpipe.tmview.sync_playwright", return_value=FakeStarter()):
                probe = TmviewProbe(
                    profile_dir=source,
                    chrome_executable=browser_path,
                    headless=True,
                )
                with probe:
                    self.assertEqual(len(launch_calls), 1)
                    user_data_dir = Path(str(launch_calls[0]["user_data_dir"]))
                    self.assertNotEqual(user_data_dir, source)
                    self.assertTrue(user_data_dir.exists())
                    self.assertTrue((user_data_dir / "keep.txt").exists())
                    self.assertFalse((user_data_dir / "SingletonLock").exists())
                    runtime_root = probe._runtime_profile_root
                    self.assertIsNotNone(runtime_root)
                assert runtime_root is not None
                self.assertFalse(runtime_root.exists())

    def test_tmview_probe_warns_when_cleanup_steps_fail(self) -> None:
        probe = TmviewProbe()
        probe._context = mock.Mock()
        probe._context.close.side_effect = RuntimeError("context down")
        probe._browser = mock.Mock()
        probe._browser.close.side_effect = RuntimeError("browser down")
        probe._playwright = mock.Mock()
        probe._playwright.stop.side_effect = RuntimeError("playwright down")

        with tempfile.TemporaryDirectory() as tmp_dir:
            probe._runtime_profile_root = Path(tmp_dir) / "runtime-profile"
            probe._runtime_profile_root.mkdir()
            with (
                mock.patch("brandpipe.tmview.shutil.rmtree", side_effect=OSError("rmtree down")) as rmtree,
                warnings.catch_warnings(record=True) as caught,
            ):
                warnings.simplefilter("always")
                probe.__exit__(None, None, None)

        self.assertEqual(rmtree.call_count, 1)
        messages = [str(item.message) for item in caught]
        self.assertEqual(len(messages), 4)
        self.assertTrue(any("tmview_context_cleanup_failed" in message for message in messages))
        self.assertTrue(any("tmview_browser_cleanup_failed" in message for message in messages))
        self.assertTrue(any("tmview_playwright_cleanup_failed" in message for message in messages))
        self.assertTrue(any("tmview_runtime_profile_cleanup_failed" in message for message in messages))
