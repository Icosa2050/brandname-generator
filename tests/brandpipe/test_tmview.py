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

import brandpipe.tmview as MODULE
from brandpipe.tmview import (
    TmviewProbeResult,
    _body_result_segments,
    _has_body_result_context,
    _ignore_tmview_profile_entries,
    _parse_result_count,
    _probe_from_body_segments,
    _probe_from_grid_rows,
    _resolve_tmview_browser_executable,
    _segment_title,
    _title_exact_or_near,
    _title_match_mode,
    build_tmview_url,
    classify_tm_status,
    clone_tmview_runtime_profile,
    normalize_alpha,
    probe_names,
    TmviewProbe,
    write_results_json,
)


def _result(
    *,
    name: str = "Incident.io",
    url: str = "https://example.test/tmview",
    query_ok: bool = True,
    source: str = "tmview_playwright",
    exact_hits: int = 0,
    near_hits: int = 0,
    result_count: int = 0,
    sample_text: str = "",
    query_nice_class: str = "9,OR,42",
    error: str = "",
    exact_sample_text: str = "",
    active_exact_hits: int = 0,
    inactive_exact_hits: int = 0,
    unknown_exact_hits: int = 0,
    state: str = "no_results",
    query_name: str = "Incident.io",
    normalized_name: str = "incidentio",
    query_sequence: str = "Incident.io",
    surface_exact_hits: int = 0,
    normalized_exact_hits: int = 0,
    surface_active_exact_hits: int = 0,
    normalized_active_exact_hits: int = 0,
) -> TmviewProbeResult:
    return TmviewProbeResult(
        name=name,
        url=url,
        query_ok=query_ok,
        source=source,
        exact_hits=exact_hits,
        near_hits=near_hits,
        result_count=result_count,
        sample_text=sample_text,
        query_nice_class=query_nice_class,
        error=error,
        exact_sample_text=exact_sample_text,
        active_exact_hits=active_exact_hits,
        inactive_exact_hits=inactive_exact_hits,
        unknown_exact_hits=unknown_exact_hits,
        state=state,
        query_name=query_name,
        normalized_name=normalized_name,
        query_sequence=query_sequence,
        surface_exact_hits=surface_exact_hits,
        normalized_exact_hits=normalized_exact_hits,
        surface_active_exact_hits=surface_active_exact_hits,
        normalized_active_exact_hits=normalized_active_exact_hits,
    )


class TmviewTests(unittest.TestCase):
    def test_normalize_alpha_preserves_digits(self) -> None:
        self.assertEqual(normalize_alpha("Cord-Nix 42"), "cordnix42")
        self.assertEqual(normalize_alpha("Set 4 You"), "set4you")
        self.assertEqual(normalize_alpha("Andalé"), "andale")
        self.assertEqual(normalize_alpha("VÆRMON"), "vaermon")
        self.assertEqual(normalize_alpha("SØLKRIN"), "soelkrin")

    def test_classify_tm_status_and_title_match_handle_empty_inputs(self) -> None:
        self.assertEqual(classify_tm_status(""), "unknown")
        self.assertEqual(classify_tm_status("Filed and published"), "active")
        self.assertEqual(classify_tm_status("Expired and cancelled"), "inactive")
        self.assertEqual(_title_match_mode("incident.io", "incidentio", ""), ("", False))
        self.assertEqual(_title_match_mode("incident.io", "incidentio", "incidentio zzzz"), ("", False))

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
        self.assertIsNone(_parse_result_count(""))
        self.assertIsNone(_parse_result_count("<missing>"))

    def test_result_context_helpers_extract_segments_and_titles(self) -> None:
        body_text = (
            "Header"
            " | - | Incident.io | Registered | goods and services"
            " | - | Ignore me"
        )
        self.assertTrue(_has_body_result_context("Incident.io | goods and services"))
        self.assertFalse(_has_body_result_context("Nothing to see here"))
        self.assertEqual(_body_result_segments(body_text), ["Incident.io | Registered | goods and services"])
        self.assertEqual(_segment_title("Incident.io | Registered | goods and services"), "Incident.io")
        self.assertEqual(_segment_title(" |  | "), "")

    def test_probe_from_body_segments_tracks_exact_near_and_status_buckets(self) -> None:
        body_text = (
            "Header"
            " | - | Incident.io | Registered | goods and services"
            " | - | Incidentio | Expired | goods and services"
            " | - | Incident.io | Applicant name Example | goods and services"
            " | - | Incidendio | Applicant name Example | goods and services"
        )

        stats = _probe_from_body_segments("Incident.io", "incidentio", body_text)

        self.assertEqual(stats["exact_hits"], 3)
        self.assertEqual(stats["near_hits"], 1)
        self.assertEqual(stats["active_exact_hits"], 1)
        self.assertEqual(stats["inactive_exact_hits"], 1)
        self.assertEqual(stats["unknown_exact_hits"], 1)
        self.assertEqual(stats["surface_exact_hits"], 2)
        self.assertEqual(stats["normalized_exact_hits"], 1)
        self.assertEqual(stats["surface_active_exact_hits"], 1)
        self.assertEqual(stats["normalized_active_exact_hits"], 0)
        self.assertEqual(len(stats["exact_samples"]), 3)
        self.assertEqual(len(stats["samples"]), 2)

    def test_probe_from_grid_rows_tracks_exact_near_and_status_buckets(self) -> None:
        rows = [
            {"title": "Incident.io", "text": "Incident.io Registered"},
            {"title": "Incidentio", "text": "Incidentio Expired"},
            {"title": "Incident.io", "text": "Incident.io Applicant name Example"},
            {"title": "Incidendio", "text": "Incidendio Applicant name Example"},
        ]

        stats = _probe_from_grid_rows("Incident.io", "incidentio", rows)

        self.assertEqual(stats["exact_hits"], 3)
        self.assertEqual(stats["near_hits"], 1)
        self.assertEqual(stats["active_exact_hits"], 1)
        self.assertEqual(stats["inactive_exact_hits"], 1)
        self.assertEqual(stats["unknown_exact_hits"], 1)
        self.assertEqual(stats["surface_exact_hits"], 2)
        self.assertEqual(stats["normalized_exact_hits"], 1)
        self.assertEqual(stats["surface_active_exact_hits"], 1)
        self.assertEqual(stats["normalized_active_exact_hits"], 0)
        self.assertEqual(len(stats["exact_samples"]), 3)
        self.assertEqual(len(stats["samples"]), 2)

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

    def test_browser_resolution_and_profile_clone_guardrails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            explicit_browser = root / "chrome"
            explicit_browser.write_text("", encoding="utf-8")
            fallback_browser = root / "edge"
            fallback_browser.write_text("", encoding="utf-8")

            with mock.patch("brandpipe.tmview.resolve_chrome_executable", return_value=explicit_browser.resolve()) as resolve:
                self.assertEqual(_resolve_tmview_browser_executable(explicit_browser), explicit_browser.resolve())
            resolve.assert_called_once_with(explicit_browser)

            with mock.patch.object(MODULE, "TMVIEW_BROWSER_CANDIDATES", (fallback_browser,)):
                self.assertEqual(_resolve_tmview_browser_executable(None), fallback_browser.resolve())

            with mock.patch.object(MODULE, "TMVIEW_BROWSER_CANDIDATES", (root / "missing-browser",)):
                with self.assertRaisesRegex(FileNotFoundError, "tmview_browser_executable_not_found"):
                    _resolve_tmview_browser_executable(None)

            with mock.patch("brandpipe.tmview.resolve_profile_dir", return_value=root / "missing-profile"):
                with self.assertRaisesRegex(FileNotFoundError, "tmview_profile_dir_not_found"):
                    clone_tmview_runtime_profile("missing-profile")

        self.assertEqual(
            _ignore_tmview_profile_entries("", ["SingletonLock", "keep.txt", "GPUCache"]),
            {"SingletonLock", "GPUCache"},
        )

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

    def test_tmview_probe_without_profile_uses_browser_launch_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            browser_path = Path(tmp_dir) / "edge"
            browser_path.write_text("", encoding="utf-8")
            launch_calls: list[dict[str, object]] = []
            new_context_calls: list[dict[str, object]] = []
            init_scripts: list[str] = []

            class FakeContext:
                def add_init_script(self, script: str) -> None:
                    init_scripts.append(script)

                def close(self) -> None:
                    return None

            class FakeBrowser:
                def new_context(self, **kwargs):
                    new_context_calls.append(kwargs)
                    return FakeContext()

                def close(self) -> None:
                    return None

            class FakeChromium:
                def launch(self, **kwargs):
                    launch_calls.append(kwargs)
                    return FakeBrowser()

            class FakePlaywright:
                chromium = FakeChromium()

                def stop(self) -> None:
                    return None

            class FakeStarter:
                def start(self):
                    return FakePlaywright()

            with (
                mock.patch("brandpipe.tmview.sync_playwright", return_value=FakeStarter()),
                mock.patch("brandpipe.tmview.resolve_chrome_executable", return_value=browser_path.resolve()),
            ):
                probe = TmviewProbe(headless=False, chrome_executable=browser_path)
                with probe:
                    self.assertTrue(probe.available())

            self.assertEqual(len(launch_calls), 1)
            self.assertEqual(launch_calls[0]["headless"], False)
            self.assertEqual(launch_calls[0]["executable_path"], str(browser_path.resolve()))
            self.assertEqual(new_context_calls, [{"java_script_enabled": True}])
            self.assertEqual(len(init_scripts), 1)
            self.assertIn("navigator", init_scripts[0])

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

    def test_probe_query_handles_invalid_unavailable_and_error_paths(self) -> None:
        probe = TmviewProbe(nice_class="9,OR,42")

        invalid = probe._probe_query(query_name="", display_name="", normalized_name="")
        self.assertEqual(invalid.error, "invalid_name")
        self.assertEqual(invalid.state, "invalid_name")
        self.assertFalse(invalid.query_ok)

        probe._import_error = "playwright_launch_error:RuntimeError"
        unavailable = probe._probe_query(
            query_name="Incident.io",
            display_name="Incident.io",
            normalized_name="incidentio",
        )
        self.assertEqual(unavailable.error, "playwright_launch_error:RuntimeError")
        self.assertEqual(unavailable.state, "browser_unavailable")
        self.assertFalse(unavailable.query_ok)

        class FakeTimeoutError(Exception):
            pass

        timeout_context = mock.Mock()
        timeout_context.new_page.side_effect = FakeTimeoutError("too slow")
        probe._import_error = ""
        probe._context = timeout_context
        timeout_result = probe._probe_query(
            query_name="Incident.io",
            display_name="Incident.io",
            normalized_name="incidentio",
        )
        self.assertEqual(timeout_result.state, "timeout")
        self.assertIn("page_error:FakeTimeoutError", timeout_result.error)

        closed: list[bool] = []
        page = mock.Mock()
        page.goto.side_effect = RuntimeError("boom")
        page.close.side_effect = lambda: closed.append(True)
        error_context = mock.Mock()
        error_context.new_page.return_value = page
        probe._context = error_context
        error_result = probe._probe_query(
            query_name="Incident.io",
            display_name="Incident.io",
            normalized_name="incidentio",
        )
        self.assertEqual(error_result.state, "page_error")
        self.assertIn("page_error:RuntimeError", error_result.error)
        self.assertEqual(closed, [True])

    def test_probe_query_collects_grid_rows_without_real_browser(self) -> None:
        class FakeLocator:
            def __init__(self, text: str) -> None:
                self.first = self
                self._text = text

            def inner_text(self, timeout: int | None = None) -> str:
                del timeout
                return self._text

        class FakePage:
            def __init__(self) -> None:
                self.closed = False
                self._extract_calls = 0
                self._scroll_calls = 0

            def goto(self, *args, **kwargs) -> None:
                del args, kwargs

            def wait_for_url(self, *args, **kwargs) -> None:
                del args, kwargs

            def wait_for_function(self, *args, **kwargs) -> None:
                del args, kwargs
                raise RuntimeError("results hook late")

            def wait_for_timeout(self, *args, **kwargs) -> None:
                del args, kwargs

            def inner_text(self, selector: str) -> str:
                self.assertEqual(selector, "body")
                return "Grid body text"

            def locator(self, selector: str) -> FakeLocator:
                self.assertEqual(selector, MODULE.TMVIEW_RESULTS_PAGINATION_SELECTOR)
                return FakeLocator("Show all 10 results")

            def evaluate(self, script: str):
                if "scrollIntoView" in script:
                    self._scroll_calls += 1
                    return True
                self._extract_calls += 1
                return [
                    {"title": "Incident.io", "text": "Incident.io Registered"},
                    {"title": "Incident.io", "text": "Incident.io Registered"},
                    {"title": "Incidentio", "text": "Incidentio Expired"},
                    {"title": "Incidendio", "text": "Incidendio Applicant name Example"},
                    {"title": "", "text": ""},
                    "bad-row",
                ]

            def close(self) -> None:
                self.closed = True

            def assertEqual(self, left, right) -> None:
                if left != right:
                    raise AssertionError(f"{left!r} != {right!r}")

        page = FakePage()
        context = mock.Mock()
        context.new_page.return_value = page
        probe = TmviewProbe(settle_ms=0, nice_class="9,OR,42")
        probe._context = context

        result = probe._probe_query(
            query_name="Incident.io",
            display_name="Incident.io",
            normalized_name="incidentio",
        )

        self.assertTrue(result.query_ok)
        self.assertEqual(result.exact_hits, 2)
        self.assertEqual(result.near_hits, 1)
        self.assertEqual(result.active_exact_hits, 1)
        self.assertEqual(result.inactive_exact_hits, 1)
        self.assertEqual(result.surface_exact_hits, 1)
        self.assertEqual(result.normalized_exact_hits, 1)
        self.assertEqual(result.state, "results")
        self.assertEqual(result.result_count, 10)
        self.assertEqual(page._extract_calls, 2)
        self.assertEqual(page._scroll_calls, 1)
        self.assertTrue(page.closed)

    def test_probe_query_falls_back_to_body_segments_when_grid_rows_fail(self) -> None:
        class FakeLocator:
            def __init__(self, text: str) -> None:
                self.first = self
                self._text = text

            def inner_text(self, timeout: int | None = None) -> str:
                del timeout
                return self._text

        class FakePage:
            def __init__(self) -> None:
                self.closed = False

            def goto(self, *args, **kwargs) -> None:
                del args, kwargs

            def wait_for_url(self, *args, **kwargs) -> None:
                del args, kwargs

            def wait_for_function(self, *args, **kwargs) -> None:
                del args, kwargs

            def wait_for_timeout(self, *args, **kwargs) -> None:
                del args, kwargs

            def inner_text(self, selector: str) -> str:
                if selector != "body":
                    raise AssertionError(selector)
                return "Header | - | Incident.io | Registered | goods and services"

            def locator(self, selector: str) -> FakeLocator:
                if selector != MODULE.TMVIEW_RESULTS_PAGINATION_SELECTOR:
                    raise AssertionError(selector)
                return FakeLocator("Show all 1 results")

            def evaluate(self, script: str):
                del script
                raise RuntimeError("grid unavailable")

            def close(self) -> None:
                self.closed = True

        page = FakePage()
        context = mock.Mock()
        context.new_page.return_value = page
        probe = TmviewProbe(settle_ms=0, nice_class="9,OR,42")
        probe._context = context

        result = probe._probe_query(
            query_name="Incident.io",
            display_name="Incident.io",
            normalized_name="incidentio",
        )

        self.assertTrue(result.query_ok)
        self.assertEqual(result.exact_hits, 1)
        self.assertEqual(result.near_hits, 0)
        self.assertEqual(result.result_count, 1)
        self.assertEqual(result.state, "results")
        self.assertTrue(page.closed)

    def test_probe_name_short_circuits_and_merges_follow_up_queries(self) -> None:
        probe = TmviewProbe(nice_class="9,OR,42")
        surface_near = _result(near_hits=1, result_count=1, sample_text="surface near", state="results")
        normalized_error = _result(
            query_ok=False,
            query_name="incidentio",
            error="page_error:RuntimeError",
            query_sequence="incidentio",
        )
        surface_retry = _result(query_name="Incident.io", query_sequence="Incident.io", sample_text="surface")
        normalized_success = _result(
            query_name="incidentio",
            query_sequence="incidentio",
            exact_hits=2,
            near_hits=1,
            result_count=4,
            sample_text="normalized",
            exact_sample_text="normalized exact",
            active_exact_hits=1,
            inactive_exact_hits=1,
            unknown_exact_hits=0,
            state="results",
            normalized_exact_hits=2,
            normalized_active_exact_hits=1,
        )

        with mock.patch.object(probe, "_probe_query", side_effect=[surface_near]):
            first = probe.probe_name("Incident.io", normalized_name="incidentio")
        self.assertIs(first, surface_near)

        with mock.patch.object(probe, "_probe_query", side_effect=[surface_retry, normalized_error]):
            fallback = probe.probe_name("Incident.io", normalized_name="incidentio")
        self.assertEqual(fallback.query_sequence, "Incident.io,incidentio")
        self.assertEqual(fallback.error, "page_error:RuntimeError")

        with mock.patch.object(probe, "_probe_query", side_effect=[surface_retry, normalized_success]):
            merged = probe.probe_name("Incident.io", normalized_name="incidentio")
        self.assertEqual(merged.exact_hits, 2)
        self.assertEqual(merged.near_hits, 1)
        self.assertEqual(merged.result_count, 4)
        self.assertEqual(merged.query_sequence, "Incident.io,incidentio")
        self.assertEqual(merged.sample_text, "surface || normalized")
        self.assertEqual(merged.exact_sample_text, "normalized exact")
        self.assertEqual(merged.state, "results")
