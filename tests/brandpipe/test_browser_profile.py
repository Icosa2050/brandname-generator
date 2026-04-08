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

from brandpipe.browser_profile import (
    DEFAULT_PROFILE_DIR,
    _extract_app_store_search_items,
    _is_challenge_page,
    browser_app_store_items,
    build_target_url,
    resolve_chrome_executable,
    resolve_profile_dir,
    run_browser_profile_smoke,
    warm_browser_profile,
)


class _FakeButton:
    def __init__(self) -> None:
        self.first = self
        self.click_calls = 0

    def click(self, timeout: int = 0) -> None:
        _ = timeout
        self.click_calls += 1


class _FakeLocator:
    def __init__(self, count: int = 0) -> None:
        self._count = count
        self.first = _FakeButton()

    def count(self) -> int:
        return self._count


class _FakePage:
    def __init__(self, *, evaluate_result=None, title: str = "Example Domain", role_count: int = 0) -> None:
        self.url = "https://example.com"
        self._title = title
        self._evaluate_result = [] if evaluate_result is None else evaluate_result
        self._role_count = role_count
        self.goto_calls: list[str] = []
        self.screenshot_calls: list[str] = []
        self.locators: list[_FakeLocator] = []

    def goto(self, url: str, wait_until: str, timeout: int) -> None:
        self.goto_calls.append(url)
        self.url = url
        _ = (wait_until, timeout)

    def wait_for_timeout(self, timeout: int) -> None:
        _ = timeout

    def screenshot(self, path: str, full_page: bool) -> None:
        Path(path).write_bytes(b"fake-image")
        self.screenshot_calls.append(path)
        _ = full_page

    def title(self) -> str:
        return self._title

    def evaluate(self, script: str):
        _ = script
        return self._evaluate_result

    def get_by_role(self, role: str, name=None):
        _ = (role, name)
        locator = _FakeLocator(count=self._role_count)
        self.locators.append(locator)
        return locator


class _TimeoutPage(_FakePage):
    def goto(self, url: str, wait_until: str, timeout: int) -> None:
        _ = (url, wait_until, timeout)
        raise TimeoutError("page load timed out")


class _FakeContext:
    def __init__(self, page: _FakePage) -> None:
        self.pages = [page]
        self._page = page
        self.closed = False
        self.storage_state_calls: list[str] = []

    def new_page(self):
        return self._page

    def storage_state(self, path: str) -> None:
        Path(path).write_text("{\"cookies\": []}\n", encoding="utf-8")
        self.storage_state_calls.append(path)

    def cookies(self):
        return [{"name": "consent"}, {"name": "sid"}]

    def close(self) -> None:
        self.closed = True


class _FakePlaywright:
    def __init__(self, context: _FakeContext) -> None:
        self.chromium = mock.Mock()
        self.chromium.launch_persistent_context.return_value = context
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class _FakeSyncPlaywright:
    def __init__(self, playwright: _FakePlaywright) -> None:
        self._playwright = playwright

    def start(self) -> _FakePlaywright:
        return self._playwright


class BrowserProfileTests(unittest.TestCase):
    def test_resolve_profile_dir_defaults_to_repo_output_path(self) -> None:
        self.assertEqual(resolve_profile_dir(), DEFAULT_PROFILE_DIR)
        self.assertEqual(
            DEFAULT_PROFILE_DIR,
            ROOT_DIR / "test_outputs" / "brandpipe" / "validate" / "playwright-profile",
        )
        self.assertEqual(resolve_profile_dir("~/tmp").name, "tmp")

    def test_build_target_url_supports_brave_search_only(self) -> None:
        self.assertIn("search.brave.com/search", build_target_url(url="", engine="brave", query="brand pipe"))
        self.assertEqual(build_target_url(url="https://example.test", engine="brave", query="ignored"), "https://example.test")
        self.assertEqual(build_target_url(url="", engine="brave", query=""), "https://example.com")
        with self.assertRaisesRegex(ValueError, "unsupported_browser_engine"):
            build_target_url(url="", engine="google", query="brand pipe")

    def test_resolve_chrome_executable_uses_explicit_existing_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            executable = Path(tmp_dir) / "Google Chrome"
            executable.write_text("", encoding="utf-8")
            self.assertEqual(resolve_chrome_executable(executable), executable.resolve())
            with self.assertRaisesRegex(FileNotFoundError, "chrome_executable_not_found"):
                resolve_chrome_executable(Path(tmp_dir) / "Missing Chrome")

    def test_extract_app_store_items_and_challenge_detection(self) -> None:
        page = _FakePage(
            evaluate_result=[
                {"link": "https://apps.apple.com/us/app/tool-grid/id1", "title": "Tool Grid", "slug": "tool-grid"},
                {"link": "", "title": "Missing Link", "slug": "missing"},
                {"link": "https://apps.apple.com/us/app/meridel/id2", "title": "Meridel", "slug": "meridel"},
                "bad-row",
            ]
        )

        items = _extract_app_store_search_items(page)

        self.assertEqual(
            items,
            [
                {"link": "https://apps.apple.com/us/app/tool-grid/id1", "title": "Tool Grid", "slug": "tool-grid"},
                {"link": "https://apps.apple.com/us/app/meridel/id2", "title": "Meridel", "slug": "meridel"},
            ],
        )
        self.assertTrue(_is_challenge_page(final_url="https://google.com/sorry/index", title="Verify you are human"))
        self.assertFalse(_is_challenge_page(final_url="https://example.com", title="Example Domain"))

    def test_run_browser_profile_smoke_writes_profile_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            profile_dir = root / "profile"
            chrome_executable = root / "Google Chrome"
            chrome_executable.write_text("", encoding="utf-8")
            page = _FakePage()
            context = _FakeContext(page)
            playwright = _FakePlaywright(context)

            with mock.patch(
                "brandpipe.browser_profile.sync_playwright",
                return_value=_FakeSyncPlaywright(playwright),
            ):
                result = run_browser_profile_smoke(
                    profile_dir=profile_dir,
                    chrome_executable=chrome_executable,
                    engine="brave",
                    query="brandpipe smoke",
                    headed=False,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["profile_dir"], str(profile_dir.resolve()))
            self.assertTrue(Path(str(result["screenshot_path"])).exists())
            self.assertTrue(Path(str(result["storage_state_path"])).exists())
            report_path = Path(str(result["report_path"]))
            self.assertTrue(report_path.exists())
            saved = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["cookies_count"], 2)
            self.assertEqual(len(page.goto_calls), 1)
            self.assertTrue(context.closed)
            self.assertTrue(playwright.stopped)

    def test_run_browser_profile_smoke_retries_when_explicit_google_url_is_used(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            profile_dir = root / "profile"
            chrome_executable = root / "Google Chrome"
            chrome_executable.write_text("", encoding="utf-8")
            page = _FakePage()
            context = _FakeContext(page)
            playwright = _FakePlaywright(context)

            with mock.patch(
                "brandpipe.browser_profile.sync_playwright",
                return_value=_FakeSyncPlaywright(playwright),
            ):
                result = run_browser_profile_smoke(
                    profile_dir=profile_dir,
                    chrome_executable=chrome_executable,
                    url="https://www.google.com/search?q=brandpipe",
                    engine="brave",
                    query="brandpipe smoke",
                    headed=False,
                )

        self.assertTrue(result["ok"])
        self.assertEqual(
            page.goto_calls,
            [
                "https://www.google.com/search?q=brandpipe",
                "https://www.google.com/search?q=brandpipe",
            ],
        )

    def test_warm_browser_profile_saves_manual_storage_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            profile_dir = root / "profile"
            chrome_executable = root / "Google Chrome"
            chrome_executable.write_text("", encoding="utf-8")
            page = _FakePage()
            context = _FakeContext(page)
            playwright = _FakePlaywright(context)

            with mock.patch(
                "brandpipe.browser_profile.sync_playwright",
                return_value=_FakeSyncPlaywright(playwright),
            ):
                with mock.patch("builtins.input", return_value=""):
                    result = warm_browser_profile(
                        profile_dir=profile_dir,
                        chrome_executable=chrome_executable,
                        engine="brave",
                        query="brandpipe warm",
                    )

            self.assertTrue(result["ok"])
            self.assertTrue(Path(str(result["storage_state_path"])).exists())
            report_path = Path(str(result["report_path"]))
            self.assertTrue(report_path.exists())
            saved = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["cookies_count"], 2)

    def test_browser_app_store_items_handles_boot_failure_page_error_and_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            profile_dir = root / "profile"
            chrome_executable = root / "Google Chrome"
            chrome_executable.write_text("", encoding="utf-8")

            boot_failure_playwright = _FakePlaywright(_FakeContext(_FakePage()))
            boot_failure_playwright.chromium.launch_persistent_context.side_effect = RuntimeError("browser unavailable")
            with mock.patch(
                "brandpipe.browser_profile.sync_playwright",
                return_value=_FakeSyncPlaywright(boot_failure_playwright),
            ):
                boot_failure = browser_app_store_items(
                    query="tool grid",
                    country="US",
                    profile_dir=profile_dir,
                    chrome_executable=chrome_executable,
                )

            page_error_context = _FakeContext(_TimeoutPage())
            page_error_playwright = _FakePlaywright(page_error_context)
            with mock.patch(
                "brandpipe.browser_profile.sync_playwright",
                return_value=_FakeSyncPlaywright(page_error_playwright),
            ):
                page_error = browser_app_store_items(
                    query="tool grid",
                    country="US",
                    profile_dir=profile_dir,
                    chrome_executable=chrome_executable,
                )

            results_page = _FakePage(
                evaluate_result=[
                    {"link": "https://apps.apple.com/us/app/tool-grid/id1", "title": "Tool Grid", "slug": "tool-grid"}
                ],
                title='Results for "tool grid"',
            )
            results_context = _FakeContext(results_page)
            results_playwright = _FakePlaywright(results_context)
            with mock.patch(
                "brandpipe.browser_profile.sync_playwright",
                return_value=_FakeSyncPlaywright(results_playwright),
            ):
                results = browser_app_store_items(
                    query="tool grid",
                    country="US",
                    profile_dir=profile_dir,
                    chrome_executable=chrome_executable,
                )

            no_results_page = _FakePage(evaluate_result=[], title='Results for "void query"')
            no_results_context = _FakeContext(no_results_page)
            no_results_playwright = _FakePlaywright(no_results_context)
            with mock.patch(
                "brandpipe.browser_profile.sync_playwright",
                return_value=_FakeSyncPlaywright(no_results_playwright),
            ):
                no_results = browser_app_store_items(
                    query="void query",
                    country="US",
                    profile_dir=profile_dir,
                    chrome_executable=chrome_executable,
                )

        self.assertFalse(boot_failure["ok"])
        self.assertEqual(boot_failure["state"], "browser_boot_failed")
        self.assertIn("RuntimeError: browser unavailable", boot_failure["error"])

        self.assertFalse(page_error["ok"])
        self.assertEqual(page_error["state"], "timeout")
        self.assertIn("TimeoutError: page load timed out", page_error["error"])

        self.assertTrue(results["ok"])
        self.assertEqual(results["country"], "us")
        self.assertEqual(results["result_count"], 1)
        self.assertEqual(results["state"], "results")

        self.assertTrue(no_results["ok"])
        self.assertEqual(no_results["state"], "no_results")
