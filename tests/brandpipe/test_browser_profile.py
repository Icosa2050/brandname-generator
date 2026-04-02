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
    build_target_url,
    resolve_chrome_executable,
    resolve_profile_dir,
    run_browser_profile_smoke,
    warm_browser_profile,
)


class _FakeButton:
    def __init__(self) -> None:
        self.first = self

    def click(self, timeout: int = 0) -> None:
        _ = timeout


class _FakeLocator:
    def __init__(self, count: int = 0) -> None:
        self._count = count
        self.first = _FakeButton()

    def count(self) -> int:
        return self._count


class _FakePage:
    def __init__(self) -> None:
        self.url = "https://example.com"
        self._title = "Example Domain"
        self.goto_calls: list[str] = []
        self.screenshot_calls: list[str] = []

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

    def get_by_role(self, role: str, name=None):
        _ = (role, name)
        return _FakeLocator(count=0)


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

    def test_build_target_url_supports_brave_search_only(self) -> None:
        self.assertIn("search.brave.com/search", build_target_url(url="", engine="brave", query="brand pipe"))
        with self.assertRaisesRegex(ValueError, "unsupported_browser_engine"):
            build_target_url(url="", engine="google", query="brand pipe")

    def test_resolve_chrome_executable_uses_explicit_existing_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            executable = Path(tmp_dir) / "Google Chrome"
            executable.write_text("", encoding="utf-8")
            self.assertEqual(resolve_chrome_executable(executable), executable.resolve())

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
