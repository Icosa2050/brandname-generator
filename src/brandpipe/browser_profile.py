from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from urllib import parse

from playwright.sync_api import sync_playwright


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_PROFILE_DIR = ROOT_DIR / "test_outputs" / "brandpipe" / "playwright-profile"
DEFAULT_ARTIFACTS_DIRNAME = "artifacts"
DEFAULT_CHROME_EXECUTABLE = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
CONSENT_LABELS = (
    "Accept all",
    "Alle akzeptieren",
    "Akzeptieren",
    "I agree",
    "Accept",
    "Zustimmen",
)
CHALLENGE_TOKENS = ("captcha", "sorry", "unusual traffic", "verify you are human")


def resolve_profile_dir(raw: str | Path | None = None) -> Path:
    if raw is None:
        return DEFAULT_PROFILE_DIR
    return Path(raw).expanduser().resolve()


def resolve_chrome_executable(raw: str | Path | None = None) -> Path:
    candidate = Path(raw).expanduser().resolve() if raw else DEFAULT_CHROME_EXECUTABLE
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"chrome_executable_not_found:{candidate}")


def build_target_url(*, url: str, engine: str, query: str) -> str:
    if url:
        return str(url)
    normalized_query = str(query or "").strip()
    if not normalized_query:
        return "https://example.com"
    encoded = parse.quote_plus(normalized_query)
    engine_key = str(engine or "google").strip().lower()
    if engine_key == "google":
        return f"https://www.google.com/search?q={encoded}&hl=en&gl=de"
    if engine_key == "brave":
        return f"https://search.brave.com/search?q={encoded}&source=web"
    raise ValueError(f"unsupported_browser_engine:{engine}")


def _dismiss_cookie_banner(page) -> None:
    for label in CONSENT_LABELS:
        try:
            button = page.get_by_role("button", name=re.compile(label, re.IGNORECASE))
            if button.count() > 0:
                button.first.click(timeout=1200)
                page.wait_for_timeout(600)
                return
        except Exception:
            continue


def _is_challenge_page(*, final_url: str, title: str) -> bool:
    haystack = f"{final_url} {title}".strip().lower()
    return any(token in haystack for token in CHALLENGE_TOKENS)


def _extract_google_search_items(page) -> list[dict[str, str]]:
    rows = page.evaluate(
        """() => {
          const out = [];
          const seen = new Set();
          const anchors = Array.from(document.querySelectorAll('a'));
          for (const anchor of anchors) {
            const href = (anchor.href || '').trim();
            const titleNode = anchor.querySelector('h3');
            const title = (titleNode?.innerText || anchor.innerText || '').trim();
            if (!href || !title) continue;
            if (!href.startsWith('http')) continue;
            if (href.includes('/search?') || href.includes('/preferences') || href.includes('/advanced_search')) continue;
            if (seen.has(href)) continue;
            seen.add(href);
            const snippet = (anchor.closest('div')?.innerText || '').trim();
            out.push({ link: href, title, snippet });
            if (out.length >= 10) break;
          }
          return out;
        }"""
    )
    if not isinstance(rows, list):
        return []
    cleaned: list[dict[str, str]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        link = str(item.get("link") or "").strip()
        title = str(item.get("title") or "").strip()
        snippet = str(item.get("snippet") or "").strip()
        if not link or not title:
            continue
        cleaned.append({"link": link, "title": title, "snippet": snippet})
    return cleaned


def _extract_app_store_search_items(page) -> list[dict[str, str]]:
    rows = page.evaluate(
        """() => {
          const out = [];
          const seen = new Set();
          const anchors = Array.from(document.querySelectorAll('a[href*="/app/"]'));
          for (const anchor of anchors) {
            const href = (anchor.href || '').trim();
            if (!href.includes('/app/')) continue;
            if (seen.has(href)) continue;
            const titleNode = anchor.querySelector('h1, h2, h3, h4');
            const aria = (anchor.getAttribute('aria-label') || '').trim();
            const text = (titleNode?.innerText || aria || anchor.innerText || '').trim();
            const title = text.split('\\n').map((part) => part.trim()).filter(Boolean)[0] || '';
            const slugMatch = href.match(/\\/app\\/([^/?#]+)\\/id\\d+/i);
            const slug = slugMatch ? slugMatch[1].trim() : '';
            seen.add(href);
            out.push({ link: href, title, slug });
            if (out.length >= 20) break;
          }
          return out;
        }"""
    )
    if not isinstance(rows, list):
        return []
    cleaned: list[dict[str, str]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        link = str(item.get("link") or "").strip()
        title = str(item.get("title") or "").strip()
        slug = str(item.get("slug") or "").strip()
        if not link:
            continue
        cleaned.append({"link": link, "title": title, "slug": slug})
    return cleaned


def run_browser_profile_smoke(
    *,
    profile_dir: str | Path | None = None,
    chrome_executable: str | Path | None = None,
    url: str = "",
    engine: str = "google",
    query: str = "",
    headed: bool = False,
    timeout_ms: int = 30000,
    settle_ms: int = 1500,
) -> dict[str, object]:
    resolved_profile_dir = resolve_profile_dir(profile_dir)
    resolved_profile_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = resolved_profile_dir / DEFAULT_ARTIFACTS_DIRNAME
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    screenshot_path = artifacts_dir / "smoke.png"
    storage_state_path = artifacts_dir / "storage-state.json"
    report_path = artifacts_dir / "smoke-result.json"
    browser_path = resolve_chrome_executable(chrome_executable)
    target_url = build_target_url(url=url, engine=engine, query=query)

    playwright = sync_playwright().start()
    try:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(resolved_profile_dir),
            executable_path=str(browser_path),
            headless=not bool(headed),
            args=["--disable-blink-features=AutomationControlled"],
            java_script_enabled=True,
        )
        try:
            page = None
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=max(3000, int(timeout_ms)))
            _dismiss_cookie_banner(page)
            if str(query or "").strip() and "google." in page.url:
                page.goto(target_url, wait_until="domcontentloaded", timeout=max(3000, int(timeout_ms)))
            page.wait_for_timeout(max(0, int(settle_ms)))
            page.screenshot(path=str(screenshot_path), full_page=True)
            context.storage_state(path=str(storage_state_path))
            cookies_count = len(context.cookies())
            result = {
                "ok": True,
                "profile_dir": str(resolved_profile_dir),
                "chrome_executable": str(browser_path),
                "target_url": target_url,
                "final_url": page.url,
                "title": page.title(),
                "headed": bool(headed),
                "cookies_count": cookies_count,
                "screenshot_path": str(screenshot_path),
                "storage_state_path": str(storage_state_path),
                "report_path": str(report_path),
            }
        finally:
            context.close()
    finally:
        playwright.stop()

    report_path.write_text(json.dumps(result, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return result


def warm_browser_profile(
    *,
    profile_dir: str | Path | None = None,
    chrome_executable: str | Path | None = None,
    url: str = "",
    engine: str = "google",
    query: str = "",
    timeout_ms: int = 30000,
    settle_ms: int = 1500,
) -> dict[str, object]:
    resolved_profile_dir = resolve_profile_dir(profile_dir)
    resolved_profile_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = resolved_profile_dir / DEFAULT_ARTIFACTS_DIRNAME
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    storage_state_path = artifacts_dir / "manual-storage-state.json"
    report_path = artifacts_dir / "warmup-result.json"
    browser_path = resolve_chrome_executable(chrome_executable)
    target_url = build_target_url(url=url, engine=engine, query=query)

    playwright = sync_playwright().start()
    try:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(resolved_profile_dir),
            executable_path=str(browser_path),
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
            java_script_enabled=True,
        )
        try:
            page = None
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=max(3000, int(timeout_ms)))
            _dismiss_cookie_banner(page)
            page.wait_for_timeout(max(0, int(settle_ms)))
            print(
                "Warm the dedicated browser profile in the opened Chrome window, then press Enter here to save it.",
                file=sys.stderr,
                flush=True,
            )
            input()
            context.storage_state(path=str(storage_state_path))
            cookies_count = len(context.cookies())
            result = {
                "ok": True,
                "profile_dir": str(resolved_profile_dir),
                "chrome_executable": str(browser_path),
                "target_url": target_url,
                "final_url": page.url,
                "title": page.title(),
                "cookies_count": cookies_count,
                "storage_state_path": str(storage_state_path),
                "report_path": str(report_path),
            }
        finally:
            context.close()
    finally:
        playwright.stop()

    report_path.write_text(json.dumps(result, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return result


def browser_search_items(
    *,
    query: str,
    engine: str = "google",
    profile_dir: str | Path | None = None,
    chrome_executable: str | Path | None = None,
    timeout_ms: int = 30000,
    settle_ms: int = 1200,
) -> dict[str, object]:
    engine_key = str(engine or "google").strip().lower()
    if engine_key != "google":
        raise ValueError(f"unsupported_browser_engine:{engine}")
    resolved_profile_dir = resolve_profile_dir(profile_dir)
    browser_path = resolve_chrome_executable(chrome_executable)
    target_url = build_target_url(url="", engine=engine_key, query=query)

    playwright = sync_playwright().start()
    try:
        try:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(resolved_profile_dir),
                executable_path=str(browser_path),
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
                java_script_enabled=True,
            )
        except Exception as exc:
            return {
                "ok": False,
                "source": "browser_google",
                "state": "browser_boot_failed",
                "error": f"{exc.__class__.__name__}: {exc}",
                "final_url": "",
                "title": "",
            }
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=max(3000, int(timeout_ms)))
            _dismiss_cookie_banner(page)
            if "google." in page.url and "/search?" not in page.url:
                page.goto(target_url, wait_until="domcontentloaded", timeout=max(3000, int(timeout_ms)))
            page.wait_for_timeout(max(0, int(settle_ms)))
            title = page.title()
            final_url = page.url
            if _is_challenge_page(final_url=final_url, title=title):
                return {
                    "ok": False,
                    "source": "browser_google",
                    "state": "challenge",
                    "error": "browser_challenge",
                    "final_url": final_url,
                    "title": title,
                }
            items = _extract_google_search_items(page)
            return {
                "ok": True,
                "source": "browser_google",
                "engine": engine_key,
                "items": items,
                "result_count": len(items),
                "final_url": final_url,
                "title": title,
                "state": "results" if items else "no_results",
            }
        except Exception as exc:
            title = ""
            final_url = ""
            try:
                title = page.title() if page is not None else ""
            except Exception:
                pass
            try:
                final_url = page.url if page is not None else ""
            except Exception:
                pass
            state = "timeout" if "Timeout" in exc.__class__.__name__ else "page_error"
            return {
                "ok": False,
                "source": "browser_google",
                "state": state,
                "error": f"{exc.__class__.__name__}: {exc}",
                "final_url": final_url,
                "title": title,
            }
        finally:
            context.close()
    finally:
        playwright.stop()


def browser_app_store_items(
    *,
    query: str,
    country: str,
    profile_dir: str | Path | None = None,
    chrome_executable: str | Path | None = None,
    timeout_ms: int = 30000,
    settle_ms: int = 1500,
) -> dict[str, object]:
    resolved_profile_dir = resolve_profile_dir(profile_dir)
    browser_path = resolve_chrome_executable(chrome_executable)
    encoded_query = parse.quote_plus(str(query or "").strip())
    country_code = str(country or "").strip().lower() or "us"
    target_url = f"https://apps.apple.com/{country_code}/search?term={encoded_query}"

    playwright = sync_playwright().start()
    try:
        try:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(resolved_profile_dir),
                executable_path=str(browser_path),
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
                java_script_enabled=True,
            )
        except Exception as exc:
            return {
                "ok": False,
                "source": "browser_app_store",
                "country": country_code,
                "state": "browser_boot_failed",
                "error": f"{exc.__class__.__name__}: {exc}",
                "items": [],
                "final_url": "",
                "title": "",
            }
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=max(3000, int(timeout_ms)))
            page.wait_for_timeout(max(0, int(settle_ms)))
            title = page.title()
            final_url = page.url
            items = _extract_app_store_search_items(page)
            return {
                "ok": True,
                "source": "browser_app_store",
                "country": country_code,
                "items": items,
                "result_count": len(items),
                "final_url": final_url,
                "title": title,
                "state": "results" if items else "no_results",
            }
        except Exception as exc:
            title = ""
            final_url = ""
            try:
                title = page.title() if page is not None else ""
            except Exception:
                pass
            try:
                final_url = page.url if page is not None else ""
            except Exception:
                pass
            state = "timeout" if "Timeout" in exc.__class__.__name__ else "page_error"
            return {
                "ok": False,
                "source": "browser_app_store",
                "country": country_code,
                "state": state,
                "error": f"{exc.__class__.__name__}: {exc}",
                "items": [],
                "final_url": final_url,
                "title": title,
            }
        finally:
            context.close()
    finally:
        playwright.stop()
