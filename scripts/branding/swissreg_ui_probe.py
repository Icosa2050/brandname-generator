#!/usr/bin/env python3
"""Swissreg UI probe via Playwright.

Probes the Swissreg database client home search UI and extracts the Marken
counter for a queried name.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class SwissregProbeResult:
    name: str
    url: str
    query_ok: bool
    source: str
    mark_count: int
    error: str


def normalize_alpha(raw: str) -> str:
    return ''.join(ch for ch in str(raw or '').strip().lower() if ch.isalpha())


class SwissregUIProbe:
    HOME_URL = 'https://www.swissreg.ch/database-client/home'

    def __init__(
        self,
        *,
        timeout_ms: int = 20000,
        settle_ms: int = 2500,
        headless: bool = True,
    ) -> None:
        self.timeout_ms = max(3000, int(timeout_ms))
        self.settle_ms = max(0, int(settle_ms))
        self.headless = bool(headless)
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._import_error = ''

    def __enter__(self) -> SwissregUIProbe:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # pragma: no cover - environment-specific
            self._import_error = f'playwright_import_error:{exc.__class__.__name__}'
            return self
        try:
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=self.headless)
            self._context = self._browser.new_context(
                user_agent='brandname-generator-swissreg-probe/1.0',
                java_script_enabled=True,
            )
            self._page = self._context.new_page()
        except Exception as exc:  # pragma: no cover - environment-specific
            self._import_error = f'playwright_launch_error:{exc.__class__.__name__}'
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover
        try:
            if self._context is not None:
                self._context.close()
        except Exception:
            pass
        try:
            if self._browser is not None:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._playwright is not None:
                self._playwright.stop()
        except Exception:
            pass

    def available(self) -> bool:
        return bool(self._page is not None and not self._import_error)

    def _dismiss_cookie_banner(self) -> None:
        if self._page is None:
            return
        # Best effort only.
        for label in (
            'Accept',
            'Akzeptieren',
            'Zustimmen',
            'Alle akzeptieren',
            'Tout accepter',
            'Accetta',
        ):
            try:
                btn = self._page.get_by_role('button', name=re.compile(label, re.IGNORECASE))
                if btn.count() > 0:
                    btn.first.click(timeout=800)
                    return
            except Exception:
                continue

    @staticmethod
    def _extract_mark_count_from_page(page) -> int:
        try:
            value = page.evaluate(
                """() => {
                  const labelHints = ['marken', 'trademarks', 'marques', 'marchi'];
                  const parseIntSafe = (txt) => {
                    const m = (txt || '').replace(/\\u00a0/g, ' ').match(/(\\d[\\d'., ]*)/);
                    if (!m) return -1;
                    const digits = m[1].replace(/[^0-9]/g, '');
                    if (!digits) return -1;
                    const n = Number.parseInt(digits, 10);
                    return Number.isFinite(n) ? n : -1;
                  };
                  let best = -1;
                  const nodes = document.querySelectorAll('*');
                  for (const node of nodes) {
                    const text = (node.innerText || '').replace(/\\s+/g, ' ').trim();
                    if (!text || text.length > 120) continue;
                    const lower = text.toLowerCase();
                    if (!labelHints.some((h) => lower.includes(h))) continue;
                    const count = parseIntSafe(text);
                    if (count > best) best = count;
                  }
                  return best;
                }"""
            )
            if isinstance(value, int):
                return value
            return int(value or -1)
        except Exception:
            return -1

    def probe_name(self, name: str) -> SwissregProbeResult:
        normalized = normalize_alpha(name)
        if not normalized:
            return SwissregProbeResult(
                name='',
                url=self.HOME_URL,
                query_ok=False,
                source='swissreg_playwright',
                mark_count=-1,
                error='invalid_name',
            )
        if not self.available():
            return SwissregProbeResult(
                name=normalized,
                url=self.HOME_URL,
                query_ok=False,
                source='swissreg_playwright',
                mark_count=-1,
                error=self._import_error or 'playwright_unavailable',
            )

        try:
            assert self._page is not None
            self._page.goto(self.HOME_URL, wait_until='domcontentloaded', timeout=self.timeout_ms)
            self._dismiss_cookie_banner()
            self._page.wait_for_timeout(min(self.settle_ms, 1200))

            input_locator = self._page.locator('input[type="search"], input[placeholder*="Search"], input[placeholder*="Suche"], input')
            if input_locator.count() == 0:
                return SwissregProbeResult(
                    name=normalized,
                    url=self.HOME_URL,
                    query_ok=False,
                    source='swissreg_playwright',
                    mark_count=-1,
                    error='search_input_not_found',
                )
            input_box = input_locator.first
            input_box.click(timeout=1500)
            input_box.fill(normalized)
            input_box.press('Enter')

            # Fallback: click search icon/button if present.
            try:
                btn = self._page.get_by_role('button', name=re.compile('search|suche|recherche|ricerca', re.IGNORECASE))
                if btn.count() > 0:
                    btn.first.click(timeout=1200)
            except Exception:
                pass

            self._page.wait_for_timeout(self.settle_ms)
            mark_count = self._extract_mark_count_from_page(self._page)
            if mark_count < 0:
                return SwissregProbeResult(
                    name=normalized,
                    url=self.HOME_URL,
                    query_ok=False,
                    source='swissreg_playwright',
                    mark_count=-1,
                    error='mark_count_not_found',
                )
            return SwissregProbeResult(
                name=normalized,
                url=self.HOME_URL,
                query_ok=True,
                source='swissreg_playwright',
                mark_count=mark_count,
                error='',
            )
        except Exception as exc:  # pragma: no cover - environment-specific
            return SwissregProbeResult(
                name=normalized,
                url=self.HOME_URL,
                query_ok=False,
                source='swissreg_playwright',
                mark_count=-1,
                error=f'page_error:{exc.__class__.__name__}',
            )


def parse_names(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or '').split(','):
        name = normalize_alpha(token)
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Probe Swissreg UI for Marken count.')
    parser.add_argument('--names', default='', help='Comma-separated names.')
    parser.add_argument('--timeout-ms', type=int, default=20000)
    parser.add_argument('--settle-ms', type=int, default=2500)
    parser.add_argument('--headful', action='store_true')
    parser.add_argument('--output-json', default='', help='Optional output JSON file.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    names = parse_names(args.names)
    if not names:
        print('No names provided.')
        return 2

    started = time.monotonic()
    results: list[SwissregProbeResult] = []
    with SwissregUIProbe(
        timeout_ms=args.timeout_ms,
        settle_ms=args.settle_ms,
        headless=not args.headful,
    ) as probe:
        for name in names:
            item = probe.probe_name(name)
            results.append(item)
            print(
                f'swissreg_probe name={name} ok={int(item.query_ok)} marks={item.mark_count} error={item.error or "-"}'
            )
    elapsed_ms = int((time.monotonic() - started) * 1000)
    payload = [asdict(item) for item in results]
    if args.output_json:
        path = Path(args.output_json).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(f'output_json={path}')
    print(f'swissreg_probe_summary total={len(results)} duration_ms={elapsed_ms}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
