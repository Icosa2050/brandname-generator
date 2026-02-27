#!/usr/bin/env python3
"""EUIPO/TMview probe via headless browser automation.

Rationale:
- TMview is JS-driven; plain HTTP fetch does not include rendered result rows.
- This probe drives a browser (Playwright) and extracts query-specific result signals.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from urllib import parse


@dataclass
class EuipoProbeResult:
    name: str
    url: str
    query_ok: bool
    source: str
    exact_hits: int
    near_hits: int
    result_count: int
    sample_text: str
    error: str


def normalize_alpha(raw: str) -> str:
    return ''.join(ch for ch in str(raw or '').lower() if ch.isalpha())


def build_euipo_url(name: str) -> str:
    return (
        'https://www.tmdn.org/tmview/#/tmview/results'
        '?page=1&pageSize=30&criteria=C&basicSearch='
        + parse.quote(name)
    )


def _title_exact_or_near(name: str, text: str) -> tuple[bool, bool]:
    plain = re.sub(r'\s+', ' ', str(text or '').strip().lower())
    if not plain:
        return False, False
    normalized = normalize_alpha(plain)
    if normalized == name:
        return True, False
    if re.search(rf'(^|[^a-z0-9]){re.escape(name)}([^a-z0-9]|$)', plain):
        return True, False
    tokens = set(re.findall(r'[a-z]{4,}', plain))
    for token in tokens:
        if token == name:
            continue
        ratio = SequenceMatcher(None, token, name).ratio()
        if ratio >= 0.86 and abs(len(token) - len(name)) <= 2:
            return False, True
    return False, False


class EuipoProbe:
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
        self._page = None
        self._import_error = ''

    def __enter__(self) -> EuipoProbe:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # pragma: no cover - env-dependent
            self._import_error = f'playwright_import_error:{exc.__class__.__name__}'
            return self
        try:
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(
                headless=self.headless,
                args=['--disable-blink-features=AutomationControlled'],
            )
            context = self._browser.new_context(
                java_script_enabled=True,
            )
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            self._page = context.new_page()
        except Exception as exc:  # pragma: no cover - env-dependent
            self._import_error = f'playwright_launch_error:{exc.__class__.__name__}'
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - env-dependent
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

    def probe_name(self, name: str) -> EuipoProbeResult:
        normalized = normalize_alpha(name)
        url = build_euipo_url(normalized)
        if not normalized:
            return EuipoProbeResult(
                name='',
                url=url,
                query_ok=False,
                source='tmview_playwright',
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text='',
                error='invalid_name',
            )
        if not self.available():
            return EuipoProbeResult(
                name=normalized,
                url=url,
                query_ok=False,
                source='tmview_playwright',
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text='',
                error=self._import_error or 'playwright_unavailable',
            )

        try:
            assert self._page is not None
            self._page.goto(url, wait_until='domcontentloaded', timeout=self.timeout_ms)
            if self.settle_ms > 0:
                self._page.wait_for_timeout(self.settle_ms)
            # TMview hash routes do not always auto-run the query. Force an
            # explicit search interaction to align with UI behavior.
            try:
                search_input = self._page.locator(
                    'input[type="text"], input[type="search"], input[placeholder*="Trade" i], input[placeholder*="mark" i]'
                )
                if search_input.count() > 0:
                    box = search_input.first
                    box.click(timeout=1500)
                    box.fill(normalized)
                    box.press('Enter')
                btn = self._page.get_by_role('button', name=re.compile('search', re.IGNORECASE))
                if btn.count() > 0:
                    btn.first.click(timeout=1500)
            except Exception:
                pass

            # Trigger lazy rendering if needed.
            self._page.mouse.wheel(0, 1200)
            self._page.wait_for_timeout(max(1200, min(3000, self.settle_ms)))

            body_text = self._page.inner_text('body')
        except Exception as exc:  # pragma: no cover - env-dependent
            return EuipoProbeResult(
                name=normalized,
                url=url,
                query_ok=False,
                source='tmview_playwright',
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text='',
                error=f'page_error:{exc.__class__.__name__}',
            )

        # Authoritative negative signal.
        if re.search(r'No\s+rows\s+found', body_text, flags=re.IGNORECASE):
            result_count = 0
        else:
            result_count = -1
        text_patterns = [
            r'Show\s+all\s+(\d[\d., ]{0,12})\s+results',
            r'(\d[\d., ]{0,12})\s+results',
        ]
        for pattern in text_patterns:
            if result_count >= 0:
                break
            match = re.search(pattern, body_text, flags=re.IGNORECASE)
            if not match:
                continue
            token = re.sub(r'[^0-9]', '', match.group(1))
            if not token:
                continue
            try:
                result_count = int(token)
            except ValueError:
                result_count = 0
        if result_count < 0 and re.search(r'No\s+rows\s+found', body_text, flags=re.IGNORECASE):
            result_count = 0
        if result_count < 0:
            result_count = 0

        # Collect candidate result-like rows from broad selectors.
        rows: list[str] = []
        try:
            extracted = self._page.evaluate(
                """() => {
                  const selectors = [
                    'table tbody tr',
                    'div[role="row"]',
                    'li',
                    'article',
                    '.result',
                    '.results',
                    '.search-result'
                  ];
                  const out = [];
                  for (const sel of selectors) {
                    for (const el of document.querySelectorAll(sel)) {
                      const txt = (el.innerText || '').replace(/\\s+/g, ' ').trim();
                      if (!txt) continue;
                      if (txt.length < 6) continue;
                      out.push(txt);
                      if (out.length >= 400) return out;
                    }
                  }
                  return out;
                }"""
            )
            if isinstance(extracted, list):
                rows = [str(item) for item in extracted]
        except Exception:
            rows = []

        exact_hits = 0
        near_hits = 0
        samples: list[str] = []
        for row in rows:
            is_exact, is_near = _title_exact_or_near(normalized, row)
            if is_exact:
                exact_hits += 1
            elif is_near:
                near_hits += 1
            if len(samples) < 2 and (is_exact or is_near):
                samples.append(row[:180])

        # Fallback signal from full body.
        if exact_hits == 0:
            body_norm = normalize_alpha(body_text)
            if normalized and normalized in body_norm:
                # Avoid hard "exact" unless result_count suggests actual result rows.
                if result_count > 0:
                    exact_hits = 1

        return EuipoProbeResult(
            name=normalized,
            url=url,
            query_ok=True,
            source='tmview_playwright',
            exact_hits=exact_hits,
            near_hits=near_hits,
            result_count=result_count,
            sample_text=' || '.join(samples),
            error='',
        )


def parse_names(raw: str) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for token in str(raw or '').split(','):
        name = normalize_alpha(token.strip())
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Probe TMview for candidate names via Playwright.')
    parser.add_argument('--names', default='', help='Comma-separated names.')
    parser.add_argument('--timeout-ms', type=int, default=20000)
    parser.add_argument('--settle-ms', type=int, default=2500)
    parser.add_argument('--headful', action='store_true', help='Run visible browser (debug).')
    parser.add_argument('--output-json', default='', help='Optional output JSON path.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    names = parse_names(args.names)
    if not names:
        print('No names provided.')
        return 2

    results: list[EuipoProbeResult] = []
    started = time.monotonic()
    with EuipoProbe(timeout_ms=args.timeout_ms, settle_ms=args.settle_ms, headless=not args.headful) as probe:
        for name in names:
            result = probe.probe_name(name)
            results.append(result)
            print(
                f'euipo_probe name={name} ok={int(result.query_ok)} exact={result.exact_hits} '
                f'near={result.near_hits} results={result.result_count} error={result.error or "-"}'
            )
    duration_ms = int((time.monotonic() - started) * 1000)
    payload = [asdict(item) for item in results]
    if args.output_json:
        out_path = Path(args.output_json).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(f'output_json={out_path}')
    print(f'euipo_probe_summary total={len(results)} duration_ms={duration_ms}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
