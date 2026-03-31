from __future__ import annotations

import json
import re
import shutil
import tempfile
import unicodedata
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from urllib import parse

from playwright.sync_api import sync_playwright

from .browser_profile import resolve_chrome_executable, resolve_profile_dir


@dataclass(frozen=True)
class TmviewProbeResult:
    name: str
    url: str
    query_ok: bool
    source: str
    exact_hits: int
    near_hits: int
    result_count: int
    sample_text: str
    query_nice_class: str = ""
    error: str = ""
    exact_sample_text: str = ""
    active_exact_hits: int = 0
    inactive_exact_hits: int = 0
    unknown_exact_hits: int = 0
    state: str = ""
    query_name: str = ""
    normalized_name: str = ""
    query_sequence: str = ""
    surface_exact_hits: int = 0
    normalized_exact_hits: int = 0
    surface_active_exact_hits: int = 0
    normalized_active_exact_hits: int = 0


ACTIVE_STATUS_PATTERNS = (
    "registered",
    "filed",
    "published",
    "pending",
    "accepted",
    "opposed",
    "under examination",
)

INACTIVE_STATUS_PATTERNS = (
    "expired",
    "ended",
    "cancelled",
    "canceled",
    "withdrawn",
    "refused",
    "invalidated",
    "revoked",
    "abandoned",
    "rejected",
    "surrendered",
    "ceased",
    "dead",
)

BODY_RESULT_CONTEXT_TOKENS = (
    "office of origin",
    "trade marks:",
    "goods and services",
    "applicant name",
    "application number",
    "view this trade mark in the office of origin",
    "trade mark office",
)
TMVIEW_HOME_URL = "https://www.tmdn.org/tmview/#/tmview"
TMVIEW_RESULTS_URL = "https://www.tmdn.org/tmview/#/tmview/results"
TMVIEW_DEFAULT_OFFICES = (
    "AL,AT,BA,BG,BX,CH,CY,CZ,DE,DK,EE,ES,FI,FR,GB,GE,GR,HR,HU,IE,IS,IT,LI,LT,LV,MC,MD,ME,MK,MT,NO,PL,PT,RO,RS,RU,SE,SI,SK,SM,UA,EM,WO"
)
TMVIEW_DEFAULT_TERRITORIES = (
    "AT,BE,BG,HR,CY,CZ,DK,EE,FI,FR,DE,GR,HU,IE,IT,LV,LT,LU,MT,NL,PL,PT,RO,SK,SI,ES,SE,AX,AL,AD,BY,BQ,BA,CW,FO,GE,GI,GG,IS,IM,JE,LI,MD,MC,ME,MK,NO,RU,SH,SM,RS,SX,SJ,CH,UA,GB,VA"
)
TMVIEW_DEFAULT_NICE_CLASS = "9,OR,42"
TMVIEW_DEFAULT_TM_STATUS = "Filed,Registered"
TMVIEW_RESULTS_PAGINATION_SELECTOR = '[data-test-id="search-results-pagination"]'
TMVIEW_RESULTS_GRID_SELECTOR = 'div.rt-table[role="grid"]'
TMVIEW_RESULTS_ROW_SELECTOR = '[role="grid"] [role="rowgroup"] [role="row"]'
TMVIEW_RESULTS_CELL_SELECTOR = '[role="gridcell"]'
TMVIEW_BROWSER_CANDIDATES = (
    Path("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
    Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
)
TMVIEW_VOLATILE_PROFILE_NAMES = {
    "SingletonLock",
    "SingletonCookie",
    "SingletonSocket",
    "RunningChromeVersion",
    "BrowserMetrics",
    "ShaderCache",
    "GrShaderCache",
    "GraphiteDawnCache",
    "Crashpad",
    "Crash Reports",
    "Code Cache",
    "GPUCache",
    "Cache",
}


def _fold_ascii_letters(raw: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(raw or ""))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_alpha(raw: str) -> str:
    folded = _fold_ascii_letters(raw).lower()
    return "".join(ch for ch in folded if ch.isalnum())


def _normalize_surface_phrase(raw: str) -> str:
    return re.sub(r"\s+", " ", _fold_ascii_letters(str(raw or "")).strip().lower())


def build_tmview_url(name: str, *, nice_class: str | None = None) -> str:
    search_value = f" {str(name or '').strip()}"
    resolved_nice_class = str(nice_class or TMVIEW_DEFAULT_NICE_CLASS).strip() or TMVIEW_DEFAULT_NICE_CLASS
    params = (
        ("page", "1"),
        ("pageSize", "30"),
        ("criteria", "F"),
        ("offices", TMVIEW_DEFAULT_OFFICES),
        ("territories", TMVIEW_DEFAULT_TERRITORIES),
        ("basicSearch", search_value),
        ("niceClass", resolved_nice_class),
        ("tmStatus", TMVIEW_DEFAULT_TM_STATUS),
    )
    return TMVIEW_RESULTS_URL + "?" + "&".join(
        f"{key}={parse.quote(value, safe=',')}" for key, value in params
    )


def classify_tm_status(raw: str) -> str:
    plain = re.sub(r"\s+", " ", str(raw or "").strip().lower())
    if not plain:
        return "unknown"
    for token in INACTIVE_STATUS_PATTERNS:
        if re.search(rf"(^|[^a-z]){re.escape(token)}([^a-z]|$)", plain):
            return "inactive"
    for token in ACTIVE_STATUS_PATTERNS:
        if re.search(rf"(^|[^a-z]){re.escape(token)}([^a-z]|$)", plain):
            return "active"
    return "unknown"


def _title_exact_or_near(name: str, text: str) -> tuple[bool, bool]:
    mode, near = _title_match_mode(name, normalize_alpha(name), text)
    return (mode != "", near)


def _title_match_mode(display_name: str, normalized_name: str, text: str) -> tuple[str, bool]:
    plain = re.sub(r"\s+", " ", _fold_ascii_letters(str(text or "")).strip().lower())
    if not plain:
        return "", False
    display_plain = _normalize_surface_phrase(display_name)
    normalized = normalize_alpha(plain)
    if display_plain and (
        plain == display_plain
        or re.search(rf"(^|[^a-z0-9]){re.escape(display_plain)}([^a-z0-9]|$)", plain)
    ):
        return "surface_exact", False
    if normalized == normalized_name:
        return "normalized_exact", False
    tokens = set(re.findall(r"[a-z0-9]{4,}", plain))
    for token in tokens:
        if token == normalized_name:
            continue
        ratio = SequenceMatcher(None, token, normalized_name).ratio()
        prefix_len = 0
        for ch1, ch2 in zip(token, normalized_name):
            if ch1 != ch2:
                break
            prefix_len += 1
        if ratio >= 0.85 and abs(len(token) - len(normalized_name)) <= 2:
            return "", True
        if (
            ratio >= 0.77
            and abs(len(token) - len(normalized_name)) <= 2
            and prefix_len >= 3
            and token[-1:] == normalized_name[-1:]
        ):
            return "", True
        if ratio >= 0.82 and abs(len(token) - len(normalized_name)) <= 1 and prefix_len >= 5:
            return "", True
    return "", False


def _parse_result_count(text: str) -> int | None:
    plain = re.sub(r"\s+", " ", str(text or "").strip())
    if not plain:
        return None
    if re.search(r"No\s+rows\s+found", plain, flags=re.IGNORECASE):
        return 0
    patterns = (
        r"\b\d+\s*-\s*\d+\s+of\s+(\d[\d., ]{0,12})\b",
        r"Show\s+all\s+(\d[\d., ]{0,12})\s+results",
        r"(\d[\d., ]{0,12})\s+results",
    )
    for pattern in patterns:
        match = re.search(pattern, plain, flags=re.IGNORECASE)
        if not match:
            continue
        token = re.sub(r"[^0-9]", "", match.group(1))
        if not token:
            continue
        try:
            return int(token)
        except ValueError:
            return 0
    return None


def _has_body_result_context(text: str) -> bool:
    plain = str(text or "").lower()
    return any(token in plain for token in BODY_RESULT_CONTEXT_TOKENS)


def _body_result_segments(body_text: str) -> list[str]:
    plain = re.sub(r"[ \t]+", " ", str(body_text or "").replace("\r", "").replace("\n", " | "))
    segments: list[str] = []
    for raw in plain.split(" | - | ")[1:]:
        segment = raw.strip(" |")
        if not segment:
            continue
        if not _has_body_result_context(segment):
            continue
        segments.append(segment)
    return segments


def _segment_title(segment: str) -> str:
    for token in str(segment or "").split("|"):
        plain = re.sub(r"\s+", " ", token).strip()
        if plain:
            return plain
    return ""


def _resolve_tmview_browser_executable(raw: str | Path | None = None) -> Path:
    if raw:
        return resolve_chrome_executable(raw)
    for candidate in TMVIEW_BROWSER_CANDIDATES:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("tmview_browser_executable_not_found")


def _ignore_tmview_profile_entries(_directory: str, names: list[str]) -> set[str]:
    ignored: set[str] = set()
    for name in names:
        if name in TMVIEW_VOLATILE_PROFILE_NAMES:
            ignored.add(name)
    return ignored


def clone_tmview_runtime_profile(profile_dir: str | Path) -> tuple[Path, Path]:
    source_dir = resolve_profile_dir(profile_dir)
    if not source_dir.exists():
        raise FileNotFoundError(f"tmview_profile_dir_not_found:{source_dir}")
    temp_root = Path(tempfile.mkdtemp(prefix="tmview-profile-"))
    runtime_dir = temp_root / "profile"
    shutil.copytree(
        source_dir,
        runtime_dir,
        symlinks=True,
        ignore=_ignore_tmview_profile_entries,
    )
    return temp_root, runtime_dir


def _empty_match_stats() -> dict[str, object]:
    return {
        "exact_hits": 0,
        "near_hits": 0,
        "samples": [],
        "exact_samples": [],
        "active_exact_hits": 0,
        "inactive_exact_hits": 0,
        "unknown_exact_hits": 0,
        "surface_exact_hits": 0,
        "normalized_exact_hits": 0,
        "surface_active_exact_hits": 0,
        "normalized_active_exact_hits": 0,
    }


def _probe_from_body_segments(display_name: str, normalized_name: str, body_text: str) -> dict[str, object]:
    stats = _empty_match_stats()
    for segment in _body_result_segments(body_text):
        title = _segment_title(segment)
        exact_mode, is_near = _title_match_mode(display_name, normalized_name, title)
        if exact_mode:
            stats["exact_hits"] = int(stats["exact_hits"]) + 1
            if exact_mode == "surface_exact":
                stats["surface_exact_hits"] = int(stats["surface_exact_hits"]) + 1
            else:
                stats["normalized_exact_hits"] = int(stats["normalized_exact_hits"]) + 1
            status = classify_tm_status(segment)
            if status == "active":
                stats["active_exact_hits"] = int(stats["active_exact_hits"]) + 1
                if exact_mode == "surface_exact":
                    stats["surface_active_exact_hits"] = int(stats["surface_active_exact_hits"]) + 1
                else:
                    stats["normalized_active_exact_hits"] = int(stats["normalized_active_exact_hits"]) + 1
            elif status == "inactive":
                stats["inactive_exact_hits"] = int(stats["inactive_exact_hits"]) + 1
            else:
                stats["unknown_exact_hits"] = int(stats["unknown_exact_hits"]) + 1
            if len(stats["exact_samples"]) < 3:
                stats["exact_samples"].append(segment[:180])
        elif is_near:
            stats["near_hits"] = int(stats["near_hits"]) + 1
        if len(stats["samples"]) < 2 and (exact_mode or is_near):
            stats["samples"].append(segment[:180])
    return stats


def _probe_from_grid_rows(display_name: str, normalized_name: str, rows: list[dict[str, str]]) -> dict[str, object]:
    stats = _empty_match_stats()
    for row in rows:
        title = str(row.get("title") or "")
        full_text = str(row.get("text") or title)
        exact_mode, is_near = _title_match_mode(display_name, normalized_name, title)
        if exact_mode:
            stats["exact_hits"] = int(stats["exact_hits"]) + 1
            if exact_mode == "surface_exact":
                stats["surface_exact_hits"] = int(stats["surface_exact_hits"]) + 1
            else:
                stats["normalized_exact_hits"] = int(stats["normalized_exact_hits"]) + 1
            status = classify_tm_status(full_text)
            if status == "active":
                stats["active_exact_hits"] = int(stats["active_exact_hits"]) + 1
                if exact_mode == "surface_exact":
                    stats["surface_active_exact_hits"] = int(stats["surface_active_exact_hits"]) + 1
                else:
                    stats["normalized_active_exact_hits"] = int(stats["normalized_active_exact_hits"]) + 1
            elif status == "inactive":
                stats["inactive_exact_hits"] = int(stats["inactive_exact_hits"]) + 1
            else:
                stats["unknown_exact_hits"] = int(stats["unknown_exact_hits"]) + 1
            if len(stats["exact_samples"]) < 3:
                stats["exact_samples"].append(full_text[:180])
        elif is_near:
            stats["near_hits"] = int(stats["near_hits"]) + 1
        if len(stats["samples"]) < 2 and (exact_mode or is_near):
            stats["samples"].append(full_text[:180])
    return stats


class TmviewProbe:
    def __init__(
        self,
        *,
        timeout_ms: int = 20000,
        settle_ms: int = 2500,
        headless: bool = True,
        profile_dir: str | Path | None = None,
        chrome_executable: str | Path | None = None,
        nice_class: str | None = None,
    ) -> None:
        self.timeout_ms = max(3000, int(timeout_ms))
        self.settle_ms = max(0, int(settle_ms))
        self.headless = bool(headless)
        self.profile_dir = resolve_profile_dir(profile_dir) if profile_dir else None
        self.nice_class = str(nice_class or TMVIEW_DEFAULT_NICE_CLASS).strip() or TMVIEW_DEFAULT_NICE_CLASS
        self.chrome_executable = _resolve_tmview_browser_executable(chrome_executable) if profile_dir else (
            resolve_chrome_executable(chrome_executable) if chrome_executable else None
        )
        self._playwright = None
        self._browser = None
        self._context = None
        self._runtime_profile_root: Path | None = None
        self._runtime_profile_dir: Path | None = None
        self._import_error = ""

    def __enter__(self) -> TmviewProbe:
        try:
            self._playwright = sync_playwright().start()
            if self.profile_dir is not None:
                self._runtime_profile_root, self._runtime_profile_dir = clone_tmview_runtime_profile(self.profile_dir)
                self._context = self._playwright.chromium.launch_persistent_context(
                    user_data_dir=str(self._runtime_profile_dir),
                    executable_path=str(self.chrome_executable) if self.chrome_executable else None,
                    headless=self.headless,
                    args=["--disable-blink-features=AutomationControlled"],
                    java_script_enabled=True,
                )
            else:
                self._browser = self._playwright.chromium.launch(
                    headless=self.headless,
                    executable_path=str(self.chrome_executable) if self.chrome_executable else None,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                self._context = self._browser.new_context(java_script_enabled=True)
                self._context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                )
        except Exception as exc:  # pragma: no cover - env-dependent
            self._import_error = f"playwright_launch_error:{exc.__class__.__name__}"
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - env-dependent
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
        try:
            if self._runtime_profile_root is not None:
                shutil.rmtree(self._runtime_profile_root, ignore_errors=True)
        except Exception:
            pass

    def available(self) -> bool:
        return bool(self._context is not None and not self._import_error)

    def _probe_query(self, *, query_name: str, display_name: str, normalized_name: str) -> TmviewProbeResult:
        query = str(query_name or "").strip()
        normalized = normalize_alpha(normalized_name or display_name)
        url = build_tmview_url(query, nice_class=self.nice_class)
        if not normalized:
            return TmviewProbeResult(
                name=display_name,
                url=url,
                query_ok=False,
                source="tmview_playwright",
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text="",
                query_nice_class=self.nice_class,
                error="invalid_name",
                state="invalid_name",
                query_name=query,
                normalized_name=normalized,
            )
        if not self.available():
            return TmviewProbeResult(
                name=display_name,
                url=url,
                query_ok=False,
                source="tmview_playwright",
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text="",
                query_nice_class=self.nice_class,
                error=self._import_error or "playwright_unavailable",
                state="browser_unavailable",
                query_name=query,
                normalized_name=normalized,
            )

        page = None
        try:
            assert self._context is not None
            page = self._context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            page.wait_for_url(re.compile(r".*/tmview/results.*"), timeout=self.timeout_ms)
            try:
                page.wait_for_function(
                    f"""() => Boolean(
                        document.querySelector('{TMVIEW_RESULTS_PAGINATION_SELECTOR}')
                        || document.querySelector('{TMVIEW_RESULTS_GRID_SELECTOR}')
                        || /No\\s+rows\\s+found/i.test(document.body.innerText || '')
                    )""",
                    timeout=self.timeout_ms,
                )
            except Exception:
                # TMView sometimes renders results without ever exposing the
                # expected pagination hook in time; continue with best-effort
                # extraction instead of hard-failing the whole probe.
                pass
            if self.settle_ms > 0:
                page.wait_for_timeout(self.settle_ms)

            rows: list[dict[str, str]] = []
            body_text = ""
            result_count = -1
            stable_passes = 0
            previous_row_count = -1
            for _ in range(4):
                page.wait_for_timeout(max(900, min(2500, self.settle_ms)))
                try:
                    current_body = page.inner_text("body")
                except Exception:
                    current_body = body_text
                if current_body:
                    body_text = current_body

                try:
                    summary_text = page.locator(TMVIEW_RESULTS_PAGINATION_SELECTOR).first.inner_text(timeout=1500)
                except Exception:
                    summary_text = body_text
                parsed_count = _parse_result_count(summary_text)
                if parsed_count is not None:
                    result_count = parsed_count

                try:
                    extracted = page.evaluate(
                        f"""() => {{
                          const rows = Array.from(document.querySelectorAll('{TMVIEW_RESULTS_ROW_SELECTOR}'));
                          return rows.map((row) => {{
                            const cells = Array.from(row.querySelectorAll('{TMVIEW_RESULTS_CELL_SELECTOR}'));
                            const titleCell = cells[3] || cells[0] || row;
                            const title = (titleCell.innerText || '').replace(/\\s+/g, ' ').trim();
                            const text = (row.innerText || '').replace(/\\s+/g, ' ').trim();
                            return {{ title, text }};
                          }}).filter((item) => item.title || item.text);
                        }}"""
                    )
                except Exception:
                    extracted = []
                rows = []
                seen_rows: set[str] = set()
                if isinstance(extracted, list):
                    for item in extracted:
                        if not isinstance(item, dict):
                            continue
                        title = str(item.get("title") or "").strip()
                        text = str(item.get("text") or "").strip()
                        key = title + "\n" + text
                        if not text or key in seen_rows:
                            continue
                        seen_rows.add(key)
                        rows.append({"title": title, "text": text})

                current_row_count = len(rows)
                if current_row_count == previous_row_count:
                    stable_passes += 1
                else:
                    stable_passes = 0
                previous_row_count = current_row_count
                if result_count == 0 or stable_passes >= 1 or current_row_count >= min(max(result_count, 0), 30):
                    break
                try:
                    page.evaluate(
                        f"""() => {{
                          const rows = Array.from(document.querySelectorAll('{TMVIEW_RESULTS_ROW_SELECTOR}'));
                          const target = rows[Math.min(24, rows.length - 1)];
                          if (target) {{
                            target.scrollIntoView({{ block: 'center' }});
                            return true;
                          }}
                          return false;
                        }}"""
                    )
                except Exception:
                    pass

            if result_count < 0:
                result_count = len(rows)

            stats = _probe_from_grid_rows(display_name, normalized, rows)

            if int(stats["exact_hits"]) == 0 and int(stats["near_hits"]) == 0 and result_count > 0:
                stats = _probe_from_body_segments(display_name, normalized, body_text)

            return TmviewProbeResult(
                name=display_name,
                url=url,
                query_ok=True,
                source="tmview_playwright",
                exact_hits=int(stats["exact_hits"]),
                near_hits=int(stats["near_hits"]),
                result_count=result_count,
                sample_text=" || ".join(stats["samples"]),
                query_nice_class=self.nice_class,
                exact_sample_text=" || ".join(stats["exact_samples"]),
                active_exact_hits=int(stats["active_exact_hits"]),
                inactive_exact_hits=int(stats["inactive_exact_hits"]),
                unknown_exact_hits=int(stats["unknown_exact_hits"]),
                error="",
                state="results" if result_count != 0 else "no_results",
                query_name=query,
                normalized_name=normalized,
                query_sequence=query,
                surface_exact_hits=int(stats["surface_exact_hits"]),
                normalized_exact_hits=int(stats["normalized_exact_hits"]),
                surface_active_exact_hits=int(stats["surface_active_exact_hits"]),
                normalized_active_exact_hits=int(stats["normalized_active_exact_hits"]),
            )
        except Exception as exc:  # pragma: no cover - env-dependent
            state = "timeout" if "Timeout" in exc.__class__.__name__ else "page_error"
            return TmviewProbeResult(
                name=display_name,
                url=url,
                query_ok=False,
                source="tmview_playwright",
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text="",
                query_nice_class=self.nice_class,
                error=f"page_error:{exc.__class__.__name__}",
                state=state,
                query_name=query,
                normalized_name=normalized,
                query_sequence=query,
            )
        finally:
            try:
                if page is not None:
                    page.close()
            except Exception:
                pass

    def probe_name(self, name: str, normalized_name: str | None = None) -> TmviewProbeResult:
        display_name = str(name or "").strip()
        normalized = normalize_alpha(normalized_name or display_name)
        surface_result = self._probe_query(
            query_name=display_name or normalized,
            display_name=display_name or normalized,
            normalized_name=normalized,
        )
        normalized_query = normalized
        if (
            not surface_result.query_ok
            or not normalized_query
            or normalized_query == (display_name or normalized)
            or surface_result.exact_hits > 0
            or surface_result.near_hits > 0
        ):
            return surface_result
        normalized_result = self._probe_query(
            query_name=normalized_query,
            display_name=display_name or normalized_query,
            normalized_name=normalized,
        )
        if not normalized_result.query_ok:
            return TmviewProbeResult(
                **{
                    **surface_result.__dict__,
                    "query_sequence": ",".join(
                        value for value in (surface_result.query_name, normalized_result.query_name) if value
                    ),
                    "error": normalized_result.error or surface_result.error,
                }
            )
        samples = [value for value in (surface_result.sample_text, normalized_result.sample_text) if value]
        exact_samples = [value for value in (surface_result.exact_sample_text, normalized_result.exact_sample_text) if value]
        return TmviewProbeResult(
            name=surface_result.name,
            url=normalized_result.url or surface_result.url,
            query_ok=True,
            source=surface_result.source,
            exact_hits=surface_result.exact_hits + normalized_result.exact_hits,
            near_hits=surface_result.near_hits + normalized_result.near_hits,
            result_count=max(surface_result.result_count, normalized_result.result_count),
            sample_text=" || ".join(dict.fromkeys(samples)),
            query_nice_class=self.nice_class,
            error="",
            exact_sample_text=" || ".join(dict.fromkeys(exact_samples)),
            active_exact_hits=surface_result.active_exact_hits + normalized_result.active_exact_hits,
            inactive_exact_hits=surface_result.inactive_exact_hits + normalized_result.inactive_exact_hits,
            unknown_exact_hits=surface_result.unknown_exact_hits + normalized_result.unknown_exact_hits,
            state="results"
            if max(surface_result.result_count, normalized_result.result_count) > 0
            else "no_results",
            query_name=surface_result.query_name,
            normalized_name=normalized,
            query_sequence=",".join(
                value for value in (surface_result.query_name, normalized_result.query_name) if value
            ),
            surface_exact_hits=surface_result.surface_exact_hits + normalized_result.surface_exact_hits,
            normalized_exact_hits=surface_result.normalized_exact_hits + normalized_result.normalized_exact_hits,
            surface_active_exact_hits=surface_result.surface_active_exact_hits + normalized_result.surface_active_exact_hits,
            normalized_active_exact_hits=surface_result.normalized_active_exact_hits + normalized_result.normalized_active_exact_hits,
        )


def probe_names(
    *,
    names: list[str],
    normalized_names: list[str] | None = None,
    profile_dir: str | Path | None = None,
    chrome_executable: str | Path | None = None,
    nice_class: str | None = None,
    timeout_ms: int = 20000,
    settle_ms: int = 2500,
    headless: bool = True,
) -> list[TmviewProbeResult]:
    prepared_names: list[tuple[str, str]] = []
    seen: set[str] = set()
    raw_normalized = list(normalized_names or [])
    for index, token in enumerate(names):
        display_name = str(token or "").strip()
        normalized = normalize_alpha(raw_normalized[index] if index < len(raw_normalized) else display_name)
        dedupe_key = f"{display_name.casefold()}::{normalized}"
        if not normalized or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        prepared_names.append((display_name or normalized, normalized))
    results: list[TmviewProbeResult] = []
    with TmviewProbe(
        timeout_ms=timeout_ms,
        settle_ms=settle_ms,
        headless=headless,
        profile_dir=profile_dir,
        chrome_executable=chrome_executable,
        nice_class=nice_class,
    ) as probe:
        for display_name, normalized in prepared_names:
            results.append(probe.probe_name(display_name, normalized_name=normalized))
    return results


def write_results_json(path: str | Path, results: list[TmviewProbeResult]) -> Path:
    out_path = Path(path).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(item) for item in results]
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_path
