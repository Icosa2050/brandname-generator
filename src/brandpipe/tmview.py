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
    error: str = ""
    exact_sample_text: str = ""
    active_exact_hits: int = 0
    inactive_exact_hits: int = 0
    unknown_exact_hits: int = 0


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
TMVIEW_DEFAULT_NICE_CLASS = "9,OR,42,OR,EMPTY"
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
    return "".join(ch for ch in folded if ch.isalpha())


def build_tmview_url(name: str) -> str:
    search_value = f" {str(name or '').strip()}"
    params = (
        ("page", "1"),
        ("pageSize", "30"),
        ("criteria", "F"),
        ("offices", TMVIEW_DEFAULT_OFFICES),
        ("territories", TMVIEW_DEFAULT_TERRITORIES),
        ("basicSearch", search_value),
        ("niceClass", TMVIEW_DEFAULT_NICE_CLASS),
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
    plain = re.sub(r"\s+", " ", _fold_ascii_letters(str(text or "")).strip().lower())
    if not plain:
        return False, False
    normalized = normalize_alpha(plain)
    if normalized == name:
        return True, False
    if re.search(rf"(^|[^a-z0-9]){re.escape(name)}([^a-z0-9]|$)", plain):
        return True, False
    tokens = set(re.findall(r"[a-z]{4,}", plain))
    for token in tokens:
        if token == name:
            continue
        ratio = SequenceMatcher(None, token, name).ratio()
        prefix_len = 0
        for ch1, ch2 in zip(token, name):
            if ch1 != ch2:
                break
            prefix_len += 1
        if ratio >= 0.85 and abs(len(token) - len(name)) <= 2:
            return False, True
        if ratio >= 0.77 and abs(len(token) - len(name)) <= 2 and prefix_len >= 3 and token[-1:] == name[-1:]:
            return False, True
        if ratio >= 0.82 and abs(len(token) - len(name)) <= 1 and prefix_len >= 5:
            return False, True
    return False, False


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


def _probe_from_body_segments(name: str, body_text: str) -> tuple[int, int, list[str], list[str], int, int, int]:
    exact_hits = 0
    near_hits = 0
    samples: list[str] = []
    exact_samples: list[str] = []
    active_exact_hits = 0
    inactive_exact_hits = 0
    unknown_exact_hits = 0
    for segment in _body_result_segments(body_text):
        title = _segment_title(segment)
        is_exact, is_near = _title_exact_or_near(name, title)
        if is_exact:
            exact_hits += 1
            status = classify_tm_status(segment)
            if status == "active":
                active_exact_hits += 1
            elif status == "inactive":
                inactive_exact_hits += 1
            else:
                unknown_exact_hits += 1
            if len(exact_samples) < 3:
                exact_samples.append(segment[:180])
        elif is_near:
            near_hits += 1
        if len(samples) < 2 and (is_exact or is_near):
            samples.append(segment[:180])
    return (
        exact_hits,
        near_hits,
        samples,
        exact_samples,
        active_exact_hits,
        inactive_exact_hits,
        unknown_exact_hits,
    )


def _probe_from_grid_rows(name: str, rows: list[dict[str, str]]) -> tuple[int, int, list[str], list[str], int, int, int]:
    exact_hits = 0
    near_hits = 0
    samples: list[str] = []
    exact_samples: list[str] = []
    active_exact_hits = 0
    inactive_exact_hits = 0
    unknown_exact_hits = 0
    for row in rows:
        title = str(row.get("title") or "")
        full_text = str(row.get("text") or title)
        is_exact, is_near = _title_exact_or_near(name, title)
        if is_exact:
            exact_hits += 1
            status = classify_tm_status(full_text)
            if status == "active":
                active_exact_hits += 1
            elif status == "inactive":
                inactive_exact_hits += 1
            else:
                unknown_exact_hits += 1
            if len(exact_samples) < 3:
                exact_samples.append(full_text[:180])
        elif is_near:
            near_hits += 1
        if len(samples) < 2 and (is_exact or is_near):
            samples.append(full_text[:180])
    return (
        exact_hits,
        near_hits,
        samples,
        exact_samples,
        active_exact_hits,
        inactive_exact_hits,
        unknown_exact_hits,
    )


class TmviewProbe:
    def __init__(
        self,
        *,
        timeout_ms: int = 20000,
        settle_ms: int = 2500,
        headless: bool = True,
        profile_dir: str | Path | None = None,
        chrome_executable: str | Path | None = None,
    ) -> None:
        self.timeout_ms = max(3000, int(timeout_ms))
        self.settle_ms = max(0, int(settle_ms))
        self.headless = bool(headless)
        self.profile_dir = resolve_profile_dir(profile_dir) if profile_dir else None
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

    def probe_name(self, name: str) -> TmviewProbeResult:
        normalized = normalize_alpha(name)
        url = build_tmview_url(normalized)
        if not normalized:
            return TmviewProbeResult(
                name="",
                url=url,
                query_ok=False,
                source="tmview_playwright",
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text="",
                error="invalid_name",
            )
        if not self.available():
            return TmviewProbeResult(
                name=normalized,
                url=url,
                query_ok=False,
                source="tmview_playwright",
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text="",
                error=self._import_error or "playwright_unavailable",
            )

        page = None
        try:
            assert self._context is not None
            page = self._context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            page.wait_for_url(re.compile(r".*/tmview/results.*"), timeout=self.timeout_ms)
            page.wait_for_function(
                f"""() => Boolean(
                    document.querySelector('{TMVIEW_RESULTS_PAGINATION_SELECTOR}')
                    || document.querySelector('{TMVIEW_RESULTS_GRID_SELECTOR}')
                    || /No\\s+rows\\s+found/i.test(document.body.innerText || '')
                )""",
                timeout=self.timeout_ms,
            )
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
                if re.search(r"No\s+rows\s+found", summary_text, flags=re.IGNORECASE):
                    result_count = 0
                if result_count < 0:
                    for pattern in (r"Show\s+all\s+(\d[\d., ]{0,12})\s+results", r"(\d[\d., ]{0,12})\s+results"):
                        match = re.search(pattern, summary_text, flags=re.IGNORECASE)
                        if not match:
                            continue
                        token = re.sub(r"[^0-9]", "", match.group(1))
                        if token:
                            try:
                                result_count = int(token)
                            except ValueError:
                                result_count = 0
                            break

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

            (
                exact_hits,
                near_hits,
                samples,
                exact_samples,
                active_exact_hits,
                inactive_exact_hits,
                unknown_exact_hits,
            ) = _probe_from_grid_rows(normalized, rows)

            if exact_hits == 0 and near_hits == 0 and result_count > 0:
                (
                    exact_hits,
                    near_hits,
                    samples,
                    exact_samples,
                    active_exact_hits,
                    inactive_exact_hits,
                    unknown_exact_hits,
                ) = _probe_from_body_segments(normalized, body_text)

            return TmviewProbeResult(
                name=normalized,
                url=url,
                query_ok=True,
                source="tmview_playwright",
                exact_hits=exact_hits,
                near_hits=near_hits,
                result_count=result_count,
                sample_text=" || ".join(samples),
                exact_sample_text=" || ".join(exact_samples),
                active_exact_hits=active_exact_hits,
                inactive_exact_hits=inactive_exact_hits,
                unknown_exact_hits=unknown_exact_hits,
                error="",
            )
        except Exception as exc:  # pragma: no cover - env-dependent
            return TmviewProbeResult(
                name=normalized,
                url=url,
                query_ok=False,
                source="tmview_playwright",
                exact_hits=-1,
                near_hits=-1,
                result_count=-1,
                sample_text="",
                error=f"page_error:{exc.__class__.__name__}",
            )
        finally:
            try:
                if page is not None:
                    page.close()
            except Exception:
                pass


def probe_names(
    *,
    names: list[str],
    profile_dir: str | Path | None = None,
    chrome_executable: str | Path | None = None,
    timeout_ms: int = 20000,
    settle_ms: int = 2500,
    headless: bool = True,
) -> list[TmviewProbeResult]:
    normalized_names: list[str] = []
    seen: set[str] = set()
    for token in names:
        name = normalize_alpha(token)
        if not name or name in seen:
            continue
        seen.add(name)
        normalized_names.append(name)
    results: list[TmviewProbeResult] = []
    with TmviewProbe(
        timeout_ms=timeout_ms,
        settle_ms=settle_ms,
        headless=headless,
        profile_dir=profile_dir,
        chrome_executable=chrome_executable,
    ) as probe:
        for name in normalized_names:
            results.append(probe.probe_name(name))
    return results


def write_results_json(path: str | Path, results: list[TmviewProbeResult]) -> Path:
    out_path = Path(path).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(item) for item in results]
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_path
