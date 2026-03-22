from __future__ import annotations

import html
import json
import os
import re
import time
from typing import Any
from urllib import error, parse, request

from .browser_profile import browser_search_items
from .models import CandidateResult, ResultStatus, ValidationConfig


USER_AGENT = "brandpipe/1.0"
VALID_NAME_RE = re.compile(r"^[a-z]{6,14}$")


def normalize_name(raw: str) -> str:
    return re.sub(r"[^a-z]", "", str(raw or "").strip().lower())


def normalized_or_fail(name: str) -> str:
    normalized = normalize_name(name)
    if not normalized or not VALID_NAME_RE.fullmatch(normalized):
        raise ValueError(f"invalid_candidate_name:{name!r}")
    return normalized


def _request(url: str, *, timeout: float, method: str = "GET") -> request.Request:
    del timeout
    return request.Request(url, headers={"User-Agent": USER_AGENT}, method=method)


def fetch_json(
    url: str,
    *,
    timeout: float = 8.0,
    retries: int = 1,
    headers: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    req = _request(url, timeout=timeout)
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)
    for attempt in range(max(0, retries) + 1):
        try:
            with request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception:
            if attempt >= retries:
                return None
            time.sleep(0.25 * (attempt + 1))
    return None


def fetch_text(
    url: str,
    *,
    timeout: float = 8.0,
    retries: int = 1,
    headers: dict[str, str] | None = None,
) -> str | None:
    req = _request(url, timeout=timeout)
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)
    for attempt in range(max(0, retries) + 1):
        try:
            with request.urlopen(req, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")
        except Exception:
            if attempt >= retries:
                return None
            time.sleep(0.25 * (attempt + 1))
    return None


def fetch_status(
    url: str,
    *,
    timeout: float = 8.0,
    retries: int = 1,
    method: str = "GET",
    headers: dict[str, str] | None = None,
) -> int | None:
    req = _request(url, timeout=timeout, method=method)
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)
    for attempt in range(max(0, retries) + 1):
        try:
            with request.urlopen(req, timeout=timeout) as response:
                _ = response.read(64)
                return int(response.status)
        except error.HTTPError as exc:
            return int(exc.code)
        except Exception:
            if attempt >= retries:
                return None
            time.sleep(0.25 * (attempt + 1))
    return None


def result(
    *,
    check_name: str,
    status: ResultStatus,
    score_delta: float,
    reason: str,
    details: dict[str, object],
) -> CandidateResult:
    return CandidateResult(
        check_name=check_name,
        status=status,
        score_delta=score_delta,
        reason=reason,
        details=details,
    )


def parse_required_domain_tlds(raw: object) -> tuple[list[str], list[str]]:
    text = str(raw or "").strip().lower()
    if not text:
        return [], []
    allowed = {"com", "de", "ch"}
    resolved: list[str] = []
    invalid: list[str] = []
    for token in [part.strip() for part in text.split(",") if part.strip()]:
        if token not in allowed:
            invalid.append(token)
            continue
        if token not in resolved:
            resolved.append(token)
    return resolved, invalid


def resolve_required_domain_tlds(config: ValidationConfig) -> list[str]:
    resolved, invalid = parse_required_domain_tlds(config.required_domain_tlds)
    if invalid:
        invalid_csv = ",".join(invalid)
        raise ValueError(f"unsupported_required_domain_tlds:{invalid_csv}")
    return resolved


def rdap_available(name: str, tld: str) -> str:
    endpoints = {
        "com": f"https://rdap.verisign.com/com/v1/domain/{name}.com",
        "de": f"https://rdap.denic.de/domain/{name}.de",
        "ch": f"https://rdap.nic.ch/domain/{name}.ch",
    }
    url = endpoints.get(tld)
    if not url:
        return "unknown"
    status = fetch_status(url, timeout=8.0, retries=0)
    if status == 200:
        return "no"
    if status == 404:
        return "yes"
    fallback = rdap_available_fqdn(f"{name}.{tld}")
    if fallback in {"yes", "no"}:
        return fallback
    return "unknown"


def rdap_available_fqdn(fqdn: str) -> str:
    status = fetch_status(f"https://rdap.org/domain/{fqdn}", timeout=8.0, retries=0)
    if status == 200:
        return "no"
    if status == 404:
        return "yes"
    return "unknown"


def check_domain(*, name: str, config: ValidationConfig) -> CandidateResult:
    normalized = normalized_or_fail(name)
    required = resolve_required_domain_tlds(config)
    availability = {tld: rdap_available(normalized, tld) for tld in ["com", "de", "ch"]}
    if required:
        missing = [tld for tld in required if availability.get(tld) == "no"]
        unknown = [tld for tld in required if availability.get(tld) == "unknown"]
        details = {"required": required, "availability": availability, "mode": "explicit_all"}
        if missing:
            return result(
                check_name="domain",
                status=ResultStatus.FAIL,
                score_delta=-18.0,
                reason=f"domain_unavailable_{'-'.join(missing)}",
                details=details,
            )
        if unknown:
            return result(
                check_name="domain",
                status=ResultStatus.UNAVAILABLE,
                score_delta=-4.0,
                reason=f"domain_unknown_{'-'.join(unknown)}",
                details=details,
            )
        return result(check_name="domain", status=ResultStatus.PASS, score_delta=0.0, reason="", details=details)

    default_pool = ["com", "de", "ch"]
    available = [tld for tld in default_pool if availability.get(tld) == "yes"]
    unknown = [tld for tld in default_pool if availability.get(tld) == "unknown"]
    details = {"required": default_pool, "availability": availability, "mode": "default_any"}
    if available:
        return result(check_name="domain", status=ResultStatus.PASS, score_delta=0.0, reason="", details=details)
    if unknown:
        return result(
            check_name="domain",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-4.0,
            reason=f"domain_unknown_{'-'.join(unknown)}",
            details=details,
        )
    return result(
        check_name="domain",
        status=ResultStatus.FAIL,
        score_delta=-18.0,
        reason="domain_unavailable_default_pool",
        details=details,
    )


def package_exists_on_pypi(name: str) -> str:
    status = fetch_status(f"https://pypi.org/pypi/{name}/json", timeout=8.0, retries=1)
    if status == 200:
        return "yes"
    if status == 404:
        return "no"
    return "unknown"


def package_exists_on_npm(name: str) -> str:
    status = fetch_status(f"https://registry.npmjs.org/{name}", timeout=8.0, retries=1)
    if status == 200:
        return "yes"
    if status == 404:
        return "no"
    return "unknown"


def check_package(*, name: str, config: ValidationConfig) -> CandidateResult:
    del config
    normalized = normalized_or_fail(name)
    pypi = package_exists_on_pypi(normalized)
    npm = package_exists_on_npm(normalized)
    collisions = [label for label, value in (("pypi", pypi), ("npm", npm)) if value == "yes"]
    unknown = [label for label, value in (("pypi", pypi), ("npm", npm)) if value == "unknown"]
    details = {"pypi": pypi, "npm": npm}
    if collisions:
        return result(
            check_name="package",
            status=ResultStatus.FAIL,
            score_delta=-10.0,
            reason=f"package_collision_{'-'.join(collisions)}",
            details=details,
        )
    if unknown:
        return result(
            check_name="package",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason=f"package_unknown_{'-'.join(unknown)}",
            details=details,
        )
    return result(check_name="package", status=ResultStatus.PASS, score_delta=0.0, reason="", details=details)


def handle_available(url: str) -> str:
    status = fetch_status(url, timeout=8.0, retries=1, method="GET")
    if status in {404, 410}:
        return "yes"
    if status in {200, 301, 302, 307, 308}:
        return "no"
    return "unknown"


def social_handle_signal(name: str) -> tuple[str, str, str, str, int, int]:
    github = handle_available(f"https://github.com/{name}")
    linkedin = handle_available(f"https://www.linkedin.com/company/{name}")
    x_handle = handle_available(f"https://x.com/{name}")
    instagram = handle_available(f"https://www.instagram.com/{name}/")
    states = [github, linkedin, x_handle, instagram]
    unavailable_count = sum(1 for state in states if state == "no")
    unknown_count = sum(1 for state in states if state == "unknown")
    return github, linkedin, x_handle, instagram, unavailable_count, unknown_count


def check_social(*, name: str, config: ValidationConfig) -> CandidateResult:
    normalized = normalized_or_fail(name)
    github, linkedin, x_handle, instagram, unavailable_count, unknown_count = social_handle_signal(normalized)
    details = {
        "github": github,
        "linkedin": linkedin,
        "x": x_handle,
        "instagram": instagram,
        "unavailable_count": unavailable_count,
        "unknown_count": unknown_count,
    }
    if unavailable_count >= config.social_unavailable_fail_threshold:
        return result(
            check_name="social",
            status=ResultStatus.FAIL,
            score_delta=-8.0,
            reason="social_handle_crowded",
            details=details,
        )
    if unknown_count > 0:
        return result(
            check_name="social",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason="social_check_unknown",
            details=details,
        )
    return result(check_name="social", status=ResultStatus.PASS, score_delta=0.0, reason="", details=details)


def app_store_signal(name: str, country: str) -> tuple[int, bool, bool]:
    url = (
        "https://itunes.apple.com/search?"
        + parse.urlencode({"term": name, "entity": "software", "country": country, "limit": 8})
    )
    data = fetch_json(url)
    if not data:
        return app_store_signal_web(name, country)
    count = int(data.get("resultCount", 0))
    exact = False
    for item in data.get("results", []):
        track = normalize_name(str(item.get("trackName", "")))
        if track == name:
            exact = True
            break
    return count, exact, True


def app_store_signal_web(name: str, country: str) -> tuple[int, bool, bool]:
    slugs: set[str] = set()
    successful_queries = 0
    for platform in ("iphone", "ipad", "mac"):
        url = f"https://apps.apple.com/{country}/{platform}/search?" + parse.urlencode({"term": name})
        html = fetch_text(url, timeout=8.0, retries=1)
        if not html:
            continue
        successful_queries += 1
        slugs.update(
            slug.lower()
            for slug in re.findall(
                r"https://apps\.apple\.com/[a-z]{2}/app/([^/\"\\s?]+)/id\\d+",
                html,
                flags=re.IGNORECASE,
            )
        )
    if successful_queries == 0:
        return -1, False, False
    count = len(slugs)
    exact = any(normalize_name(slug) == name for slug in slugs)
    return count, exact, True


def check_app_store(*, name: str, config: ValidationConfig) -> CandidateResult:
    normalized = normalized_or_fail(name)
    countries = [country.strip().lower() for country in config.store_countries.split(",") if country.strip()]
    exact: list[str] = []
    unknown: list[str] = []
    counts: dict[str, int] = {}
    for country in countries:
        count, is_exact, ok = app_store_signal(normalized, country)
        counts[country] = count
        if is_exact:
            exact.append(country)
        if not ok:
            unknown.append(country)
    details = {"countries": countries, "counts": counts, "exact": exact, "unknown": unknown}
    if exact:
        return result(
            check_name="app_store",
            status=ResultStatus.FAIL,
            score_delta=-18.0,
            reason=f"app_store_exact_collision_{'-'.join(exact)}",
            details=details,
        )
    if unknown:
        return result(
            check_name="app_store",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-3.0,
            reason=f"app_store_unknown_{'-'.join(unknown)}",
            details=details,
        )
    return result(check_name="app_store", status=ResultStatus.PASS, score_delta=0.0, reason="", details=details)


def resolve_web_search_order(config: ValidationConfig) -> list[str]:
    supported = {"brave", "google_cse", "duckduckgo", "browser_google"}
    order: list[str] = []
    for token in [part.strip().lower() for part in config.web_search_order.split(",") if part.strip()]:
        if token in supported and token not in order:
            order.append(token)
    return order or ["brave", "google_cse", "duckduckgo"]


def browser_google_signal(name: str, *, config: ValidationConfig) -> dict[str, object]:
    try:
        payload = browser_search_items(
            query=name,
            engine="google",
            profile_dir=config.web_browser_profile_dir or None,
            chrome_executable=config.web_browser_chrome_executable or None,
            timeout_ms=max(3000, int(float(config.timeout_s) * 1000)),
        )
    except Exception as exc:
        return {
            "ok": False,
            "source": "browser_google",
            "error": f"{exc.__class__.__name__}: {exc}",
        }
    if not bool(payload.get("ok")):
        return payload
    rows = payload.get("items")
    if not isinstance(rows, list):
        return {"ok": False, "source": "browser_google", "error": "browser_items_missing"}
    signal = _analyze_search_items(normalized=name, items=rows, source="browser_google")
    signal["page_title"] = str(payload.get("title") or "")
    signal["final_url"] = str(payload.get("final_url") or "")
    return signal


def brave_search(name: str, *, config: ValidationConfig) -> dict[str, Any] | None:
    api_key = str(
        os.getenv(config.web_brave_api_env)
        or os.getenv("BRAVE_API_KEY")
        or os.getenv("BRAVE_SEARCH_API_KEY")
        or ""
    ).strip()
    if not api_key:
        return None
    query = f'"{name}"'
    url = "https://api.search.brave.com/res/v1/web/search?" + parse.urlencode(
        {
            "q": query,
            "count": str(max(1, min(20, int(config.web_brave_top)))),
            "country": config.web_brave_country,
            "search_lang": config.web_brave_search_lang,
            # Made-up names should not be spell-corrected into existing words.
            "spellcheck": "false",
        }
    )
    return fetch_json(
        url,
        timeout=config.timeout_s,
        retries=1,
        headers={"X-Subscription-Token": api_key, "Accept": "application/json"},
    )


def brave_signal(name: str, *, config: ValidationConfig) -> dict[str, object]:
    payload = brave_search(name, config=config)
    if payload is None:
        return {"ok": False, "source": "brave"}
    web_payload = payload.get("web")
    if not isinstance(web_payload, dict):
        return {"ok": False, "source": "brave", "payload_keys": sorted(payload.keys())[:10]}
    items = web_payload.get("results")
    if not isinstance(items, list):
        return {"ok": False, "source": "brave", "payload_keys": sorted(web_payload.keys())[:10]}
    rows: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "link": str(item.get("url") or item.get("link") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "snippet": str(item.get("description") or item.get("snippet") or "").strip(),
            }
        )
    signal = _analyze_search_items(normalized=name, items=rows, source="brave")
    signal["reported_result_count"] = int(web_payload.get("total", 0) or 0)
    return signal


def google_cse_search(name: str, *, config: ValidationConfig) -> dict[str, Any] | None:
    api_key = str(os.getenv(config.web_google_api_env) or "").strip()
    cx = str(os.getenv(config.web_google_cx_env) or "").strip()
    if not api_key or not cx:
        return None
    query = f'"{name}"'
    url = "https://customsearch.googleapis.com/customsearch/v1?" + parse.urlencode(
        {
            "key": api_key,
            "cx": cx,
            "q": query,
            "num": str(max(1, min(10, int(config.web_google_top)))),
            "gl": config.web_google_gl,
            "hl": config.web_google_hl,
        }
    )
    return fetch_json(url, timeout=config.timeout_s, retries=1)


def _analyze_search_items(*, normalized: str, items: list[dict[str, object]], source: str) -> dict[str, object]:
    exact_hits = 0
    near_hits = 0
    sample_domains: list[str] = []
    first_hit_exact = False
    for index, item in enumerate(items):
        link = str(item.get("link") or "").strip()
        title = str(item.get("title") or "").strip()
        snippet = str(item.get("snippet") or "").strip()
        domain = parse.urlparse(link).netloc.lower()
        label = _domain_label(domain)
        title_token = normalize_name(title)
        snippet_token = normalize_name(snippet)
        if domain and domain not in sample_domains:
            sample_domains.append(domain)
        is_exact = label == normalized or title_token == normalized
        is_near = normalized in label or normalized in title_token or normalized in snippet_token
        if is_exact:
            exact_hits += 1
            if index == 0:
                first_hit_exact = True
        elif is_near:
            near_hits += 1
    return {
        "ok": True,
        "exact_hits": exact_hits,
        "near_hits": near_hits,
        "result_count": len(items),
        "sample_domains": sample_domains[:8],
        "first_hit_exact": first_hit_exact,
        "source": source,
    }


def google_cse_signal(name: str, *, config: ValidationConfig) -> dict[str, object]:
    payload = google_cse_search(name, config=config)
    if payload is None:
        return {"ok": False, "source": "google_cse"}
    items = payload.get("items")
    if not isinstance(items, list):
        return {"ok": False, "source": "google_cse", "payload_keys": sorted(payload.keys())[:10]}
    rows: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "link": str(item.get("link") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "snippet": str(item.get("snippet") or "").strip(),
            }
        )
    return _analyze_search_items(normalized=name, items=rows, source="google_cse")


def extract_result_domain(raw_href: str) -> str:
    href = html.unescape(raw_href)
    if href.startswith("//"):
        href = f"https:{href}"
    if "duckduckgo.com/l/?" in href:
        try:
            parsed = parse.urlparse(href)
            params = parse.parse_qs(parsed.query)
            target = params.get("uddg", [""])[0]
            if target:
                href = parse.unquote(target)
        except Exception:
            return ""
    try:
        parsed = parse.urlparse(href if "://" in href else f"https://{href}")
    except Exception:
        return ""
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def parse_ddg_results(page: str) -> list[tuple[str, str]]:
    return re.findall(
        r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        page,
        re.IGNORECASE | re.DOTALL,
    )


def duckduckgo_search_signal(name: str, *, config: ValidationConfig) -> dict[str, object]:
    url = "https://html.duckduckgo.com/html/?" + parse.urlencode({"q": f'"{name}"'})
    page = fetch_text(url, timeout=config.timeout_s, retries=1)
    if not page:
        return {"ok": False, "source": "duckduckgo"}
    rows: list[dict[str, object]] = []
    for href, title_html in parse_ddg_results(page):
        rows.append(
            {
                "link": f"https://{extract_result_domain(href)}",
                "title": re.sub(r"<[^>]+>", " ", html.unescape(title_html)),
                "snippet": "",
            }
        )
    return _analyze_search_items(normalized=name, items=rows, source="duckduckgo")


def _domain_label(domain: str) -> str:
    token = str(domain or "").strip().lower()
    if not token:
        return ""
    if token.startswith("www."):
        token = token[4:]
    return normalize_name(token.split(".", 1)[0])


def check_web(*, name: str, config: ValidationConfig) -> CandidateResult:
    normalized = normalized_or_fail(name)
    provider_signals = {
        "brave": brave_signal,
        "google_cse": google_cse_signal,
        "duckduckgo": duckduckgo_search_signal,
        "browser_google": browser_google_signal,
    }
    signals: list[dict[str, object]] = []
    selected: dict[str, object] | None = None
    for provider_name in resolve_web_search_order(config):
        runner = provider_signals[provider_name]
        signal = runner(normalized, config=config)
        signals.append(signal)
        if bool(signal.get("ok")):
            selected = signal
            break
    if selected is None:
        return result(
            check_name="web",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason="web_search_unavailable",
            details={"sources": signals},
        )

    exact_hits = int(selected.get("exact_hits", 0))
    near_hits = int(selected.get("near_hits", 0))
    first_hit_exact = bool(selected.get("first_hit_exact"))
    sample_domains = [str(domain).strip() for domain in selected.get("sample_domains", []) if str(domain).strip()]
    details = {
        "exact_hits": exact_hits,
        "near_hits": near_hits,
        "result_count": int(selected.get("result_count", 0)),
        "reported_result_count": int(selected.get("reported_result_count", 0)),
        "sample_domains": sample_domains[:8],
        "first_hit_exact": first_hit_exact,
        "provider": str(selected.get("source") or ""),
        "final_url": str(selected.get("final_url") or ""),
        "page_title": str(selected.get("page_title") or ""),
        "sources": signals,
    }
    if first_hit_exact:
        return result(
            check_name="web",
            status=ResultStatus.FAIL,
            score_delta=-24.0,
            reason="web_first_hit_exact",
            details=details,
        )
    if exact_hits >= 1:
        return result(
            check_name="web",
            status=ResultStatus.FAIL,
            score_delta=-20.0,
            reason="web_exact_collision",
            details=details,
        )
    if near_hits >= 3:
        return result(
            check_name="web",
            status=ResultStatus.FAIL,
            score_delta=-10.0,
            reason="web_near_collision",
            details=details,
        )
    if near_hits >= 2:
        return result(
            check_name="web",
            status=ResultStatus.WARN,
            score_delta=-4.0,
            reason="web_near_warning",
            details=details,
        )
    return result(check_name="web", status=ResultStatus.PASS, score_delta=0.0, reason="", details=details)


def unsupported_result(check_name: str, reason: str) -> CandidateResult:
    return result(
        check_name=check_name,
        status=ResultStatus.UNSUPPORTED,
        score_delta=-2.0,
        reason=reason,
        details={"supported": False},
    )


def check_company(*, name: str, config: ValidationConfig) -> CandidateResult:
    del name, config
    return unsupported_result("company", "company_check_unavailable")


def check_tm(*, name: str, config: ValidationConfig) -> CandidateResult:
    del name, config
    return unsupported_result("tm", "tm_check_unavailable")


def check_tm_cheap(*, name: str, config: ValidationConfig) -> CandidateResult:
    del name, config
    return unsupported_result("tm_cheap", "tm_cheap_check_unavailable")
