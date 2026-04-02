from __future__ import annotations

import base64
import os
import re
from typing import Any
from urllib import parse

from .browser_profile import browser_app_store_items
from .http_client import HttpResponse, fetch_json, fetch_status
from .models import CandidateResult, ErrorKind, ResultStatus, ValidationConfig
from .name_normalization import fold_brand_text, normalize_brand_token
from .validation_runtime import ProbeResult


EXPLICIT_DOMAIN_RE = re.compile(r"^(?P<label>[a-z0-9-]{2,63})\.(?P<tld>[a-z]{2,24})$", re.IGNORECASE)
TMVIEW_TIMEOUT_FLOOR_MS = 15000
COMPANY_LEGAL_SUFFIX_TOKENS = frozenset(
    {
        "gmbh",
        "ag",
        "llc",
        "ltd",
        "limited",
        "inc",
        "corp",
        "corporation",
        "company",
        "co",
        "sa",
        "sarl",
        "bv",
        "kg",
        "ug",
        "plc",
    }
)


def normalize_name(raw: str) -> str:
    return normalize_brand_token(raw)


def normalized_or_fail(name: str, *, config: ValidationConfig | None = None) -> str:
    active_config = config or ValidationConfig()
    normalized = normalize_name(name)
    shape = active_config.name_shape_policy
    if not normalized:
        raise ValueError(f"invalid_candidate_name:{name!r}")
    if not (int(shape.min_length) <= len(normalized) <= int(shape.max_length)):
        raise ValueError(f"invalid_candidate_name:{name!r}")
    if not bool(shape.allow_digits) and re.search(r"\d", normalized):
        raise ValueError(f"invalid_candidate_name:{name!r}")
    if bool(shape.require_letter) and not re.search(r"[a-z]", normalized):
        raise ValueError(f"invalid_candidate_name:{name!r}")
    return normalized


def display_name(name: str) -> str:
    return str(name or "").strip()


def explicit_domain_parts(name: str) -> tuple[str, str] | None:
    match = EXPLICIT_DOMAIN_RE.fullmatch(display_name(name).lower())
    if not match:
        return None
    return str(match.group("label")), str(match.group("tld"))


def package_query_name(name: str, normalized: str) -> str:
    surface = fold_brand_text(display_name(name)).lower()
    if surface and " " not in surface:
        cleaned = re.sub(r"[^a-z0-9._-]", "", surface)
        if cleaned:
            return cleaned
    return normalized


def social_query_name(name: str, normalized: str) -> str:
    surface = re.sub(r"[^a-z0-9-]", "", fold_brand_text(display_name(name)).lower())
    return surface or normalized


def _int_or_zero(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _candidate_result(
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


def _probe_result(
    *,
    check_name: str,
    status: ResultStatus,
    score_delta: float,
    reason: str,
    details: dict[str, object],
    error_kind: ErrorKind = ErrorKind.NONE,
    retryable: bool = False,
    http_status: int | None = None,
    headers: dict[str, str] | None = None,
    retry_after_s: float | None = None,
    transport: str = "",
    evidence: dict[str, object] | None = None,
) -> ProbeResult:
    merged_details = dict(details)
    merged_details.setdefault("error_kind", error_kind.value)
    merged_details.setdefault("retryable", bool(retryable))
    if http_status is not None:
        merged_details.setdefault("http_status", int(http_status))
    if retry_after_s is not None:
        merged_details.setdefault("retry_after_s", float(retry_after_s))
    if headers:
        merged_details.setdefault("http_headers", dict(headers))
    if evidence:
        merged_details.setdefault("evidence", dict(evidence))
    if transport:
        merged_details.setdefault("transport", transport)
    return ProbeResult(
        candidate_result=_candidate_result(
            check_name=check_name,
            status=status,
            score_delta=score_delta,
            reason=reason,
            details=merged_details,
        ),
        error_kind=error_kind,
        retryable=bool(retryable),
        http_status=http_status,
        headers=dict(headers or {}),
        retry_after_s=retry_after_s,
        transport=transport,
        evidence=dict(evidence or {}),
    )


def _probe_from_http_unavailable(
    *,
    check_name: str,
    reason: str,
    response: HttpResponse,
    score_delta: float,
    details: dict[str, object],
    transport: str,
) -> ProbeResult:
    return _probe_result(
        check_name=check_name,
        status=ResultStatus.UNAVAILABLE,
        score_delta=score_delta,
        reason=reason,
        details=details,
        error_kind=response.error_kind,
        retryable=bool(response.retryable),
        http_status=response.status_code,
        headers=response.headers,
        retry_after_s=response.retry_after_s,
        transport=transport,
    )


def _browser_error_kind(raw_error: object) -> ErrorKind:
    token = str(raw_error or "").strip().lower()
    if not token:
        return ErrorKind.UNEXPECTED
    if "challenge" in token or "captcha" in token:
        return ErrorKind.CHALLENGE
    if "timeout" in token:
        return ErrorKind.TIMEOUT
    if "not_found" in token or "chrome_executable_not_found" in token:
        return ErrorKind.CONFIG
    if "parse" in token or "items_missing" in token:
        return ErrorKind.PARSE
    if "browser" in token or "playwright" in token:
        return ErrorKind.BROWSER
    return ErrorKind.UNEXPECTED


def _error_kind_from_token(raw: object) -> ErrorKind | None:
    token = str(raw or "").strip()
    if not token:
        return None
    try:
        return ErrorKind(token)
    except ValueError:
        return None


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


def _rdap_url(name: str, tld: str) -> str:
    endpoints = {
        "com": f"https://rdap.verisign.com/com/v1/domain/{name}.com",
        "de": f"https://rdap.denic.de/domain/{name}.de",
        "ch": f"https://rdap.nic.ch/domain/{name}.ch",
    }
    return endpoints.get(tld, f"https://rdap.org/domain/{name}.{tld}")


def rdap_available(name: str, tld: str) -> str:
    return rdap_probe(name, tld)["availability"]


def rdap_probe(name: str, tld: str) -> dict[str, object]:
    response = fetch_status(_rdap_url(name, tld), timeout=8.0)
    availability = "unknown"
    if response.status_code == 200:
        availability = "no"
    elif response.status_code == 404:
        availability = "yes"
    return {
        "availability": availability,
        "status_code": response.status_code,
        "error_kind": response.error_kind.value,
        "headers": response.headers,
        "retry_after_s": response.retry_after_s,
        "retryable": bool(response.retryable),
    }


def probe_domain(*, name: str, config: ValidationConfig) -> ProbeResult:
    normalized = normalized_or_fail(name, config=config)
    required = resolve_required_domain_tlds(config)
    explicit_domain = explicit_domain_parts(name)
    candidate_tlds = [explicit_domain[1]] if explicit_domain else ["com", "de", "ch"]
    rdap_name = explicit_domain[0] if explicit_domain else normalized
    probes = {tld: rdap_probe(rdap_name, tld) for tld in candidate_tlds}
    availability = {tld: str(payload["availability"]) for tld, payload in probes.items()}
    retryable_tlds = [tld for tld, payload in probes.items() if payload["availability"] == "unknown" and payload["retryable"]]
    details = {
        "required": required or candidate_tlds,
        "availability": availability,
        "http_statuses": {tld: payload["status_code"] for tld, payload in probes.items()},
        "error_kinds": {tld: payload["error_kind"] for tld, payload in probes.items()},
        "mode": "surface_exact" if explicit_domain else ("explicit_all" if required else "default_any"),
        "query": display_name(name),
        "query_name": rdap_name,
    }
    if explicit_domain and not required:
        exact_tld = explicit_domain[1]
        availability_value = availability.get(exact_tld, "unknown")
        if availability_value == "no":
            return _probe_result(
                check_name="domain",
                status=ResultStatus.FAIL,
                score_delta=-18.0,
                reason=f"domain_unavailable_{exact_tld}",
                details=details,
            )
        if availability_value == "unknown":
            retry_tld = retryable_tlds[0] if retryable_tlds else None
            return _probe_result(
                check_name="domain",
                status=ResultStatus.UNAVAILABLE,
                score_delta=-4.0,
                reason=f"domain_unknown_{exact_tld}",
                details=details,
                error_kind=ErrorKind.TRANSPORT if retry_tld else ErrorKind.NONE,
                retryable=bool(retry_tld),
                http_status=probes.get(retry_tld, {}).get("status_code") if retry_tld else None,
                headers=probes.get(retry_tld, {}).get("headers") if retry_tld else None,
                retry_after_s=probes.get(retry_tld, {}).get("retry_after_s") if retry_tld else None,
                transport="rdap",
            )
        return _probe_result(
            check_name="domain",
            status=ResultStatus.PASS,
            score_delta=0.0,
            reason="",
            details=details,
        )
    if required:
        missing = [tld for tld in required if availability.get(tld) == "no"]
        unknown = [tld for tld in required if availability.get(tld) == "unknown"]
        if missing:
            return _probe_result(
                check_name="domain",
                status=ResultStatus.FAIL,
                score_delta=-18.0,
                reason=f"domain_unavailable_{'-'.join(missing)}",
                details=details,
            )
        if unknown:
            retry_tld = retryable_tlds[0] if retryable_tlds else None
            return _probe_result(
                check_name="domain",
                status=ResultStatus.UNAVAILABLE,
                score_delta=-4.0,
                reason=f"domain_unknown_{'-'.join(unknown)}",
                details=details,
                error_kind=ErrorKind.TRANSPORT if retry_tld else ErrorKind.NONE,
                retryable=bool(retry_tld),
                http_status=probes.get(retry_tld, {}).get("status_code") if retry_tld else None,
                headers=probes.get(retry_tld, {}).get("headers") if retry_tld else None,
                retry_after_s=probes.get(retry_tld, {}).get("retry_after_s") if retry_tld else None,
                transport="rdap",
            )
        return _probe_result(
            check_name="domain",
            status=ResultStatus.PASS,
            score_delta=0.0,
            reason="",
            details=details,
        )

    available = [tld for tld, value in availability.items() if value == "yes"]
    unknown = [tld for tld, value in availability.items() if value == "unknown"]
    if available:
        return _probe_result(
            check_name="domain",
            status=ResultStatus.PASS,
            score_delta=0.0,
            reason="",
            details=details,
        )
    if unknown:
        retry_tld = retryable_tlds[0] if retryable_tlds else None
        return _probe_result(
            check_name="domain",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-4.0,
            reason=f"domain_unknown_{'-'.join(unknown)}",
            details=details,
            error_kind=ErrorKind.TRANSPORT if retry_tld else ErrorKind.NONE,
            retryable=bool(retry_tld),
            http_status=probes.get(retry_tld, {}).get("status_code") if retry_tld else None,
            headers=probes.get(retry_tld, {}).get("headers") if retry_tld else None,
            retry_after_s=probes.get(retry_tld, {}).get("retry_after_s") if retry_tld else None,
            transport="rdap",
        )
    return _probe_result(
        check_name="domain",
        status=ResultStatus.FAIL,
        score_delta=-18.0,
        reason="domain_unavailable_default_pool",
        details=details,
    )


def package_exists_on_pypi(name: str) -> str:
    return package_probe("pypi", name)["exists"]


def package_exists_on_npm(name: str) -> str:
    return package_probe("npm", name)["exists"]


def package_probe(registry: str, name: str) -> dict[str, object]:
    if registry == "pypi":
        response = fetch_status(f"https://pypi.org/pypi/{name}/json", timeout=8.0)
    else:
        response = fetch_status(f"https://registry.npmjs.org/{name}", timeout=8.0)
    exists = "unknown"
    if response.status_code == 200:
        exists = "yes"
    elif response.status_code == 404:
        exists = "no"
    return {
        "exists": exists,
        "status_code": response.status_code,
        "error_kind": response.error_kind.value,
        "headers": response.headers,
        "retry_after_s": response.retry_after_s,
        "retryable": bool(response.retryable),
    }


def probe_package(*, name: str, config: ValidationConfig) -> ProbeResult:
    normalized = normalized_or_fail(name, config=config)
    query_name = package_query_name(name, normalized)
    probes = {
        "pypi": package_probe("pypi", query_name),
        "npm": package_probe("npm", query_name),
    }
    collisions = [label for label, payload in probes.items() if payload["exists"] == "yes"]
    unknown = [label for label, payload in probes.items() if payload["exists"] == "unknown"]
    retryable = [label for label, payload in probes.items() if payload["exists"] == "unknown" and payload["retryable"]]
    details = {
        "query": display_name(name),
        "query_name": query_name,
        "pypi": probes["pypi"]["exists"],
        "npm": probes["npm"]["exists"],
        "http_statuses": {label: payload["status_code"] for label, payload in probes.items()},
        "error_kinds": {label: payload["error_kind"] for label, payload in probes.items()},
    }
    if collisions:
        return _probe_result(
            check_name="package",
            status=ResultStatus.FAIL,
            score_delta=-10.0,
            reason=f"package_collision_{'-'.join(collisions)}",
            details=details,
        )
    if unknown:
        retry_label = retryable[0] if retryable else None
        return _probe_result(
            check_name="package",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason=f"package_unknown_{'-'.join(unknown)}",
            details=details,
            error_kind=ErrorKind.TRANSPORT if retry_label else ErrorKind.NONE,
            retryable=bool(retry_label),
            http_status=probes.get(retry_label, {}).get("status_code") if retry_label else None,
            headers=probes.get(retry_label, {}).get("headers") if retry_label else None,
            retry_after_s=probes.get(retry_label, {}).get("retry_after_s") if retry_label else None,
            transport="package_registry",
        )
    return _probe_result(
        check_name="package",
        status=ResultStatus.PASS,
        score_delta=0.0,
        reason="",
        details=details,
    )


def handle_available(url: str) -> str:
    return social_handle_probe(url)["availability"]


def social_handle_probe(url: str) -> dict[str, object]:
    response = fetch_status(url, timeout=8.0)
    availability = "unknown"
    if response.status_code in {404, 410}:
        availability = "yes"
    elif response.status_code in {200, 301, 302, 307, 308}:
        availability = "no"
    return {
        "availability": availability,
        "status_code": response.status_code,
        "error_kind": response.error_kind.value,
        "headers": response.headers,
        "retry_after_s": response.retry_after_s,
        "retryable": bool(response.retryable),
    }


def social_handle_signal(name: str) -> tuple[str, str, str, str, int, int]:
    github = handle_available(f"https://github.com/{name}")
    linkedin = handle_available(f"https://www.linkedin.com/company/{name}")
    x_handle = handle_available(f"https://x.com/{name}")
    instagram = handle_available(f"https://www.instagram.com/{name}/")
    states = [github, linkedin, x_handle, instagram]
    unavailable_count = sum(1 for state in states if state == "no")
    unknown_count = sum(1 for state in states if state == "unknown")
    return github, linkedin, x_handle, instagram, unavailable_count, unknown_count


def probe_social(*, name: str, config: ValidationConfig) -> ProbeResult:
    normalized = normalized_or_fail(name, config=config)
    query_name = social_query_name(name, normalized)
    probes = {
        "github": social_handle_probe(f"https://github.com/{query_name}"),
        "linkedin": social_handle_probe(f"https://www.linkedin.com/company/{query_name}"),
        "x": social_handle_probe(f"https://x.com/{query_name}"),
        "instagram": social_handle_probe(f"https://www.instagram.com/{query_name}/"),
    }
    unavailable_count = sum(1 for payload in probes.values() if payload["availability"] == "no")
    unknown_count = sum(1 for payload in probes.values() if payload["availability"] == "unknown")
    retryable = [label for label, payload in probes.items() if payload["availability"] == "unknown" and payload["retryable"]]
    details = {
        "query": display_name(name),
        "query_name": query_name,
        **{key: payload["availability"] for key, payload in probes.items()},
    }
    details["unavailable_count"] = unavailable_count
    details["unknown_count"] = unknown_count
    details["http_statuses"] = {key: payload["status_code"] for key, payload in probes.items()}
    if unavailable_count >= config.social_unavailable_fail_threshold:
        return _probe_result(
            check_name="social",
            status=ResultStatus.WARN,
            score_delta=-3.0,
            reason="social_handle_crowded",
            details=details,
        )
    if unknown_count > 0:
        retry_label = retryable[0] if retryable else None
        payload = probes.get(retry_label, {})
        return _probe_result(
            check_name="social",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason="social_check_unknown",
            details=details,
            error_kind=ErrorKind.TRANSPORT if retry_label else ErrorKind.NONE,
            retryable=bool(retry_label),
            http_status=payload.get("status_code") if retry_label else None,
            headers=payload.get("headers") if retry_label else None,
            retry_after_s=payload.get("retry_after_s") if retry_label else None,
            transport="social",
        )
    return _probe_result(
        check_name="social",
        status=ResultStatus.PASS,
        score_delta=0.0,
        reason="",
        details=details,
    )


def app_store_browser_signal(name: str, country: str, *, config: ValidationConfig) -> dict[str, object]:
    query_name = display_name(name)
    normalized_query = normalize_name(name)
    try:
        payload = browser_app_store_items(
            query=query_name,
            country=country,
            profile_dir=config.web_browser_profile_dir or None,
            chrome_executable=config.web_browser_chrome_executable or None,
            timeout_ms=max(3000, int(float(config.timeout_s) * 1000.0)),
        )
    except Exception as exc:
        return {
            "ok": False,
            "source": "browser_app_store",
            "state": "browser_exception",
            "error": f"{exc.__class__.__name__}:{exc}",
        }
    rows = payload.get("items")
    if not bool(payload.get("ok")):
        return {
            "ok": False,
            "source": str(payload.get("source") or "browser_app_store"),
            "state": str(payload.get("state") or "browser_error"),
            "error": str(payload.get("error") or "browser_error"),
            "final_url": str(payload.get("final_url") or ""),
            "title": str(payload.get("title") or ""),
        }
    if not isinstance(rows, list):
        return {
            "ok": False,
            "source": str(payload.get("source") or "browser_app_store"),
            "state": "parse_error",
            "error": "browser_items_missing",
            "final_url": str(payload.get("final_url") or ""),
            "title": str(payload.get("title") or ""),
        }
    exact = False
    for item in rows:
        if not isinstance(item, dict):
            continue
        if normalize_name(str(item.get("title") or "")) == normalized_query:
            exact = True
            break
        if normalize_name(str(item.get("slug") or "")) == normalized_query:
            exact = True
            break
    return {
        "ok": True,
        "source": str(payload.get("source") or "browser_app_store"),
        "state": str(payload.get("state") or "results"),
        "result_count": len(rows),
        "exact": exact,
        "items": rows,
        "final_url": str(payload.get("final_url") or ""),
        "title": str(payload.get("title") or ""),
        "query": query_name,
        "normalized_query": normalized_query,
    }


def probe_app_store(*, name: str, config: ValidationConfig) -> ProbeResult:
    normalized = normalized_or_fail(name, config=config)
    query_name = display_name(name) or normalized
    countries = [country.strip().lower() for country in config.store_countries.split(",") if country.strip()]
    exact: list[str] = []
    unknown: list[str] = []
    counts: dict[str, int] = {}
    states: dict[str, str] = {}
    final_urls: dict[str, str] = {}
    errors: dict[str, str] = {}
    retryable_country: str | None = None
    retryable_kind = ErrorKind.NONE
    for country in countries:
        signal = app_store_browser_signal(query_name, country, config=config)
        counts[country] = int(signal.get("result_count", -1))
        states[country] = str(signal.get("state") or "")
        if str(signal.get("final_url") or "").strip():
            final_urls[country] = str(signal.get("final_url") or "").strip()
        if bool(signal.get("exact")):
            exact.append(country)
        if not bool(signal.get("ok")):
            unknown.append(country)
            errors[country] = str(signal.get("error") or "")
            if retryable_country is None:
                kind = _browser_error_kind(signal.get("error"))
                if kind in {ErrorKind.TIMEOUT, ErrorKind.BROWSER}:
                    retryable_country = country
                    retryable_kind = kind
    details = {
        "query": display_name(name),
        "query_name": query_name,
        "countries": countries,
        "counts": counts,
        "exact": exact,
        "unknown": unknown,
        "states": states,
        "final_urls": final_urls,
        "errors": errors,
    }
    if exact:
        return _probe_result(
            check_name="app_store",
            status=ResultStatus.FAIL,
            score_delta=-18.0,
            reason=f"app_store_exact_collision_{'-'.join(exact)}",
            details=details,
        )
    if unknown:
        return _probe_result(
            check_name="app_store",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-3.0,
            reason=f"app_store_unknown_{'-'.join(unknown)}",
            details=details,
            error_kind=retryable_kind,
            retryable=bool(retryable_country),
            transport="browser_app_store",
            evidence={"country": retryable_country} if retryable_country else {},
        )
    return _probe_result(
        check_name="app_store",
        status=ResultStatus.PASS,
        score_delta=0.0,
        reason="",
        details=details,
    )


def resolve_web_search_order(config: ValidationConfig) -> list[str]:
    supported = {"serper", "brave"}
    order: list[str] = []
    for token in [part.strip().lower() for part in config.web_search_order.split(",") if part.strip()]:
        if token in supported and token not in order:
            order.append(token)
    if "serper" in order:
        order = ["serper", *[item for item in order if item != "serper"]]
    return order or ["serper", "brave"]


def _domain_label(domain: str) -> str:
    token = str(domain or "").strip().lower()
    if not token:
        return ""
    if token.startswith("www."):
        token = token[4:]
    return normalize_name(token.split(".", 1)[0])


def _analyze_search_items(*, normalized: str, items: list[dict[str, object]], source: str, query: str) -> dict[str, object]:
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
        "query": query,
        "normalized_query": normalized,
    }


def brave_search(name: str, *, config: ValidationConfig) -> dict[str, Any] | None:
    response = brave_search_response(name, config=config)
    return response.json() if response.ok else None


def serper_search_response(name: str, *, config: ValidationConfig) -> HttpResponse:
    api_key = str(
        os.getenv(config.web_google_api_env)
        or os.getenv("SERPER_API_KEY")
        or ""
    ).strip()
    if not api_key:
        return HttpResponse(
            ok=False,
            url="",
            status_code=None,
            text="",
            headers={},
            error_kind=ErrorKind.CONFIG,
            error_message="serper_api_key_missing",
        )
    query = f'"{name}"'
    url = "https://google.serper.dev/search?" + parse.urlencode(
        {
            "q": query,
            "num": str(max(1, min(10, int(config.web_google_top)))),
            "gl": config.web_google_gl,
            "hl": config.web_google_hl,
            "autocorrect": "false",
        }
    )
    return fetch_json(
        url,
        timeout=config.timeout_s,
        headers={
            "X-API-KEY": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )


def serper_signal(name: str, *, config: ValidationConfig) -> dict[str, object]:
    query_name = display_name(name)
    normalized_query = normalized_or_fail(name, config=config)
    response = serper_search_response(name, config=config)
    if not response.ok:
        return {
            "ok": False,
            "source": "serper",
            "error_kind": response.error_kind.value,
            "error": response.error_message,
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": bool(response.retryable),
        }
    payload = response.json()
    if payload is None:
        return {
            "ok": False,
            "source": "serper",
            "error_kind": ErrorKind.PARSE.value,
            "error": "serper_json_parse_failed",
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": False,
        }
    items = payload.get("organic")
    if not isinstance(items, list):
        return {
            "ok": False,
            "source": "serper",
            "error_kind": ErrorKind.PARSE.value,
            "error": "serper_results_missing",
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": False,
        }
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
    signal = _analyze_search_items(normalized=normalized_query, items=rows, source="serper", query=query_name)
    signal["status_code"] = response.status_code
    signal["headers"] = response.headers
    signal["retry_after_s"] = response.retry_after_s
    return signal


def brave_search_response(name: str, *, config: ValidationConfig) -> HttpResponse:
    api_key = str(
        os.getenv(config.web_brave_api_env)
        or os.getenv("BRAVE_API_KEY")
        or os.getenv("BRAVE_SEARCH_API_KEY")
        or ""
    ).strip()
    if not api_key:
        return HttpResponse(
            ok=False,
            url="",
            status_code=None,
            text="",
            headers={},
            error_kind=ErrorKind.CONFIG,
            error_message="brave_api_key_missing",
        )
    query = f'"{name}"'
    url = "https://api.search.brave.com/res/v1/web/search?" + parse.urlencode(
        {
            "q": query,
            "count": str(max(1, min(20, int(config.web_brave_top)))),
            "country": config.web_brave_country,
            "search_lang": config.web_brave_search_lang,
            "spellcheck": "false",
        }
    )
    return fetch_json(
        url,
        timeout=config.timeout_s,
        headers={"X-Subscription-Token": api_key, "Accept": "application/json"},
    )


def brave_signal(name: str, *, config: ValidationConfig) -> dict[str, object]:
    query_name = display_name(name)
    normalized_query = normalized_or_fail(name, config=config)
    response = brave_search_response(name, config=config)
    if not response.ok:
        return {
            "ok": False,
            "source": "brave",
            "error_kind": response.error_kind.value,
            "error": response.error_message,
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": bool(response.retryable),
        }
    payload = response.json()
    if payload is None:
        return {
            "ok": False,
            "source": "brave",
            "error_kind": ErrorKind.PARSE.value,
            "error": "brave_json_parse_failed",
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": False,
        }
    web_payload = payload.get("web")
    if not isinstance(web_payload, dict):
        return {
            "ok": False,
            "source": "brave",
            "error_kind": ErrorKind.PARSE.value,
            "error": "brave_web_payload_missing",
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": False,
        }
    items = web_payload.get("results")
    if not isinstance(items, list):
        return {
            "ok": False,
            "source": "brave",
            "error_kind": ErrorKind.PARSE.value,
            "error": "brave_results_missing",
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": False,
        }
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
    signal = _analyze_search_items(normalized=normalized_query, items=rows, source="brave", query=query_name)
    signal["reported_result_count"] = int(web_payload.get("total", 0) or 0)
    signal["status_code"] = response.status_code
    signal["headers"] = response.headers
    signal["retry_after_s"] = response.retry_after_s
    return signal


def _web_result_from_signal(signal: dict[str, object], *, details: dict[str, object]) -> tuple[ResultStatus, float, str]:
    exact_hits = int(signal.get("exact_hits", 0))
    near_hits = int(signal.get("near_hits", 0))
    first_hit_exact = bool(signal.get("first_hit_exact"))
    if first_hit_exact:
        return ResultStatus.FAIL, -24.0, "web_first_hit_exact"
    if exact_hits >= 1:
        return ResultStatus.FAIL, -20.0, "web_exact_collision"
    if near_hits >= 3:
        return ResultStatus.FAIL, -10.0, "web_near_collision"
    if near_hits >= 2:
        return ResultStatus.WARN, -4.0, "web_near_warning"
    return ResultStatus.PASS, 0.0, ""


def probe_web(*, name: str, config: ValidationConfig) -> ProbeResult:
    normalized = normalized_or_fail(name, config=config)
    query_name = display_name(name) or normalized
    order = resolve_web_search_order(config)
    dispatch = {
        "serper": serper_signal,
        "brave": brave_signal,
    }
    signals: dict[str, dict[str, object] | None] = {
        "serper": None,
        "brave": None,
    }
    primary_provider = order[0]
    primary_signal = dispatch[primary_provider](query_name, config=config)
    signals[primary_provider] = primary_signal
    should_probe_fallbacks = not primary_signal.get("ok") or (
        int(primary_signal.get("exact_hits", 0)) == 0 and int(primary_signal.get("near_hits", 0)) < 3
    )
    if should_probe_fallbacks:
        for provider in order[1:]:
            if signals[provider] is None:
                signals[provider] = dispatch[provider](query_name, config=config)
    fallbacks = [signals[provider] for provider in order[1:] if isinstance(signals[provider], dict)]
    successful_fallbacks = [signal for signal in fallbacks if bool(signal.get("ok"))]
    selected = primary_signal if primary_signal.get("ok") else (successful_fallbacks[0] if successful_fallbacks else None)
    details = {
        "query": display_name(name),
        "query_name": query_name,
        "normalized_query": normalized,
        "provider_order": order,
        "serper": signals["serper"],
        "brave": signals["brave"],
    }
    if not selected or not selected.get("ok"):
        fallback = next((signal for signal in reversed(fallbacks) if signal is not None), primary_signal)
        explicit_error_kind = _error_kind_from_token(fallback.get("error_kind"))
        error_kind = explicit_error_kind or _browser_error_kind(fallback.get("error"))
        retryable = error_kind in {ErrorKind.RATE_LIMITED, ErrorKind.TIMEOUT, ErrorKind.BROWSER}
        return _probe_result(
            check_name="web",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason="web_search_unavailable",
            details=details,
            error_kind=error_kind,
            retryable=retryable,
            http_status=fallback.get("status_code"),
            headers=fallback.get("headers"),
            retry_after_s=fallback.get("retry_after_s"),
            transport=str(fallback.get("source") or "web"),
        )
    status, score_delta, reason = _web_result_from_signal(selected, details=details)
    for fallback_signal in successful_fallbacks:
        fallback_status, fallback_score_delta, fallback_reason = _web_result_from_signal(fallback_signal, details=details)
        severity = {
            ResultStatus.FAIL: 3,
            ResultStatus.WARN: 2,
            ResultStatus.UNAVAILABLE: 1,
            ResultStatus.PASS: 0,
        }
        if severity[fallback_status] > severity[status]:
            selected = fallback_signal
            status = fallback_status
            score_delta = fallback_score_delta
            reason = fallback_reason
    details["provider"] = str(selected.get("source") or "")
    details["final_url"] = str(selected.get("final_url") or "")
    details["page_title"] = str(selected.get("page_title") or "")
    details["sample_domains"] = [str(domain) for domain in selected.get("sample_domains", []) if str(domain).strip()]
    details["exact_hits"] = int(selected.get("exact_hits", 0))
    details["near_hits"] = int(selected.get("near_hits", 0))
    details["result_count"] = int(selected.get("result_count", 0))
    details["reported_result_count"] = int(selected.get("reported_result_count", 0))
    details["first_hit_exact"] = bool(selected.get("first_hit_exact"))
    return _probe_result(
        check_name="web",
        status=status,
        score_delta=score_delta,
        reason=reason,
        details=details,
    )


def unsupported_result(check_name: str, reason: str) -> CandidateResult:
    return _candidate_result(
        check_name=check_name,
        status=ResultStatus.UNSUPPORTED,
        score_delta=-2.0,
        reason=reason,
        details={"supported": False},
    )


def _normalize_company_entity_name(raw: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", str(raw or "").lower())
    filtered = [token for token in tokens if token not in COMPANY_LEGAL_SUFFIX_TOKENS]
    return normalize_name(" ".join(filtered))


def company_house_signal(name: str, *, config: ValidationConfig) -> dict[str, object]:
    normalized_query = normalize_name(name)
    api_key = str(os.getenv("COMPANIES_HOUSE_API_KEY") or "").strip()
    if not api_key:
        return {
            "ok": False,
            "configured": False,
            "reason": "company_unconfigured",
            "result_count": 0,
            "exact_active_hits": 0,
            "near_active_hits": 0,
            "sample_titles": [],
            "error_kind": ErrorKind.CONFIG.value,
        }

    query = {
        "q": name,
        "items_per_page": str(max(1, min(20, int(config.company_top)))),
    }
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    response = fetch_json(
        "https://api.company-information.service.gov.uk/search/companies?" + parse.urlencode(query),
        timeout=config.timeout_s,
        headers={"Authorization": f"Basic {token}"},
    )
    if not response.ok:
        return {
            "ok": False,
            "configured": True,
            "reason": "company_lookup_unavailable",
            "result_count": -1,
            "exact_active_hits": 0,
            "near_active_hits": 0,
            "sample_titles": [],
            "error_kind": response.error_kind.value,
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": bool(response.retryable),
        }
    payload = response.json()
    if payload is None:
        return {
            "ok": False,
            "configured": True,
            "reason": "company_lookup_parse_failed",
            "result_count": -1,
            "exact_active_hits": 0,
            "near_active_hits": 0,
            "sample_titles": [],
            "error_kind": ErrorKind.PARSE.value,
            "status_code": response.status_code,
            "headers": response.headers,
            "retry_after_s": response.retry_after_s,
            "retryable": False,
        }
    items = payload.get("items")
    if not isinstance(items, list):
        items = []
    exact_active_hits = 0
    near_active_hits = 0
    sample_titles: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if title and len(sample_titles) < 6:
            sample_titles.append(title)
        if str(item.get("company_status") or "").strip().lower() != "active":
            continue
        normalized_title = _normalize_company_entity_name(title)
        if not normalized_title:
            continue
        if normalized_title == normalized_query:
            exact_active_hits += 1
            continue
        if normalized_title.startswith(normalized_query) or normalized_query in normalized_title:
            near_active_hits += 1
    return {
        "ok": True,
        "configured": True,
        "reason": "",
        "result_count": len(items),
        "exact_active_hits": exact_active_hits,
        "near_active_hits": near_active_hits,
        "sample_titles": sample_titles,
        "status_code": response.status_code,
        "headers": response.headers,
        "retry_after_s": response.retry_after_s,
    }


def probe_company(*, name: str, config: ValidationConfig) -> ProbeResult:
    normalized = normalized_or_fail(name, config=config)
    query_name = display_name(name) or normalized
    signal = company_house_signal(query_name, config=config)
    details = dict(signal)
    details["query"] = display_name(name)
    details["query_name"] = query_name
    details["normalized_query"] = normalized
    if not bool(signal.get("configured")):
        return _probe_result(
            check_name="company",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-1.0,
            reason="company_unconfigured",
            details=details,
            error_kind=ErrorKind.CONFIG,
            transport="companies_house",
        )
    if not bool(signal.get("ok")):
        error_kind = ErrorKind(str(signal.get("error_kind") or ErrorKind.UNEXPECTED.value))
        return _probe_result(
            check_name="company",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason=str(signal.get("reason") or "company_lookup_unavailable"),
            details=details,
            error_kind=error_kind,
            retryable=bool(signal.get("retryable")) and error_kind in {ErrorKind.RATE_LIMITED, ErrorKind.TIMEOUT, ErrorKind.TRANSPORT},
            http_status=signal.get("status_code"),
            headers=signal.get("headers"),
            retry_after_s=signal.get("retry_after_s"),
            transport="companies_house",
        )
    if int(signal.get("exact_active_hits", 0)) > 0:
        return _probe_result(
            check_name="company",
            status=ResultStatus.FAIL,
            score_delta=-16.0,
            reason="company_exact_active",
            details=details,
        )
    if int(signal.get("near_active_hits", 0)) > 0:
        return _probe_result(
            check_name="company",
            status=ResultStatus.WARN,
            score_delta=-4.0,
            reason="company_near_active",
            details=details,
        )
    return _probe_result(
        check_name="company",
        status=ResultStatus.PASS,
        score_delta=0.0,
        reason="",
        details=details,
    )


def probe_tm(*, name: str, config: ValidationConfig) -> ProbeResult:
    normalized = normalized_or_fail(name, config=config)
    query_name = display_name(name) or normalized
    profile_dir = str(config.tmview_profile_dir or "").strip()
    if not profile_dir:
        return _probe_result(
            check_name="tm",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-1.0,
            reason="tm_profile_missing",
            details={"profile_dir": profile_dir},
            error_kind=ErrorKind.CONFIG,
            transport="tmview",
        )
    try:
        from .tmview import probe_names

        results = probe_names(
            names=[query_name],
            normalized_names=[normalized],
            profile_dir=profile_dir,
            chrome_executable=(config.tmview_chrome_executable or None),
            timeout_ms=max(TMVIEW_TIMEOUT_FLOOR_MS, int(float(config.timeout_s) * 1000)),
            settle_ms=2500,
            headless=True,
        )
    except Exception as exc:
        return _probe_result(
            check_name="tm",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason="tm_probe_unavailable",
            details={
                "profile_dir": profile_dir,
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
            },
            error_kind=ErrorKind.BROWSER,
            retryable=True,
            transport="tmview",
        )
    if not results:
        return _probe_result(
            check_name="tm",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason="tm_probe_empty",
            details={"profile_dir": profile_dir},
            error_kind=ErrorKind.PARSE,
            transport="tmview",
        )
    probe = results[0]
    details = {
        "profile_dir": profile_dir,
        "url": str(probe.url or ""),
        "query": query_name,
        "normalized_query": normalized,
        "query_ok": bool(probe.query_ok),
        "result_count": _int_or_zero(probe.result_count),
        "exact_hits": _int_or_zero(probe.exact_hits),
        "near_hits": _int_or_zero(probe.near_hits),
        "active_exact_hits": _int_or_zero(probe.active_exact_hits),
        "inactive_exact_hits": _int_or_zero(probe.inactive_exact_hits),
        "unknown_exact_hits": _int_or_zero(probe.unknown_exact_hits),
        "surface_exact_hits": _int_or_zero(getattr(probe, "surface_exact_hits", 0)),
        "normalized_exact_hits": _int_or_zero(getattr(probe, "normalized_exact_hits", 0)),
        "surface_active_exact_hits": _int_or_zero(getattr(probe, "surface_active_exact_hits", 0)),
        "normalized_active_exact_hits": _int_or_zero(getattr(probe, "normalized_active_exact_hits", 0)),
        "query_sequence": str(getattr(probe, "query_sequence", "") or ""),
        "sample_text": str(probe.sample_text or ""),
        "exact_sample_text": str(probe.exact_sample_text or ""),
        "error": str(probe.error or ""),
        "state": str(getattr(probe, "state", "") or ""),
    }
    if not bool(probe.query_ok):
        error_kind = _browser_error_kind(probe.error)
        return _probe_result(
            check_name="tm",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason="tm_query_unavailable",
            details=details,
            error_kind=error_kind,
            retryable=error_kind in {ErrorKind.TIMEOUT, ErrorKind.BROWSER},
            transport="tmview",
        )
    if _int_or_zero(getattr(probe, "surface_active_exact_hits", 0)) > 0:
        return _probe_result(
            check_name="tm",
            status=ResultStatus.FAIL,
            score_delta=-20.0,
            reason="tm_surface_exact_active_collision",
            details=details,
        )
    if _int_or_zero(getattr(probe, "surface_exact_hits", 0)) > 0:
        return _probe_result(
            check_name="tm",
            status=ResultStatus.WARN,
            score_delta=-6.0,
            reason="tm_surface_exact_review",
            details=details,
        )
    if _int_or_zero(getattr(probe, "normalized_exact_hits", 0)) > 0:
        return _probe_result(
            check_name="tm",
            status=ResultStatus.WARN,
            score_delta=-5.0,
            reason="tm_normalized_exact_review",
            details=details,
        )
    if int(probe.near_hits) > 0:
        return _probe_result(
            check_name="tm",
            status=ResultStatus.WARN,
            score_delta=-4.0,
            reason="tm_near_review",
            details=details,
        )
    return _probe_result(
        check_name="tm",
        status=ResultStatus.PASS,
        score_delta=0.0,
        reason="",
        details=details,
    )


def probe_tm_cheap(*, name: str, config: ValidationConfig) -> ProbeResult:
    del name, config
    return _probe_result(
        check_name="tm_cheap",
        status=ResultStatus.UNSUPPORTED,
        score_delta=-2.0,
        reason="tm_cheap_check_unavailable",
        details={"supported": False},
    )


def check_domain(*, name: str, config: ValidationConfig) -> CandidateResult:
    return probe_domain(name=name, config=config).candidate_result


def check_package(*, name: str, config: ValidationConfig) -> CandidateResult:
    return probe_package(name=name, config=config).candidate_result


def check_social(*, name: str, config: ValidationConfig) -> CandidateResult:
    return probe_social(name=name, config=config).candidate_result


def check_app_store(*, name: str, config: ValidationConfig) -> CandidateResult:
    return probe_app_store(name=name, config=config).candidate_result


def check_web(*, name: str, config: ValidationConfig) -> CandidateResult:
    return probe_web(name=name, config=config).candidate_result


def check_company(*, name: str, config: ValidationConfig) -> CandidateResult:
    return probe_company(name=name, config=config).candidate_result


def check_tm(*, name: str, config: ValidationConfig) -> CandidateResult:
    return probe_tm(name=name, config=config).candidate_result


def check_tm_cheap(*, name: str, config: ValidationConfig) -> CandidateResult:
    return probe_tm_cheap(name=name, config=config).candidate_result
