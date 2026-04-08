# ruff: noqa: E402
from __future__ import annotations

import json
import sys
from types import SimpleNamespace
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import brandpipe.validation_checks as validation_checks
from brandpipe.http_client import HttpResponse
from brandpipe.models import ErrorKind, ResultStatus, ValidationConfig
from brandpipe.naming_policy import NameShapePolicy
from brandpipe.validation_checks import (
    _analyze_search_items,
    _browser_error_kind,
    _domain_label,
    _error_kind_from_token,
    _int_or_zero,
    _normalize_company_entity_name,
    _rdap_url,
    _web_result_from_signal,
    app_store_browser_signal,
    brave_search,
    brave_search_response,
    brave_signal,
    company_house_signal,
    explicit_domain_parts,
    handle_available,
    package_exists_on_npm,
    package_exists_on_pypi,
    package_probe,
    package_query_name,
    parse_required_domain_tlds,
    probe_app_store,
    probe_company,
    probe_domain,
    probe_package,
    probe_social,
    probe_tm_cheap,
    probe_web,
    rdap_probe,
    resolve_web_search_order,
    resolve_required_domain_tlds,
    serper_signal,
    serper_search_response,
    social_handle_signal,
    social_handle_probe,
    social_query_name,
    normalized_or_fail,
    unsupported_result,
)


def _http_response(
    *,
    ok: bool,
    status_code: int | None,
    text: str = "",
    headers: dict[str, str] | None = None,
    error_kind: ErrorKind = ErrorKind.NONE,
    error_message: str = "",
    retry_after_s: float | None = None,
    url: str = "https://example.test",
) -> HttpResponse:
    return HttpResponse(
        ok=ok,
        url=url,
        status_code=status_code,
        text=text,
        headers=headers or {},
        error_kind=error_kind,
        error_message=error_message,
        retry_after_s=retry_after_s,
    )


class ValidationChecksTests(unittest.TestCase):
    def test_normalized_or_fail_enforces_custom_shape_policy(self) -> None:
        config = ValidationConfig(
            name_shape_policy=NameShapePolicy(min_length=4, max_length=6, allow_digits=False, require_letter=True)
        )

        self.assertEqual(normalized_or_fail("Vanto", config=config), "vanto")

        with self.assertRaisesRegex(ValueError, "invalid_candidate_name"):
            normalized_or_fail("1234", config=config)
        with self.assertRaisesRegex(ValueError, "invalid_candidate_name"):
            normalized_or_fail("vantora", config=config)
        with self.assertRaisesRegex(ValueError, "invalid_candidate_name"):
            normalized_or_fail("set4you", config=config)

    def test_query_helpers_preserve_surface_exact_domain_and_social_slug(self) -> None:
        self.assertEqual(explicit_domain_parts("Incident.IO"), ("incident", "io"))
        self.assertIsNone(explicit_domain_parts("Incident IO"))
        self.assertEqual(package_query_name("incident.io", "incidentio"), "incident.io")
        self.assertEqual(package_query_name("XnView MP", "xnviewmp"), "xnviewmp")
        self.assertEqual(social_query_name("XnView MP", "xnviewmp"), "xnviewmp")

    def test_helper_parsers_and_error_kind_mapping_cover_invalid_tokens(self) -> None:
        self.assertEqual(_int_or_zero("4"), 4)
        self.assertEqual(_int_or_zero("abc"), 0)
        self.assertEqual(_browser_error_kind("captcha_required"), ErrorKind.CHALLENGE)
        self.assertEqual(_browser_error_kind("page_timeout"), ErrorKind.TIMEOUT)
        self.assertEqual(_browser_error_kind("chrome_executable_not_found"), ErrorKind.CONFIG)
        self.assertEqual(_browser_error_kind("items_missing"), ErrorKind.PARSE)
        self.assertEqual(_browser_error_kind("playwright_crash"), ErrorKind.BROWSER)
        self.assertEqual(_browser_error_kind("mystery"), ErrorKind.UNEXPECTED)
        self.assertEqual(_error_kind_from_token(ErrorKind.TIMEOUT.value), ErrorKind.TIMEOUT)
        self.assertIsNone(_error_kind_from_token("not-real"))
        self.assertEqual(parse_required_domain_tlds("com,de,io,de"), (["com", "de"], ["io"]))
        self.assertEqual(resolve_required_domain_tlds(ValidationConfig(required_domain_tlds="com,ch")), ["com", "ch"])
        with self.assertRaisesRegex(ValueError, "unsupported_required_domain_tlds:io"):
            resolve_required_domain_tlds(ValidationConfig(required_domain_tlds="io"))

    def test_rdap_probe_maps_known_status_codes(self) -> None:
        not_found = HttpResponse(
            ok=False,
            url="https://rdap.example/vantora.com",
            status_code=404,
            text="",
            headers={},
            error_kind=ErrorKind.NONE,
            error_message="",
        )

        with mock.patch("brandpipe.validation_checks.fetch_status", return_value=not_found) as fetch_mock:
            payload = rdap_probe("vantora", "com")

        self.assertEqual(fetch_mock.call_args.args[0], _rdap_url("vantora", "com"))
        self.assertEqual(payload["availability"], "yes")
        self.assertEqual(payload["status_code"], 404)
        self.assertTrue(payload["retryable"])

    def test_package_probe_and_social_handle_probe_map_http_statuses(self) -> None:
        with mock.patch(
            "brandpipe.validation_checks.fetch_status",
            return_value=HttpResponse(
                ok=True,
                url="https://pypi.org/pypi/vantora/json",
                status_code=200,
                text="{}",
                headers={},
                error_kind=ErrorKind.NONE,
                error_message="",
            ),
        ):
            package = package_probe("pypi", "vantora")

        self.assertEqual(package["exists"], "yes")

        with mock.patch(
            "brandpipe.validation_checks.fetch_status",
            return_value=HttpResponse(
                ok=False,
                url="https://github.com/vantora",
                status_code=410,
                text="",
                headers={},
                error_kind=ErrorKind.NONE,
                error_message="",
            ),
        ):
            social = social_handle_probe("https://github.com/vantora")

        self.assertEqual(social["availability"], "yes")

    def test_probe_package_returns_retryable_unavailable_when_registry_unknown(self) -> None:
        config = ValidationConfig(checks=["package"])

        def fake_package_probe(registry: str, name: str) -> dict[str, object]:
            self.assertEqual(name, "vantora")
            if registry == "pypi":
                return {
                    "exists": "unknown",
                    "status_code": 429,
                    "error_kind": ErrorKind.RATE_LIMITED.value,
                    "headers": {"Retry-After": "9"},
                    "retry_after_s": 9.0,
                    "retryable": True,
                }
            return {
                "exists": "no",
                "status_code": 404,
                "error_kind": ErrorKind.NONE.value,
                "headers": {},
                "retry_after_s": None,
                "retryable": False,
            }

        with mock.patch("brandpipe.validation_checks.package_probe", side_effect=fake_package_probe):
            result = probe_package(name="vantora", config=config)

        self.assertEqual(result.candidate_result.status, ResultStatus.UNAVAILABLE)
        self.assertEqual(result.candidate_result.reason, "package_unknown_pypi")
        self.assertEqual(result.error_kind, ErrorKind.TRANSPORT)
        self.assertTrue(result.retryable)
        self.assertEqual(result.transport, "package_registry")
        self.assertEqual(result.candidate_result.details["query_name"], "vantora")

    def test_probe_social_uses_slug_query_name_and_marks_retryable_unknown(self) -> None:
        config = ValidationConfig(checks=["social"], social_unavailable_fail_threshold=3)
        seen_urls: list[str] = []

        def fake_social_probe(url: str) -> dict[str, object]:
            seen_urls.append(url)
            if "github.com" in url:
                return {
                    "availability": "unknown",
                    "status_code": 429,
                    "error_kind": ErrorKind.RATE_LIMITED.value,
                    "headers": {"Retry-After": "15"},
                    "retry_after_s": 15.0,
                    "retryable": True,
                }
            return {
                "availability": "yes",
                "status_code": 404,
                "error_kind": ErrorKind.NONE.value,
                "headers": {},
                "retry_after_s": None,
                "retryable": False,
            }

        with mock.patch("brandpipe.validation_checks.social_handle_probe", side_effect=fake_social_probe):
            result = probe_social(name="XnView MP", config=config)

        self.assertEqual(result.candidate_result.status, ResultStatus.UNAVAILABLE)
        self.assertEqual(result.candidate_result.reason, "social_check_unknown")
        self.assertEqual(result.transport, "social")
        self.assertTrue(result.retryable)
        self.assertEqual(result.candidate_result.details["query_name"], "xnviewmp")
        self.assertIn("https://github.com/xnviewmp", seen_urls[0])

    def test_search_analysis_counts_exact_and_near_hits(self) -> None:
        signal = _analyze_search_items(
            normalized="vantora",
            items=[
                {
                    "link": "https://vantora.example",
                    "title": "Vantora",
                    "snippet": "Official site",
                },
                {
                    "link": "https://www.example.com/company",
                    "title": "A review",
                    "snippet": "The vantora platform is mentioned here",
                },
            ],
            source="serper",
            query="Vantora",
        )

        self.assertEqual(_domain_label("www.Vantora.example"), "vantora")
        self.assertEqual(signal["exact_hits"], 1)
        self.assertEqual(signal["near_hits"], 1)
        self.assertEqual(signal["sample_domains"], ["vantora.example", "www.example.com"])
        self.assertTrue(signal["first_hit_exact"])

    def test_serper_signal_parses_results_and_reports_parse_failure(self) -> None:
        ok_response = HttpResponse(
            ok=True,
            url="https://google.serper.dev/search",
            status_code=200,
            text=json.dumps(
                {
                    "organic": [
                        {
                            "link": "https://vantora.example",
                            "title": "Vantora",
                            "snippet": "Official site",
                        }
                    ]
                }
            ),
            headers={"X-Test": "1"},
            error_kind=ErrorKind.NONE,
            error_message="",
        )

        with mock.patch("brandpipe.validation_checks.serper_search_response", return_value=ok_response):
            signal = serper_signal("Vantora", config=ValidationConfig())

        self.assertTrue(signal["ok"])
        self.assertEqual(signal["source"], "serper")
        self.assertEqual(signal["exact_hits"], 1)
        self.assertEqual(signal["status_code"], 200)
        self.assertEqual(signal["headers"], {"X-Test": "1"})

        missing_results = HttpResponse(
            ok=True,
            url="https://google.serper.dev/search",
            status_code=200,
            text=json.dumps({"not_organic": []}),
            headers={},
            error_kind=ErrorKind.NONE,
            error_message="",
        )
        with mock.patch("brandpipe.validation_checks.serper_search_response", return_value=missing_results):
            bad_signal = serper_signal("Vantora", config=ValidationConfig())

        self.assertFalse(bad_signal["ok"])
        self.assertEqual(bad_signal["error"], "serper_results_missing")

    def test_web_result_from_signal_uses_expected_severity_thresholds(self) -> None:
        self.assertEqual(
            _web_result_from_signal({"exact_hits": 0, "near_hits": 0, "first_hit_exact": True}, details={}),
            (ResultStatus.FAIL, -24.0, "web_first_hit_exact"),
        )
        self.assertEqual(
            _web_result_from_signal({"exact_hits": 1, "near_hits": 0, "first_hit_exact": False}, details={}),
            (ResultStatus.FAIL, -20.0, "web_exact_collision"),
        )
        self.assertEqual(
            _web_result_from_signal({"exact_hits": 0, "near_hits": 3, "first_hit_exact": False}, details={}),
            (ResultStatus.FAIL, -10.0, "web_near_collision"),
        )
        self.assertEqual(
            _web_result_from_signal({"exact_hits": 0, "near_hits": 2, "first_hit_exact": False}, details={}),
            (ResultStatus.WARN, -4.0, "web_near_warning"),
        )
        self.assertEqual(
            _web_result_from_signal({"exact_hits": 0, "near_hits": 1, "first_hit_exact": False}, details={}),
            (ResultStatus.PASS, 0.0, ""),
        )

    def test_probe_domain_covers_explicit_required_and_default_failure_paths(self) -> None:
        exact_unavailable = {
            "availability": "no",
            "status_code": 200,
            "error_kind": ErrorKind.NONE.value,
            "headers": {},
            "retry_after_s": None,
            "retryable": True,
        }
        with mock.patch("brandpipe.validation_checks.rdap_probe", return_value=exact_unavailable) as rdap_mock:
            explicit_result = probe_domain(name="Vantora.de", config=ValidationConfig())

        self.assertEqual(rdap_mock.call_args.args, ("vantora", "de"))
        self.assertEqual(explicit_result.candidate_result.status, ResultStatus.FAIL)
        self.assertEqual(explicit_result.candidate_result.reason, "domain_unavailable_de")
        self.assertEqual(explicit_result.candidate_result.details["mode"], "surface_exact")

        self.assertEqual(_domain_label(""), "")

        def fake_required_probe(name: str, tld: str) -> dict[str, object]:
            self.assertEqual(name, "vantora")
            if tld == "com":
                return {
                    "availability": "yes",
                    "status_code": 404,
                    "error_kind": ErrorKind.NONE.value,
                    "headers": {},
                    "retry_after_s": None,
                    "retryable": False,
                }
            return {
                "availability": "unknown",
                "status_code": 429,
                "error_kind": ErrorKind.RATE_LIMITED.value,
                "headers": {"Retry-After": "9"},
                "retry_after_s": 9.0,
                "retryable": True,
            }

        with mock.patch("brandpipe.validation_checks.rdap_probe", side_effect=fake_required_probe):
            required_result = probe_domain(
                name="Vantora",
                config=ValidationConfig(required_domain_tlds="com,de"),
            )

        self.assertEqual(required_result.candidate_result.status, ResultStatus.UNAVAILABLE)
        self.assertEqual(required_result.candidate_result.reason, "domain_unknown_de")
        self.assertEqual(required_result.error_kind, ErrorKind.TRANSPORT)
        self.assertTrue(required_result.retryable)
        self.assertEqual(required_result.http_status, 429)
        self.assertEqual(required_result.retry_after_s, 9.0)
        self.assertEqual(required_result.transport, "rdap")

        default_fail_probe = {
            "availability": "no",
            "status_code": 200,
            "error_kind": ErrorKind.NONE.value,
            "headers": {},
            "retry_after_s": None,
            "retryable": False,
        }
        with mock.patch("brandpipe.validation_checks.rdap_probe", return_value=default_fail_probe):
            default_result = probe_domain(name="Vantora", config=ValidationConfig())

        self.assertEqual(default_result.candidate_result.status, ResultStatus.FAIL)
        self.assertEqual(default_result.candidate_result.reason, "domain_unavailable_default_pool")
        self.assertEqual(default_result.candidate_result.details["mode"], "default_any")

    def test_app_store_browser_signal_maps_payload_variants(self) -> None:
        config = ValidationConfig(
            timeout_s=1.0,
            web_browser_profile_dir="/tmp/browser-profile",
            web_browser_chrome_executable="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        )

        with mock.patch("brandpipe.validation_checks.browser_app_store_items", side_effect=RuntimeError("boom")):
            exception_signal = app_store_browser_signal("Vantora", "de", config=config)

        self.assertFalse(exception_signal["ok"])
        self.assertEqual(exception_signal["state"], "browser_exception")
        self.assertIn("RuntimeError:boom", exception_signal["error"])

        with mock.patch(
            "brandpipe.validation_checks.browser_app_store_items",
            return_value={
                "ok": False,
                "source": "browser_app_store",
                "state": "page_timeout",
                "error": "page_timeout",
                "final_url": "https://apps.apple.com/de/search",
                "title": "Search",
            },
        ):
            browser_error = app_store_browser_signal("Vantora", "de", config=config)

        self.assertFalse(browser_error["ok"])
        self.assertEqual(browser_error["state"], "page_timeout")
        self.assertEqual(browser_error["final_url"], "https://apps.apple.com/de/search")

        with mock.patch(
            "brandpipe.validation_checks.browser_app_store_items",
            return_value={"ok": True, "source": "browser_app_store", "items": None, "title": "Search"},
        ):
            parse_error = app_store_browser_signal("Vantora", "de", config=config)

        self.assertFalse(parse_error["ok"])
        self.assertEqual(parse_error["state"], "parse_error")
        self.assertEqual(parse_error["error"], "browser_items_missing")

        with mock.patch(
            "brandpipe.validation_checks.browser_app_store_items",
            return_value={
                "ok": True,
                "source": "browser_app_store",
                "state": "results",
                "items": ["skip-me", {"title": "Vantora App", "slug": "vantora"}],
                "final_url": "https://apps.apple.com/de/search?term=vantora",
                "title": "Results",
            },
        ) as browser_mock:
            success_signal = app_store_browser_signal("Vantora", "de", config=config)

        self.assertTrue(success_signal["ok"])
        self.assertTrue(success_signal["exact"])
        self.assertEqual(success_signal["result_count"], 2)
        self.assertEqual(browser_mock.call_args.kwargs["timeout_ms"], 3000)
        self.assertEqual(browser_mock.call_args.kwargs["profile_dir"], "/tmp/browser-profile")

    def test_probe_app_store_reports_exact_unknown_and_pass_states(self) -> None:
        config = ValidationConfig(store_countries="de,ch")

        with mock.patch(
            "brandpipe.validation_checks.app_store_browser_signal",
            side_effect=[
                {"ok": True, "state": "results", "result_count": 1, "exact": True, "final_url": "https://apps.apple.com/de"},
                {"ok": True, "state": "results", "result_count": 0, "exact": False, "final_url": "https://apps.apple.com/ch"},
            ],
        ):
            exact_result = probe_app_store(name="Vantora", config=config)

        self.assertEqual(exact_result.candidate_result.status, ResultStatus.FAIL)
        self.assertEqual(exact_result.candidate_result.reason, "app_store_exact_collision_de")

        with mock.patch(
            "brandpipe.validation_checks.app_store_browser_signal",
            side_effect=[
                {
                    "ok": False,
                    "state": "page_timeout",
                    "error": "page_timeout",
                    "result_count": -1,
                    "final_url": "https://apps.apple.com/de",
                },
                {"ok": True, "state": "results", "result_count": 0, "exact": False, "final_url": "https://apps.apple.com/ch"},
            ],
        ):
            unknown_result = probe_app_store(name="Vantora", config=config)

        self.assertEqual(unknown_result.candidate_result.status, ResultStatus.UNAVAILABLE)
        self.assertEqual(unknown_result.candidate_result.reason, "app_store_unknown_de")
        self.assertEqual(unknown_result.error_kind, ErrorKind.TIMEOUT)
        self.assertTrue(unknown_result.retryable)
        self.assertEqual(unknown_result.transport, "browser_app_store")
        self.assertEqual(unknown_result.evidence, {"country": "de"})

        with mock.patch(
            "brandpipe.validation_checks.app_store_browser_signal",
            side_effect=[
                {"ok": True, "state": "results", "result_count": 0, "exact": False, "final_url": "https://apps.apple.com/de"},
                {"ok": True, "state": "results", "result_count": 0, "exact": False, "final_url": "https://apps.apple.com/ch"},
            ],
        ):
            pass_result = probe_app_store(name="Vantora", config=config)

        self.assertEqual(pass_result.candidate_result.status, ResultStatus.PASS)
        self.assertEqual(pass_result.candidate_result.reason, "")

    def test_web_search_helpers_build_requests_and_parse_brave_payloads(self) -> None:
        self.assertEqual(
            resolve_web_search_order(ValidationConfig(web_search_order="brave,serper,legacy,brave")),
            ["serper", "brave"],
        )
        self.assertEqual(resolve_web_search_order(ValidationConfig(web_search_order="legacy")), ["serper", "brave"])

        with mock.patch.dict("os.environ", {}, clear=True):
            missing_serper = serper_search_response("Vantora", config=ValidationConfig())
            missing_brave = brave_search_response("Vantora", config=ValidationConfig())

        self.assertEqual(missing_serper.error_kind, ErrorKind.CONFIG)
        self.assertEqual(missing_serper.error_message, "serper_api_key_missing")
        self.assertEqual(missing_brave.error_kind, ErrorKind.CONFIG)
        self.assertEqual(missing_brave.error_message, "brave_api_key_missing")

        with mock.patch.dict("os.environ", {"SERPER_API_KEY": "serper-key", "BRAVE_API_KEY": "brave-key"}, clear=True):
            with mock.patch(
                "brandpipe.validation_checks.fetch_json",
                return_value=_http_response(ok=True, status_code=200, text="{}"),
            ) as fetch_mock:
                serper_search_response("Vantora", config=ValidationConfig(web_google_top=99))
                brave_search_response("Vantora", config=ValidationConfig(web_brave_top=99))

        self.assertIn("google.serper.dev/search?", fetch_mock.call_args_list[0].args[0])
        self.assertIn("q=%22Vantora%22", fetch_mock.call_args_list[0].args[0])
        self.assertIn("num=10", fetch_mock.call_args_list[0].args[0])
        self.assertEqual(fetch_mock.call_args_list[0].kwargs["headers"]["X-API-KEY"], "serper-key")
        self.assertIn("api.search.brave.com/res/v1/web/search?", fetch_mock.call_args_list[1].args[0])
        self.assertIn("count=20", fetch_mock.call_args_list[1].args[0])
        self.assertEqual(fetch_mock.call_args_list[1].kwargs["headers"]["X-Subscription-Token"], "brave-key")

        with mock.patch(
            "brandpipe.validation_checks.brave_search_response",
            return_value=_http_response(ok=True, status_code=200, text=json.dumps({"hello": "world"})),
        ):
            self.assertEqual(brave_search("Vantora", config=ValidationConfig()), {"hello": "world"})
        with mock.patch(
            "brandpipe.validation_checks.brave_search_response",
            return_value=_http_response(ok=False, status_code=429, error_kind=ErrorKind.RATE_LIMITED, error_message="limited"),
        ):
            self.assertIsNone(brave_search("Vantora", config=ValidationConfig()))

        with mock.patch(
            "brandpipe.validation_checks.brave_search_response",
            return_value=_http_response(ok=False, status_code=429, error_kind=ErrorKind.RATE_LIMITED, error_message="limited"),
        ):
            unavailable_signal = brave_signal("Vantora", config=ValidationConfig())
        self.assertFalse(unavailable_signal["ok"])
        self.assertEqual(unavailable_signal["error_kind"], ErrorKind.RATE_LIMITED.value)

        with mock.patch(
            "brandpipe.validation_checks.brave_search_response",
            return_value=_http_response(ok=True, status_code=200, text="{not-json"),
        ):
            parse_signal = brave_signal("Vantora", config=ValidationConfig())
        self.assertEqual(parse_signal["error"], "brave_json_parse_failed")

        with mock.patch(
            "brandpipe.validation_checks.brave_search_response",
            return_value=_http_response(ok=True, status_code=200, text=json.dumps({})),
        ):
            web_missing_signal = brave_signal("Vantora", config=ValidationConfig())
        self.assertEqual(web_missing_signal["error"], "brave_web_payload_missing")

        with mock.patch(
            "brandpipe.validation_checks.brave_search_response",
            return_value=_http_response(ok=True, status_code=200, text=json.dumps({"web": {}})),
        ):
            results_missing_signal = brave_signal("Vantora", config=ValidationConfig())
        self.assertEqual(results_missing_signal["error"], "brave_results_missing")

        with mock.patch(
            "brandpipe.validation_checks.brave_search_response",
            return_value=_http_response(
                ok=True,
                status_code=200,
                text=json.dumps(
                    {
                        "web": {
                            "results": [
                                "skip-me",
                                {
                                    "url": "https://vantora.example",
                                    "title": "Vantora",
                                    "description": "Official site",
                                },
                            ],
                            "total": 11,
                        }
                    }
                ),
                headers={"X-Brave": "1"},
            ),
        ):
            ok_signal = brave_signal("Vantora", config=ValidationConfig())

        self.assertTrue(ok_signal["ok"])
        self.assertEqual(ok_signal["exact_hits"], 1)
        self.assertEqual(ok_signal["reported_result_count"], 11)
        self.assertEqual(ok_signal["headers"], {"X-Brave": "1"})

    def test_probe_web_prefers_more_severe_fallback_and_surfaces_unavailable_errors(self) -> None:
        with mock.patch(
            "brandpipe.validation_checks.serper_signal",
            return_value={
                "ok": True,
                "source": "serper",
                "exact_hits": 0,
                "near_hits": 0,
                "result_count": 2,
                "sample_domains": ["example.com"],
                "first_hit_exact": False,
            },
        ), mock.patch(
            "brandpipe.validation_checks.brave_signal",
            return_value={
                "ok": True,
                "source": "brave",
                "exact_hits": 1,
                "near_hits": 0,
                "result_count": 1,
                "sample_domains": ["vantora.example"],
                "first_hit_exact": False,
            },
        ):
            collision_result = probe_web(name="Vantora", config=ValidationConfig())

        self.assertEqual(collision_result.candidate_result.status, ResultStatus.FAIL)
        self.assertEqual(collision_result.candidate_result.reason, "web_exact_collision")
        self.assertEqual(collision_result.candidate_result.details["provider"], "brave")

        with mock.patch(
            "brandpipe.validation_checks.serper_signal",
            return_value={
                "ok": False,
                "source": "serper",
                "error_kind": ErrorKind.RATE_LIMITED.value,
                "error": "limited",
                "status_code": 429,
                "headers": {"Retry-After": "12"},
                "retry_after_s": 12.0,
            },
        ), mock.patch(
            "brandpipe.validation_checks.brave_signal",
            return_value={
                "ok": False,
                "source": "brave",
                "error_kind": ErrorKind.TIMEOUT.value,
                "error": "timeout",
                "status_code": None,
                "headers": {},
                "retry_after_s": None,
            },
        ):
            unavailable_result = probe_web(name="Vantora", config=ValidationConfig())

        self.assertEqual(unavailable_result.candidate_result.status, ResultStatus.UNAVAILABLE)
        self.assertEqual(unavailable_result.candidate_result.reason, "web_search_unavailable")
        self.assertEqual(unavailable_result.error_kind, ErrorKind.TIMEOUT)
        self.assertTrue(unavailable_result.retryable)
        self.assertEqual(unavailable_result.transport, "brave")

    def test_company_helpers_cover_normalization_lookup_and_probe_paths(self) -> None:
        self.assertEqual(_normalize_company_entity_name("Vantora GmbH & Co KG"), "vantora")

        with mock.patch.dict("os.environ", {}, clear=True):
            unconfigured_signal = company_house_signal("Vantora", config=ValidationConfig())
        self.assertFalse(unconfigured_signal["ok"])
        self.assertFalse(unconfigured_signal["configured"])
        self.assertEqual(unconfigured_signal["reason"], "company_unconfigured")

        with mock.patch.dict("os.environ", {"COMPANIES_HOUSE_API_KEY": "secret"}, clear=True):
            with mock.patch(
                "brandpipe.validation_checks.fetch_json",
                return_value=_http_response(
                    ok=False,
                    status_code=429,
                    error_kind=ErrorKind.RATE_LIMITED,
                    error_message="limited",
                    headers={"Retry-After": "4"},
                    retry_after_s=4.0,
                ),
            ):
                unavailable_signal = company_house_signal("Vantora", config=ValidationConfig())
            with mock.patch(
                "brandpipe.validation_checks.fetch_json",
                return_value=_http_response(ok=True, status_code=200, text="{not-json"),
            ):
                parse_signal = company_house_signal("Vantora", config=ValidationConfig())
            with mock.patch(
                "brandpipe.validation_checks.fetch_json",
                return_value=_http_response(
                    ok=True,
                    status_code=200,
                    text=json.dumps(
                        {
                            "items": [
                                {"title": "Vantora GmbH", "company_status": "active"},
                                {"title": "Vantora Labs Limited", "company_status": "active"},
                                {"title": "Vantora Ventures Ltd", "company_status": "dissolved"},
                                "skip-me",
                            ]
                        }
                    ),
                    headers={"X-CH": "1"},
                ),
            ):
                ok_signal = company_house_signal("Vantora", config=ValidationConfig())

        self.assertFalse(unavailable_signal["ok"])
        self.assertEqual(unavailable_signal["reason"], "company_lookup_unavailable")
        self.assertEqual(unavailable_signal["error_kind"], ErrorKind.RATE_LIMITED.value)
        self.assertFalse(parse_signal["ok"])
        self.assertEqual(parse_signal["reason"], "company_lookup_parse_failed")
        self.assertTrue(ok_signal["ok"])
        self.assertEqual(ok_signal["exact_active_hits"], 1)
        self.assertEqual(ok_signal["near_active_hits"], 1)
        self.assertEqual(ok_signal["sample_titles"][:3], ["Vantora GmbH", "Vantora Labs Limited", "Vantora Ventures Ltd"])

        with mock.patch(
            "brandpipe.validation_checks.company_house_signal",
            return_value={
                "ok": False,
                "configured": True,
                "reason": "company_lookup_parse_failed",
                "result_count": -1,
                "exact_active_hits": 0,
                "near_active_hits": 0,
                "sample_titles": [],
                "error_kind": ErrorKind.PARSE.value,
                "status_code": 200,
                "headers": {},
                "retry_after_s": None,
                "retryable": True,
            },
        ):
            unavailable_probe = probe_company(name="Vantora", config=ValidationConfig())

        self.assertEqual(unavailable_probe.candidate_result.status, ResultStatus.UNAVAILABLE)
        self.assertEqual(unavailable_probe.error_kind, ErrorKind.PARSE)
        self.assertFalse(unavailable_probe.retryable)

        with mock.patch(
            "brandpipe.validation_checks.company_house_signal",
            return_value={
                "ok": True,
                "configured": True,
                "reason": "",
                "result_count": 2,
                "exact_active_hits": 0,
                "near_active_hits": 2,
                "sample_titles": ["Vantora Labs Limited"],
                "status_code": 200,
                "headers": {},
                "retry_after_s": None,
            },
        ):
            near_probe = probe_company(name="Vantora", config=ValidationConfig())

        self.assertEqual(near_probe.candidate_result.status, ResultStatus.WARN)
        self.assertEqual(near_probe.candidate_result.reason, "company_near_active")

    def test_wrapper_helpers_delegate_to_underlying_probes(self) -> None:
        with mock.patch("brandpipe.validation_checks.package_probe", return_value={"exists": "yes"}) as package_probe_mock:
            self.assertEqual(package_exists_on_pypi("vantora"), "yes")
            self.assertEqual(package_exists_on_npm("vantora"), "yes")
        self.assertEqual(package_probe_mock.call_args_list[0].args, ("pypi", "vantora"))
        self.assertEqual(package_probe_mock.call_args_list[1].args, ("npm", "vantora"))

        with mock.patch("brandpipe.validation_checks.social_handle_probe", return_value={"availability": "yes"}):
            self.assertEqual(handle_available("https://github.com/vantora"), "yes")
        with mock.patch("brandpipe.validation_checks.handle_available", side_effect=["yes", "no", "unknown", "yes"]):
            self.assertEqual(
                social_handle_signal("vantora"),
                ("yes", "no", "unknown", "yes", 1, 1),
            )

        unsupported = unsupported_result("custom", "disabled")
        self.assertEqual(unsupported.status, ResultStatus.UNSUPPORTED)
        self.assertEqual(unsupported.details, {"supported": False})

        tm_cheap = probe_tm_cheap(name="Vantora", config=ValidationConfig())
        self.assertEqual(tm_cheap.candidate_result.status, ResultStatus.UNSUPPORTED)

        expected = mock.sentinel.candidate_result
        wrappers = [
            ("probe_domain", validation_checks.check_domain),
            ("probe_package", validation_checks.check_package),
            ("probe_social", validation_checks.check_social),
            ("probe_app_store", validation_checks.check_app_store),
            ("probe_web", validation_checks.check_web),
            ("probe_company", validation_checks.check_company),
            ("probe_tm", validation_checks.check_tm),
            ("probe_tm_cheap", validation_checks.check_tm_cheap),
        ]
        for probe_name, wrapper in wrappers:
            with self.subTest(wrapper=wrapper.__name__):
                with mock.patch(
                    f"brandpipe.validation_checks.{probe_name}",
                    return_value=SimpleNamespace(candidate_result=expected),
                ):
                    self.assertIs(wrapper(name="Vantora", config=ValidationConfig()), expected)


if __name__ == "__main__":
    unittest.main()
