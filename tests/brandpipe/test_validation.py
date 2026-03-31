# ruff: noqa: E402
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.http_client import HttpResponse
from brandpipe.models import ErrorKind, ResultStatus, ValidationConfig
from brandpipe.validation import CHECK_PROBERS, validate_candidate
from brandpipe.validation_checks import resolve_web_search_order


class ValidationTests(unittest.TestCase):
    def test_domain_check_preserves_digits_in_candidate_name(self) -> None:
        config = ValidationConfig(checks=["domain"])

        def fake_rdap_probe(name: str, tld: str) -> dict[str, object]:
            self.assertEqual(name, "set4you")
            return {
                "availability": "yes",
                "status_code": 404,
                "error_kind": ErrorKind.NONE.value,
                "headers": {},
                "retry_after_s": None,
                "retryable": False,
            }

        with mock.patch("brandpipe.validation_checks.rdap_probe", side_effect=fake_rdap_probe):
            results = validate_candidate(name="set4you", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)

    def test_domain_check_uses_explicit_surface_tld_when_present(self) -> None:
        config = ValidationConfig(checks=["domain"])
        calls: list[tuple[str, str]] = []

        def fake_rdap_probe(name: str, tld: str) -> dict[str, object]:
            calls.append((name, tld))
            return {
                "availability": "yes",
                "status_code": 404,
                "error_kind": ErrorKind.NONE.value,
                "headers": {},
                "retry_after_s": None,
                "retryable": False,
            }

        with mock.patch("brandpipe.validation_checks.rdap_probe", side_effect=fake_rdap_probe):
            results = validate_candidate(name="incident.io", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        self.assertEqual(calls, [("incident", "io")])

    def test_domain_check_reports_failure_when_required_tld_is_taken(self) -> None:
        config = ValidationConfig(checks=["domain"], required_domain_tlds="com,de")

        def fake_rdap_probe(name: str, tld: str) -> dict[str, object]:
            self.assertEqual(name, "vantora")
            return {
                "availability": {"com": "no", "de": "yes", "ch": "yes"}[tld],
                "status_code": {"com": 200, "de": 404, "ch": 404}[tld],
                "error_kind": ErrorKind.NONE.value,
                "headers": {},
                "retry_after_s": None,
                "retryable": False,
            }

        with mock.patch("brandpipe.validation_checks.rdap_probe", side_effect=fake_rdap_probe):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "domain_unavailable_com")

    def test_domain_check_marks_unknown_pool_as_unavailable(self) -> None:
        config = ValidationConfig(checks=["domain"])

        def fake_rdap_probe(name: str, tld: str) -> dict[str, object]:
            del name
            return {
                "availability": {"com": "unknown", "de": "no", "ch": "unknown"}[tld],
                "status_code": {"com": 429, "de": 200, "ch": 504}[tld],
                "error_kind": ErrorKind.RATE_LIMITED.value if tld == "com" else ErrorKind.TIMEOUT.value,
                "headers": {"Retry-After": "5"} if tld == "com" else {},
                "retry_after_s": 5.0 if tld == "com" else None,
                "retryable": tld in {"com", "ch"},
            }

        with mock.patch("brandpipe.validation_checks.rdap_probe", side_effect=fake_rdap_probe):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.UNAVAILABLE)
        self.assertEqual(results[0].reason, "domain_unknown_com-ch")

    def test_package_check_detects_registry_collision(self) -> None:
        config = ValidationConfig(checks=["package"])

        def fake_package_probe(registry: str, name: str) -> dict[str, object]:
            self.assertEqual(name, "vantora")
            exists = "yes" if registry == "pypi" else "no"
            return {
                "exists": exists,
                "status_code": 200 if exists == "yes" else 404,
                "error_kind": ErrorKind.NONE.value,
                "headers": {},
                "retry_after_s": None,
                "retryable": False,
            }

        with mock.patch("brandpipe.validation_checks.package_probe", side_effect=fake_package_probe):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "package_collision_pypi")

    def test_web_check_normalizes_legacy_provider_order(self) -> None:
        config = ValidationConfig(checks=["web"], web_search_order="brave,google_cse,duckduckgo")
        self.assertEqual(resolve_web_search_order(config), ["brave", "browser_google"])

    def test_web_check_uses_brave_first_when_exact_collision_found(self) -> None:
        config = ValidationConfig(checks=["web"])
        with (
            mock.patch(
                "brandpipe.validation_checks.brave_signal",
                return_value={
                    "ok": True,
                    "source": "brave",
                    "exact_hits": 1,
                    "near_hits": 0,
                    "result_count": 3,
                    "sample_domains": ["vantora.example"],
                    "first_hit_exact": False,
                },
            ),
            mock.patch("brandpipe.validation_checks.browser_google_signal") as browser_mock,
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "web_exact_collision")
        browser_mock.assert_not_called()

    def test_web_check_uses_browser_google_when_brave_is_unavailable(self) -> None:
        config = ValidationConfig(checks=["web"])
        with (
            mock.patch(
                "brandpipe.validation_checks.brave_signal",
                return_value={
                    "ok": False,
                    "source": "brave",
                    "error_kind": ErrorKind.RATE_LIMITED.value,
                    "error": "rate_limited",
                    "status_code": 429,
                    "headers": {"Retry-After": "12"},
                    "retry_after_s": 12.0,
                    "retryable": True,
                },
            ),
            mock.patch(
                "brandpipe.validation_checks.browser_google_signal",
                return_value={
                    "ok": True,
                    "source": "browser_google",
                    "exact_hits": 1,
                    "near_hits": 0,
                    "result_count": 3,
                    "sample_domains": ["vantora.example"],
                    "first_hit_exact": False,
                    "final_url": "https://www.google.com/search?q=vantora",
                    "page_title": "vantora - Google Search",
                    "state": "results",
                },
            ),
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].details["provider"], "browser_google")

    def test_web_check_returns_unavailable_without_pending_fallback(self) -> None:
        config = ValidationConfig(checks=["web"])
        with (
            mock.patch(
                "brandpipe.validation_checks.brave_signal",
                return_value={
                    "ok": False,
                    "source": "brave",
                    "error_kind": ErrorKind.CONFIG.value,
                    "error": "brave_api_key_missing",
                    "status_code": None,
                    "headers": {},
                    "retry_after_s": None,
                    "retryable": False,
                },
            ),
            mock.patch(
                "brandpipe.validation_checks.browser_google_signal",
                return_value={
                    "ok": False,
                    "source": "browser_google",
                    "state": "browser_boot_failed",
                    "error": "playwright_missing",
                },
            ),
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.UNAVAILABLE)
        self.assertEqual(results[0].reason, "web_search_unavailable")

    def test_app_store_check_is_browser_only(self) -> None:
        config = ValidationConfig(checks=["app_store"], store_countries="us")
        with mock.patch(
            "brandpipe.validation_checks.app_store_browser_signal",
            return_value={
                "ok": True,
                "source": "browser_app_store",
                "state": "results",
                "result_count": 0,
                "exact": False,
                "final_url": "https://apps.apple.com/us/search?term=vantora",
                "title": 'Results for "vantora"',
            },
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        self.assertEqual(results[0].details["final_urls"]["us"], "https://apps.apple.com/us/search?term=vantora")

    def test_app_store_uses_surface_query_for_spaced_name(self) -> None:
        config = ValidationConfig(checks=["app_store"], store_countries="us")
        captured_queries: list[str] = []

        def fake_browser_signal(name: str, country: str, *, config: ValidationConfig) -> dict[str, object]:
            del config
            captured_queries.append(f"{country}:{name}")
            return {
                "ok": True,
                "source": "browser_app_store",
                "state": "results",
                "result_count": 0,
                "exact": False,
                "final_url": "https://apps.apple.com/us/search?term=xnview%20mp",
                "title": 'Results for "XnView MP"',
            }

        with mock.patch("brandpipe.validation_checks.app_store_browser_signal", side_effect=fake_browser_signal):
            results = validate_candidate(name="XnView MP", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        self.assertEqual(captured_queries, ["us:XnView MP"])

    def test_app_store_unavailable_stays_unavailable_without_http_fallback(self) -> None:
        config = ValidationConfig(checks=["app_store"], store_countries="us")
        with mock.patch(
            "brandpipe.validation_checks.app_store_browser_signal",
            return_value={
                "ok": False,
                "source": "browser_app_store",
                "state": "timeout",
                "error": "page_timeout",
            },
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.UNAVAILABLE)
        self.assertEqual(results[0].reason, "app_store_unknown_us")

    def test_company_check_detects_exact_active_hit(self) -> None:
        config = ValidationConfig(checks=["company"])
        with mock.patch(
            "brandpipe.validation_checks.company_house_signal",
            return_value={
                "ok": True,
                "configured": True,
                "result_count": 3,
                "exact_active_hits": 1,
                "near_active_hits": 0,
                "sample_titles": ["Vantora GmbH"],
            },
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "company_exact_active")

    def test_company_check_marks_unconfigured_api_as_unavailable(self) -> None:
        config = ValidationConfig(checks=["company"])
        with mock.patch(
            "brandpipe.validation_checks.company_house_signal",
            return_value={
                "ok": False,
                "configured": False,
                "result_count": 0,
                "exact_active_hits": 0,
                "near_active_hits": 0,
                "sample_titles": [],
                "error_kind": ErrorKind.CONFIG.value,
            },
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.UNAVAILABLE)
        self.assertEqual(results[0].reason, "company_unconfigured")

    def test_tm_check_uses_tmview_probe_and_warns_on_near_hits(self) -> None:
        config = ValidationConfig(checks=["tm"], tmview_profile_dir="/tmp/tmview-profile")
        with mock.patch(
            "brandpipe.tmview.probe_names",
            return_value=[
                mock.Mock(
                    query_ok=True,
                    url="https://example.test",
                    result_count=3,
                    exact_hits=0,
                    near_hits=2,
                    active_exact_hits=0,
                    inactive_exact_hits=0,
                    unknown_exact_hits=0,
                    sample_text="Vantoro",
                    exact_sample_text="",
                    error="",
                    state="results",
                )
            ],
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.WARN)
        self.assertEqual(results[0].reason, "tm_near_review")

    def test_tm_check_blocks_surface_exact_active_collision(self) -> None:
        config = ValidationConfig(checks=["tm"], tmview_profile_dir="/tmp/tmview-profile")
        with mock.patch(
            "brandpipe.tmview.probe_names",
            return_value=[
                mock.Mock(
                    query_ok=True,
                    url="https://example.test",
                    result_count=1,
                    exact_hits=1,
                    near_hits=0,
                    active_exact_hits=1,
                    inactive_exact_hits=0,
                    unknown_exact_hits=0,
                    surface_exact_hits=1,
                    normalized_exact_hits=0,
                    surface_active_exact_hits=1,
                    normalized_active_exact_hits=0,
                    query_sequence="incident.io",
                    sample_text="INCIDENT.IO",
                    exact_sample_text="INCIDENT.IO",
                    error="",
                    state="results",
                )
            ],
        ):
            results = validate_candidate(name="incident.io", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "tm_surface_exact_active_collision")

    def test_tm_check_warns_on_normalized_exact_collision(self) -> None:
        config = ValidationConfig(checks=["tm"], tmview_profile_dir="/tmp/tmview-profile")
        with mock.patch(
            "brandpipe.tmview.probe_names",
            return_value=[
                mock.Mock(
                    query_ok=True,
                    url="https://example.test",
                    result_count=1,
                    exact_hits=1,
                    near_hits=0,
                    active_exact_hits=1,
                    inactive_exact_hits=0,
                    unknown_exact_hits=0,
                    surface_exact_hits=0,
                    normalized_exact_hits=1,
                    surface_active_exact_hits=0,
                    normalized_active_exact_hits=1,
                    query_sequence="incident.io,incidentio",
                    sample_text="Incidentio",
                    exact_sample_text="Incidentio",
                    error="",
                    state="results",
                )
            ],
        ):
            results = validate_candidate(name="incident.io", config=config)

        self.assertEqual(results[0].status, ResultStatus.WARN)
        self.assertEqual(results[0].reason, "tm_normalized_exact_review")

    def test_unsupported_checks_are_explicitly_marked(self) -> None:
        config = ValidationConfig(checks=["tm_cheap", "mystery"])
        results = validate_candidate(name="vantora", config=config)

        self.assertEqual([item.status for item in results], [ResultStatus.UNSUPPORTED, ResultStatus.UNSUPPORTED])
        self.assertEqual([item.reason for item in results], ["tm_cheap_check_unavailable", "validation_check_unknown"])

    def test_social_rate_limit_is_unavailable_not_taken(self) -> None:
        config = ValidationConfig(checks=["social"], social_unavailable_fail_threshold=3)
        rate_limited = HttpResponse(
            ok=False,
            url="https://github.com/vantora",
            status_code=429,
            text="",
            headers={"Retry-After": "30"},
            error_kind=ErrorKind.RATE_LIMITED,
            error_message="too_many_requests",
            retry_after_s=30.0,
        )
        with mock.patch("brandpipe.validation_checks.fetch_status", return_value=rate_limited):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.UNAVAILABLE)
        self.assertEqual(results[0].reason, "social_check_unknown")

    def test_social_crowding_is_advisory_not_blocking(self) -> None:
        config = ValidationConfig(checks=["social"], social_unavailable_fail_threshold=3)

        def fake_probe(url: str) -> dict[str, object]:
            del url
            return {
                "availability": "no",
                "status_code": 200,
                "error_kind": ErrorKind.NONE.value,
                "headers": {},
                "retry_after_s": None,
                "retryable": False,
            }

        with mock.patch("brandpipe.validation_checks.social_handle_probe", side_effect=fake_probe):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.WARN)
        self.assertEqual(results[0].reason, "social_handle_crowded")

    def test_blocker_failure_skips_expensive_follow_ups(self) -> None:
        config = ValidationConfig(checks=["domain", "web", "app_store"])

        with mock.patch.dict(
            "brandpipe.validation.CHECK_PROBERS",
            {
                "domain": lambda *, name, config: mock.Mock(
                    candidate_result=mock.Mock(
                        check_name="domain",
                        status=ResultStatus.FAIL,
                        score_delta=-18.0,
                        reason="domain_unavailable_com",
                        details={},
                    ),
                    check_name="domain",
                ),
                "web": lambda *, name, config: self.fail("web should have been skipped"),
                "app_store": lambda *, name, config: self.fail("app_store should have been skipped"),
            },
        ):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual([item.check_name for item in results], ["domain", "web", "app_store"])
        self.assertEqual(results[1].status, ResultStatus.SKIPPED)
        self.assertEqual(results[1].reason, "skipped_due_to_blocker")
        self.assertEqual(results[2].status, ResultStatus.SKIPPED)


if __name__ == "__main__":
    unittest.main()
