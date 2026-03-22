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

from brandpipe.models import ResultStatus, ValidationConfig
from brandpipe.validation import validate_candidate


class ValidationTests(unittest.TestCase):
    def test_domain_check_reports_failure_when_required_tld_is_taken(self) -> None:
        config = ValidationConfig(checks=["domain"], required_domain_tlds="com,de")

        def fake_rdap_available(name: str, tld: str) -> str:
            self.assertEqual(name, "vantora")
            return {"com": "no", "de": "yes", "ch": "yes"}[tld]

        with mock.patch("brandpipe.validation_checks.rdap_available", side_effect=fake_rdap_available):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "domain_unavailable_com")

    def test_domain_check_default_any_mode_passes_when_one_core_tld_is_available(self) -> None:
        config = ValidationConfig(checks=["domain"])

        def fake_rdap_available(name: str, tld: str) -> str:
            self.assertEqual(name, "vantora")
            return {"com": "no", "de": "yes", "ch": "no"}[tld]

        with mock.patch("brandpipe.validation_checks.rdap_available", side_effect=fake_rdap_available):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        self.assertEqual(results[0].details["mode"], "default_any")

    def test_domain_check_default_any_mode_fails_when_all_core_tlds_are_taken(self) -> None:
        config = ValidationConfig(checks=["domain"])

        with mock.patch("brandpipe.validation_checks.rdap_available", return_value="no"):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "domain_unavailable_default_pool")

    def test_domain_check_default_any_mode_is_unavailable_when_none_are_known_available(self) -> None:
        config = ValidationConfig(checks=["domain"])

        def fake_rdap_available(name: str, tld: str) -> str:
            return {"com": "unknown", "de": "no", "ch": "unknown"}[tld]

        with mock.patch("brandpipe.validation_checks.rdap_available", side_effect=fake_rdap_available):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.UNAVAILABLE)
        self.assertEqual(results[0].reason, "domain_unknown_com-ch")

    def test_package_check_detects_registry_collision(self) -> None:
        config = ValidationConfig(checks=["package"])
        with mock.patch("brandpipe.validation_checks.package_exists_on_pypi", return_value="yes"):
            with mock.patch("brandpipe.validation_checks.package_exists_on_npm", return_value="no"):
                results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "package_collision_pypi")

    def test_web_check_is_unavailable_without_google_configuration(self) -> None:
        config = ValidationConfig(
            checks=["web"],
            web_brave_api_env="MISSING_BRAVE_API_KEY",
            web_google_api_env="MISSING_WEB_API_KEY",
            web_google_cx_env="MISSING_WEB_CX",
            web_retry_attempts=0,
        )
        with mock.patch("brandpipe.validation_checks.brave_signal", return_value={"ok": False, "source": "brave"}):
            with mock.patch(
                "brandpipe.validation_checks.google_cse_signal",
                return_value={"ok": False, "source": "google_cse"},
            ):
                with mock.patch(
                    "brandpipe.validation_checks.duckduckgo_search_signal",
                    return_value={"ok": False, "source": "duckduckgo"},
                ):
                    results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.WARN)
        self.assertEqual(results[0].reason, "web_check_pending")
        self.assertTrue(results[0].details["pending_review"])

    def test_web_check_uses_brave_first_when_available(self) -> None:
        config = ValidationConfig(checks=["web"])
        with mock.patch(
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
        ):
            with mock.patch("brandpipe.validation_checks.google_cse_signal") as google_mock:
                with mock.patch("brandpipe.validation_checks.duckduckgo_search_signal") as ddg_mock:
                    results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "web_exact_collision")
        google_mock.assert_not_called()
        ddg_mock.assert_not_called()

    def test_web_check_uses_secondary_search_signal_after_provider_failure(self) -> None:
        config = ValidationConfig(checks=["web"])
        with mock.patch("brandpipe.validation_checks.brave_signal", return_value={"ok": False, "source": "brave"}):
            with mock.patch(
                "brandpipe.validation_checks.google_cse_signal",
                return_value={"ok": False, "source": "google_cse"},
            ):
                with mock.patch(
                    "brandpipe.validation_checks.duckduckgo_search_signal",
                    return_value={
                        "ok": True,
                        "source": "duckduckgo",
                        "exact_hits": 1,
                        "near_hits": 0,
                        "result_count": 3,
                        "sample_domains": ["vantora.example"],
                        "first_hit_exact": False,
                    },
                ):
                    results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.FAIL)
        self.assertEqual(results[0].reason, "web_exact_collision")

    def test_web_check_uses_browser_google_provider_when_requested(self) -> None:
        config = ValidationConfig(
            checks=["web"],
            web_search_order="browser_google",
            web_browser_profile_dir="/tmp/browser-profile",
        )
        with mock.patch(
            "brandpipe.validation_checks.browser_google_signal",
            return_value={
                "ok": True,
                "source": "browser_google",
                "exact_hits": 0,
                "near_hits": 0,
                "result_count": 4,
                "sample_domains": ["example.org"],
                "first_hit_exact": False,
                "final_url": "https://www.google.com/search?q=vantora",
                "page_title": "vantora - Google Search",
            },
        ) as browser_mock:
            with mock.patch("brandpipe.validation_checks.brave_signal") as brave_mock:
                results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        self.assertEqual(results[0].details["provider"], "browser_google")
        self.assertEqual(results[0].details["final_url"], "https://www.google.com/search?q=vantora")
        browser_mock.assert_called_once()
        brave_mock.assert_not_called()

    def test_web_check_does_not_fallback_when_primary_returns_zero_results(self) -> None:
        config = ValidationConfig(checks=["web"])
        with mock.patch(
            "brandpipe.validation_checks.brave_signal",
            return_value={
                "ok": True,
                "source": "brave",
                "exact_hits": 0,
                "near_hits": 0,
                "result_count": 0,
                "sample_domains": [],
                "first_hit_exact": False,
            },
        ):
            with mock.patch("brandpipe.validation_checks.google_cse_signal") as google_mock:
                with mock.patch("brandpipe.validation_checks.duckduckgo_search_signal") as ddg_mock:
                    results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        google_mock.assert_not_called()
        ddg_mock.assert_not_called()

    def test_web_check_single_near_hit_is_now_treated_as_pass(self) -> None:
        config = ValidationConfig(checks=["web"])
        with mock.patch(
            "brandpipe.validation_checks.brave_signal",
            return_value={
                "ok": True,
                "source": "brave",
                "exact_hits": 0,
                "near_hits": 1,
                "result_count": 1,
                "sample_domains": ["iiwiki.us"],
                "first_hit_exact": False,
            },
        ):
            results = validate_candidate(name="lancorda", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        self.assertEqual(results[0].reason, "")

    def test_web_check_two_near_hits_is_warn(self) -> None:
        config = ValidationConfig(checks=["web"])
        with mock.patch(
            "brandpipe.validation_checks.brave_signal",
            return_value={
                "ok": True,
                "source": "brave",
                "exact_hits": 0,
                "near_hits": 2,
                "result_count": 2,
                "sample_domains": ["example-one.test", "example-two.test"],
                "first_hit_exact": False,
            },
        ):
            results = validate_candidate(name="lancorda", config=config)

        self.assertEqual(results[0].status, ResultStatus.WARN)
        self.assertEqual(results[0].reason, "web_near_warning")

    def test_unsupported_checks_are_explicitly_marked(self) -> None:
        config = ValidationConfig(checks=["company", "tm", "tm_cheap", "mystery"])
        results = validate_candidate(name="vantora", config=config)

        self.assertEqual([item.status for item in results], [ResultStatus.UNSUPPORTED] * 4)
        self.assertEqual(
            [item.reason for item in results],
            [
                "company_check_unavailable",
                "tm_check_unavailable",
                "tm_cheap_check_unavailable",
                "validation_check_unknown",
            ],
        )

    def test_social_rate_limit_counts_as_unavailable_not_taken(self) -> None:
        config = ValidationConfig(checks=["social"], social_unavailable_fail_threshold=3)
        with mock.patch("brandpipe.validation_checks.fetch_status", return_value=429):
            results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.UNAVAILABLE)
        self.assertEqual(results[0].reason, "social_check_unknown")

    def test_web_unavailable_retries_and_promotes_to_pending_warning(self) -> None:
        config = ValidationConfig(checks=["domain", "web"], web_retry_attempts=2, web_retry_backoff_s=0.0)
        unavailable = [
            mock.Mock(
                check_name="web",
                status=ResultStatus.UNAVAILABLE,
                score_delta=-2.0,
                reason="web_search_unavailable",
                details={"sources": []},
            ),
            mock.Mock(
                check_name="web",
                status=ResultStatus.UNAVAILABLE,
                score_delta=-2.0,
                reason="web_search_unavailable",
                details={"sources": []},
            ),
        ]

        with mock.patch("brandpipe.validation_checks.rdap_available", return_value="yes"):
            web_runner = mock.Mock(side_effect=unavailable)
            with mock.patch.dict("brandpipe.validation.CHECK_RUNNERS", {"web": web_runner}):
                with mock.patch("brandpipe.validation.check_web", side_effect=unavailable):
                    with mock.patch("brandpipe.validation.time.sleep") as sleep_mock:
                        results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        self.assertEqual(results[1].status, ResultStatus.WARN)
        self.assertEqual(results[1].reason, "web_check_pending")
        self.assertEqual(results[1].details["retried_web_attempts"], 2)
        self.assertTrue(results[1].details["pending_review"])
        self.assertEqual(sleep_mock.call_count, 0)

    def test_web_unavailable_retry_can_recover_to_pass(self) -> None:
        config = ValidationConfig(checks=["domain", "web"], web_retry_attempts=1, web_retry_backoff_s=0.0)
        first = mock.Mock(
            check_name="web",
            status=ResultStatus.UNAVAILABLE,
            score_delta=-2.0,
            reason="web_search_unavailable",
            details={"sources": []},
        )
        recovered = mock.Mock(
            check_name="web",
            status=ResultStatus.PASS,
            score_delta=0.0,
            reason="",
            details={"provider": "brave"},
        )

        with mock.patch("brandpipe.validation_checks.rdap_available", return_value="yes"):
            web_runner = mock.Mock(side_effect=[first, recovered])
            with mock.patch.dict("brandpipe.validation.CHECK_RUNNERS", {"web": web_runner}):
                with mock.patch("brandpipe.validation.check_web", side_effect=[recovered]):
                    results = validate_candidate(name="vantora", config=config)

        self.assertEqual(results[0].status, ResultStatus.PASS)
        self.assertEqual(results[1].status, ResultStatus.PASS)
        self.assertEqual(results[1].details["retried_web_attempts"], 1)


if __name__ == "__main__":
    unittest.main()
