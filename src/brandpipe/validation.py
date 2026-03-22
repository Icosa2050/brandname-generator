from __future__ import annotations

import time

from .models import CandidateResult, ResultStatus, ValidationConfig
from .validation_checks import (
    check_app_store,
    check_company,
    check_domain,
    check_package,
    check_social,
    check_tm,
    check_tm_cheap,
    check_web,
)


CHECK_RUNNERS = {
    "domain": check_domain,
    "company": check_company,
    "web": check_web,
    "tm": check_tm,
    "tm_cheap": check_tm_cheap,
    "app_store": check_app_store,
    "package": check_package,
    "social": check_social,
}


def _replace_result(results: list[CandidateResult], updated: CandidateResult) -> list[CandidateResult]:
    replaced: list[CandidateResult] = []
    did_replace = False
    for item in results:
        if item.check_name == updated.check_name and not did_replace:
            replaced.append(updated)
            did_replace = True
            continue
        replaced.append(item)
    if not did_replace:
        replaced.append(updated)
    return replaced


def _stabilize_web_unavailable(*, name: str, config: ValidationConfig, results: list[CandidateResult]) -> list[CandidateResult]:
    web_result = next((item for item in results if item.check_name == "web"), None)
    if web_result is None:
        return results
    if web_result.status != ResultStatus.UNAVAILABLE or web_result.reason != "web_search_unavailable":
        return results
    if any(
        item.check_name != "web" and item.status in {ResultStatus.FAIL, ResultStatus.WARN, ResultStatus.UNAVAILABLE}
        for item in results
    ):
        return results

    latest = web_result
    retry_attempts = max(0, int(config.web_retry_attempts))
    for attempt in range(retry_attempts):
        delay_s = float(config.web_retry_backoff_s) * float(attempt + 1)
        if delay_s > 0:
            time.sleep(delay_s)
        latest = check_web(name=name, config=config)
        if latest.status != ResultStatus.UNAVAILABLE or latest.reason != "web_search_unavailable":
            break

    if retry_attempts > 0:
        retry_details = dict(latest.details)
        retry_details["retried_web_attempts"] = retry_attempts
        retry_details["initial_reason"] = web_result.reason
        latest = CandidateResult(
            check_name=latest.check_name,
            status=latest.status,
            score_delta=latest.score_delta,
            reason=latest.reason,
            details=retry_details,
        )

    if latest.status == ResultStatus.UNAVAILABLE and latest.reason == "web_search_unavailable":
        pending_details = dict(latest.details)
        pending_details["pending_review"] = True
        latest = CandidateResult(
            check_name="web",
            status=ResultStatus.WARN,
            score_delta=-2.0,
            reason="web_check_pending",
            details=pending_details,
        )

    return _replace_result(results, latest)


def validate_candidate(*, name: str, config: ValidationConfig) -> list[CandidateResult]:
    results: list[CandidateResult] = []
    for check_name in config.checks:
        runner = CHECK_RUNNERS.get(check_name)
        if runner is None:
            results.append(
                CandidateResult(
                    check_name=check_name,
                    status=ResultStatus.UNSUPPORTED,
                    score_delta=-1.0,
                    reason="validation_check_unknown",
                    details={"check_name": check_name},
                )
            )
            continue
        results.append(runner(name=name, config=config))
    return _stabilize_web_unavailable(name=name, config=config, results=results)
