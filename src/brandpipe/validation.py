from __future__ import annotations

from .models import CandidateResult, ResultStatus, ValidationConfig
from .validation_checks import (
    probe_app_store,
    probe_company,
    probe_domain,
    probe_package,
    probe_social,
    probe_tm,
    probe_tm_cheap,
    probe_web,
)
from .validation_runtime import EARLY_EXIT_CHECKS, ProbeResult


CHECK_PROBERS = {
    "domain": probe_domain,
    "company": probe_company,
    "web": probe_web,
    "tm": probe_tm,
    "tm_cheap": probe_tm_cheap,
    "app_store": probe_app_store,
    "package": probe_package,
    "social": probe_social,
}


def skipped_result(*, check_name: str, blocker_check: str) -> ProbeResult:
    return ProbeResult(
        candidate_result=CandidateResult(
            check_name=check_name,
            status=ResultStatus.SKIPPED,
            score_delta=0.0,
            reason="skipped_due_to_blocker",
            details={"blocked_by": blocker_check, "retryable": False, "error_kind": "none"},
        )
    )


def probe_check(*, check_name: str, name: str, config: ValidationConfig) -> ProbeResult:
    runner = CHECK_PROBERS.get(check_name)
    if runner is None:
        return ProbeResult(
            candidate_result=CandidateResult(
                check_name=check_name,
                status=ResultStatus.UNSUPPORTED,
                score_delta=-1.0,
                reason="validation_check_unknown",
                details={"check_name": check_name, "retryable": False, "error_kind": "none"},
            )
        )
    return runner(name=name, config=config)


def probe_candidate(
    *,
    name: str,
    config: ValidationConfig,
    start_index: int = 0,
    prior_results: list[ProbeResult] | None = None,
) -> list[ProbeResult]:
    probes: list[ProbeResult] = list(prior_results or [])
    blocker_check = next(
        (
            item.check_name
            for item in probes
            if item.candidate_result.status == ResultStatus.FAIL and item.check_name in EARLY_EXIT_CHECKS
        ),
        "",
    )
    for index, check_name in enumerate(config.checks):
        if index < max(0, int(start_index)):
            continue
        if blocker_check:
            probes.append(skipped_result(check_name=check_name, blocker_check=blocker_check))
            continue
        probe = probe_check(check_name=check_name, name=name, config=config)
        probes.append(probe)
        if probe.candidate_result.status == ResultStatus.FAIL and check_name in EARLY_EXIT_CHECKS:
            blocker_check = check_name
    return probes


def validate_candidate(*, name: str, config: ValidationConfig) -> list[CandidateResult]:
    return [probe.candidate_result for probe in probe_candidate(name=name, config=config)]
