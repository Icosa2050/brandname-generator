from __future__ import annotations

from dataclasses import dataclass, field

from .models import CandidateResult, ErrorKind, ResultStatus


DEFAULT_VALIDATION_ORDER: tuple[str, ...] = (
    "domain",
    "package",
    "company",
    "web",
    "app_store",
    "social",
    "tm",
)
BLOCKER_CHECKS = frozenset({"domain", "package", "company", "web", "app_store", "tm"})
EARLY_EXIT_CHECKS = frozenset({"domain", "package", "company"})
RETRYABLE_ERROR_KINDS = frozenset(
    {
        ErrorKind.RATE_LIMITED,
        ErrorKind.TIMEOUT,
        ErrorKind.TRANSPORT,
        ErrorKind.BROWSER,
    }
)


@dataclass(frozen=True)
class ProbeResult:
    candidate_result: CandidateResult
    error_kind: ErrorKind = ErrorKind.NONE
    retryable: bool = False
    http_status: int | None = None
    headers: dict[str, str] = field(default_factory=dict)
    retry_after_s: float | None = None
    transport: str = ""
    evidence: dict[str, object] = field(default_factory=dict)

    @property
    def check_name(self) -> str:
        return self.candidate_result.check_name

    @classmethod
    def unavailable(
        cls,
        *,
        check_name: str,
        score_delta: float,
        reason: str,
        error_kind: ErrorKind,
        retryable: bool,
        transport: str = "",
        evidence: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        http_status: int | None = None,
        retry_after_s: float | None = None,
        details: dict[str, object] | None = None,
    ) -> "ProbeResult":
        merged_details = dict(details or {})
        merged_details.setdefault("error_kind", error_kind.value)
        merged_details.setdefault("retryable", bool(retryable))
        if transport:
            merged_details.setdefault("transport", transport)
        if http_status is not None:
            merged_details.setdefault("http_status", int(http_status))
        if retry_after_s is not None:
            merged_details.setdefault("retry_after_s", float(retry_after_s))
        if headers:
            merged_details.setdefault("http_headers", dict(headers))
        if evidence:
            merged_details.setdefault("evidence", dict(evidence))
        return cls(
            candidate_result=CandidateResult(
                check_name=check_name,
                status=ResultStatus.UNAVAILABLE,
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

    def attempt_payload(self) -> dict[str, object]:
        return {
            "check_name": self.candidate_result.check_name,
            "status": self.candidate_result.status.value,
            "reason": self.candidate_result.reason,
            "score_delta": self.candidate_result.score_delta,
            "error_kind": self.error_kind.value,
            "retryable": bool(self.retryable),
            "http_status": self.http_status,
            "headers": dict(self.headers),
            "retry_after_s": self.retry_after_s,
            "transport": self.transport,
            "evidence": dict(self.evidence),
            "details": dict(self.candidate_result.details),
        }


@dataclass(frozen=True)
class AttemptRecord:
    run_id: int
    candidate_id: int
    check_name: str
    attempt_number: int
    status: str
    reason: str
    error_kind: str
    retryable: bool
    http_status: int | None
    retry_after_s: float | None
    headers: dict[str, str]
    evidence: dict[str, object]
    details: dict[str, object]
