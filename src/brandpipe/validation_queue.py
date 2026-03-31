from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
import hashlib
import json
import time

from . import db
from .models import ErrorKind, JobStatus, ResultStatus, RunStatus, ValidationConfig
from .validation import probe_check, skipped_result
from .validation_runtime import DEFAULT_VALIDATION_ORDER, EARLY_EXIT_CHECKS, ProbeResult


SHORTLIST_RUN_PREFIX = "shortlist_validation:"
MAX_RETRY_ATTEMPTS = 3


def shortlist_fingerprint(*, names: Iterable[str], config: ValidationConfig) -> str:
    payload = {
        "names": [str(name).strip() for name in names if str(name).strip()],
        "checks": list(config.checks or DEFAULT_VALIDATION_ORDER),
        "required_domain_tlds": str(config.required_domain_tlds or ""),
        "store_countries": str(config.store_countries or ""),
        "company_top": int(config.company_top),
        "social_unavailable_fail_threshold": int(config.social_unavailable_fail_threshold),
        "web_search_order": str(config.web_search_order or ""),
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validation_state_title(*, fingerprint: str) -> str:
    return f"{SHORTLIST_RUN_PREFIX}{fingerprint}"


def _parse_iso(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _iso_after(seconds: float) -> str:
    target = datetime.now(timezone.utc) + timedelta(seconds=max(0.0, float(seconds)))
    return target.strftime("%Y-%m-%dT%H:%M:%SZ")


def _retry_delay_s(probe: ProbeResult, *, attempt_count: int) -> float:
    if probe.retry_after_s is not None:
        return max(0.0, float(probe.retry_after_s))
    error_kind = probe.error_kind.value
    if error_kind == "rate_limited":
        return min(60.0, 10.0 * max(1, attempt_count))
    if error_kind == "timeout":
        return min(20.0, 4.0 * max(1, attempt_count))
    if error_kind == "browser":
        return min(20.0, 5.0 * max(1, attempt_count))
    return min(15.0, 2.0 * max(1, attempt_count))


def _next_due_retry_s(conn, *, run_id: int) -> float | None:
    jobs = db.list_validation_jobs(conn, run_id=run_id)
    retry_rows = [
        row
        for row in jobs
        if str(row["status"]) == JobStatus.RETRY_WAIT.value and str(row["next_retry_at"] or "").strip()
    ]
    if not retry_rows:
        return None
    now = datetime.now(timezone.utc)
    earliest = min(_parse_iso(str(row["next_retry_at"])) for row in retry_rows)
    return max(0.0, (earliest - now).total_seconds())


def _existing_result_keys(conn, *, candidate_id: int) -> set[str]:
    return {
        str(row["result_key"])
        for row in db.fetch_results_for_candidate(conn, candidate_id=candidate_id)
    }


def _attempt_count_for_check(conn, *, job_id: int, check_name: str) -> int:
    attempts = db.fetch_validation_attempts(conn, job_id=job_id)
    return sum(1 for row in attempts if str(row["check_name"]) == check_name)


def _upsert_probe_result(conn, *, candidate_id: int, probe: ProbeResult) -> None:
    result = probe.candidate_result
    db.upsert_result(
        conn,
        candidate_id=candidate_id,
        result_key=result.check_name,
        status=result.status.value,
        score_delta=result.score_delta,
        reason=result.reason,
        details=result.details,
    )


def _record_probe_attempt(conn, *, job_row, probe: ProbeResult, attempt_number: int) -> None:
    payload = probe.attempt_payload()
    db.record_validation_attempt(
        conn,
        job_id=int(job_row["id"]),
        run_id=int(job_row["run_id"]),
        candidate_id=int(job_row["candidate_id"]),
        check_name=probe.check_name,
        attempt_number=attempt_number,
        status=str(payload["status"]),
        reason=str(payload["reason"]),
        error_kind=str(payload["error_kind"]),
        retryable=bool(payload["retryable"]),
        http_status=payload["http_status"],
        retry_after_s=payload["retry_after_s"],
        headers=dict(payload["headers"]),
        evidence=dict(payload["evidence"]),
        details=dict(payload["details"]),
    )


def _unexpected_probe_result(*, check_name: str, exc: Exception) -> ProbeResult:
    return ProbeResult.unavailable(
        check_name=check_name,
        score_delta=-2.0,
        reason="validation_probe_exception",
        error_kind=ErrorKind.UNEXPECTED,
        retryable=False,
        transport="validation_queue",
        evidence={
            "error_class": exc.__class__.__name__,
            "error_message": str(exc),
        },
    )


def prepare_shortlist_run(
    conn,
    *,
    candidate_names: list[str],
    config: ValidationConfig,
    fingerprint: str,
) -> tuple[int, bool]:
    title = validation_state_title(fingerprint=fingerprint)
    existing = db.find_latest_run_by_title(conn, title=title)
    if existing is not None:
        return int(existing["id"]), False
    run_id = db.create_run(
        conn,
        title=title,
        brief={"shortlist_fingerprint": fingerprint, "input_count": len(candidate_names)},
        config={"validation": config.__dict__, "shortlist_fingerprint": fingerprint},
    )
    db.add_candidates(
        conn,
        run_id=run_id,
        names=candidate_names,
        source_kind="shortlist",
        source_detail=fingerprint,
    )
    candidate_rows = db.list_candidates(conn, run_id=run_id)
    name_to_id = {str(row["name"]): int(row["id"]) for row in candidate_rows}
    ordered_ids = [name_to_id[name] for name in candidate_names if name in name_to_id]
    db.ensure_validation_jobs(
        conn,
        run_id=run_id,
        ordered_candidate_ids=ordered_ids,
        shortlist_fingerprint=fingerprint,
    )
    conn.commit()
    return run_id, True


def find_resume_run(conn, *, fingerprint: str) -> int | None:
    title = validation_state_title(fingerprint=fingerprint)
    row = db.find_latest_run_by_title(conn, title=title)
    if row is None:
        return None
    return int(row["id"])


def detect_state_mismatch(conn, *, fingerprint: str) -> bool:
    matching_title = validation_state_title(fingerprint=fingerprint)
    runs = db.list_runs(conn, limit=20)
    if not runs:
        return False
    return all(str(row["title"]) != matching_title for row in runs)


def run_validation_jobs(
    conn,
    *,
    run_id: int,
    config: ValidationConfig,
    sleep_fn=time.sleep,
    mark_run_complete: bool = False,
) -> dict[str, object]:
    checks = list(config.checks or DEFAULT_VALIDATION_ORDER)
    db.set_run_state(conn, run_id=run_id, status=RunStatus.RUNNING.value, current_step="validation_queue")
    conn.commit()
    total_retry_waits = 0
    while True:
        now = db.now_iso()
        job_row = db.claim_next_validation_job(conn, run_id=run_id, now=now)
        if job_row is None:
            counts = db.count_validation_jobs(conn, run_id=run_id)
            pending = counts.get(JobStatus.PENDING.value, 0)
            running = counts.get(JobStatus.RUNNING.value, 0)
            retry_wait = counts.get(JobStatus.RETRY_WAIT.value, 0)
            if pending == 0 and running == 0 and retry_wait == 0:
                break
            next_due = _next_due_retry_s(conn, run_id=run_id)
            if next_due is None:
                break
            sleep_fn(min(max(next_due, 0.0), 1.0))
            total_retry_waits += 1
            continue

        candidate_row = db.get_candidate(conn, candidate_id=int(job_row["candidate_id"]))
        if candidate_row is None:
            db.update_validation_job(
                conn,
                job_id=int(job_row["id"]),
                status=JobStatus.FAILED.value,
                last_error_kind="missing_candidate",
                last_error_message="candidate_row_missing",
                finished=True,
            )
            conn.commit()
            continue

        candidate_name = str(candidate_row["name"] or "").strip()
        existing_keys = _existing_result_keys(conn, candidate_id=int(candidate_row["id"]))
        blocker_check = ""
        resume_check = str(job_row["resume_check"] or "").strip()
        reached_resume = not bool(resume_check)
        terminal = False
        for check_name in checks:
            if not reached_resume:
                reached_resume = check_name == resume_check
                if not reached_resume:
                    continue
            if check_name in existing_keys:
                if check_name in EARLY_EXIT_CHECKS:
                    existing_rows = db.fetch_results_for_candidate(conn, candidate_id=int(candidate_row["id"]))
                    if any(
                        str(row["result_key"]) == check_name and str(row["status"]) == ResultStatus.FAIL.value
                        for row in existing_rows
                    ):
                        blocker_check = check_name
                continue
            if blocker_check:
                skipped = skipped_result(check_name=check_name, blocker_check=blocker_check)
                _upsert_probe_result(conn, candidate_id=int(candidate_row["id"]), probe=skipped)
                existing_keys.add(check_name)
                continue
            try:
                probe = probe_check(check_name=check_name, name=candidate_name, config=config)
            except Exception as exc:
                probe = _unexpected_probe_result(check_name=check_name, exc=exc)
            attempt_number = _attempt_count_for_check(conn, job_id=int(job_row["id"]), check_name=check_name) + 1
            _record_probe_attempt(conn, job_row=job_row, probe=probe, attempt_number=attempt_number)
            if probe.retryable and probe.candidate_result.status == ResultStatus.UNAVAILABLE:
                current_attempts = int(job_row["attempt_count"]) + 1
                if current_attempts <= MAX_RETRY_ATTEMPTS:
                    delay_s = _retry_delay_s(probe, attempt_count=current_attempts)
                    db.update_validation_job(
                        conn,
                        job_id=int(job_row["id"]),
                        status=JobStatus.RETRY_WAIT.value,
                        resume_check=check_name,
                        attempt_count=current_attempts,
                        next_retry_at=_iso_after(delay_s),
                        last_error_kind=probe.error_kind.value,
                        last_error_message=probe.candidate_result.reason,
                    )
                    conn.commit()
                    terminal = True
                    break
            _upsert_probe_result(conn, candidate_id=int(candidate_row["id"]), probe=probe)
            existing_keys.add(check_name)
            if probe.candidate_result.status == ResultStatus.FAIL and check_name in EARLY_EXIT_CHECKS:
                blocker_check = check_name
        if terminal:
            continue
        db.update_validation_job(
            conn,
            job_id=int(job_row["id"]),
            status=JobStatus.COMPLETED.value,
            resume_check="",
            next_retry_at=None,
            last_error_kind="",
            last_error_message="",
            finished=True,
        )
        conn.commit()

    counts = db.count_validation_jobs(conn, run_id=run_id)
    if mark_run_complete:
        db.set_run_state(conn, run_id=run_id, status=RunStatus.COMPLETED.value, current_step="complete", completed=True)
    else:
        db.set_run_state(conn, run_id=run_id, status=RunStatus.RUNNING.value, current_step="validation_complete")
    conn.commit()
    return {
        "job_counts": dict(counts),
        "retry_wait_loops": total_retry_waits,
    }
