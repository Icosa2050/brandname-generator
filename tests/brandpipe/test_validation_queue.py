# ruff: noqa: E402
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe import db
from brandpipe.models import CandidateResult, ErrorKind, ResultStatus, ValidationConfig
from brandpipe.pipeline import run_shortlist_validation
from brandpipe.validation_runtime import ProbeResult
from brandpipe.validation_queue import (
    _attempt_count_for_check,
    _existing_result_keys,
    _iso_after,
    _next_due_retry_s,
    _parse_iso,
    _retry_delay_s,
    _unexpected_probe_result,
    MAX_RETRY_ATTEMPTS,
    detect_state_mismatch,
    find_resume_run,
    prepare_shortlist_run,
    run_validation_jobs,
    shortlist_fingerprint,
    validation_state_title,
)


class ValidationQueueTests(unittest.TestCase):
    def test_shortlist_fingerprint_and_state_title_are_stable(self) -> None:
        config = ValidationConfig(
            checks=["domain", "web"],
            required_domain_tlds="com,de",
            store_countries="de,ch",
            web_search_order="serper,brave",
        )

        first = shortlist_fingerprint(names=["vantora", " meridel "], config=config)
        second = shortlist_fingerprint(names=["vantora", "meridel"], config=config)
        changed = shortlist_fingerprint(names=["vantora"], config=config)

        self.assertEqual(first, second)
        self.assertNotEqual(first, changed)
        self.assertTrue(validation_state_title(fingerprint=first).startswith("shortlist_validation:"))

    def test_time_and_retry_helpers_cover_delay_rules(self) -> None:
        parsed = _parse_iso("2026-04-07T12:30:00Z")
        self.assertEqual(parsed.tzinfo, timezone.utc)
        self.assertEqual(parsed.year, 2026)

        future = _parse_iso(_iso_after(0.1))
        self.assertGreaterEqual(future, datetime.now(timezone.utc) - timedelta(seconds=1))

        self.assertEqual(
            _retry_delay_s(
                ProbeResult.unavailable(
                    check_name="web",
                    score_delta=-2.0,
                    reason="retry-later",
                    error_kind=ErrorKind.RATE_LIMITED,
                    retryable=True,
                    retry_after_s=7.0,
                ),
                attempt_count=1,
            ),
            7.0,
        )
        self.assertEqual(
            _retry_delay_s(
                ProbeResult.unavailable(
                    check_name="web",
                    score_delta=-2.0,
                    reason="rate-limited",
                    error_kind=ErrorKind.RATE_LIMITED,
                    retryable=True,
                ),
                attempt_count=2,
            ),
            20.0,
        )
        self.assertEqual(
            _retry_delay_s(
                ProbeResult.unavailable(
                    check_name="web",
                    score_delta=-2.0,
                    reason="timed-out",
                    error_kind=ErrorKind.TIMEOUT,
                    retryable=True,
                ),
                attempt_count=2,
            ),
            8.0,
        )
        self.assertEqual(
            _retry_delay_s(
                ProbeResult.unavailable(
                    check_name="web",
                    score_delta=-2.0,
                    reason="browser",
                    error_kind=ErrorKind.BROWSER,
                    retryable=True,
                ),
                attempt_count=2,
            ),
            10.0,
        )
        self.assertEqual(
            _retry_delay_s(
                ProbeResult.unavailable(
                    check_name="web",
                    score_delta=-2.0,
                    reason="transport",
                    error_kind=ErrorKind.TRANSPORT,
                    retryable=True,
                ),
                attempt_count=3,
            ),
            6.0,
        )

        unexpected = _unexpected_probe_result(check_name="company", exc=RuntimeError("boom"))
        self.assertEqual(unexpected.candidate_result.reason, "validation_probe_exception")
        self.assertEqual(unexpected.error_kind, ErrorKind.UNEXPECTED)
        self.assertEqual(unexpected.transport, "validation_queue")
        self.assertEqual(unexpected.evidence["error_class"], "RuntimeError")

    def test_db_backed_queue_helpers_report_existing_results_and_due_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="queue-helper-test",
                    brief={"source": "test"},
                    config={"validation": {}},
                )
                db.add_candidates(conn, run_id=run_id, names=["vantora"], source_kind="shortlist", source_detail="test")
                candidate_row = db.list_candidates(conn, run_id=run_id)[0]
                db.ensure_validation_jobs(
                    conn,
                    run_id=run_id,
                    ordered_candidate_ids=[int(candidate_row["id"])],
                    shortlist_fingerprint="fingerprint",
                )
                job_row = db.list_validation_jobs(conn, run_id=run_id)[0]
                db.upsert_result(
                    conn,
                    candidate_id=int(candidate_row["id"]),
                    result_key="domain",
                    status=ResultStatus.PASS.value,
                    score_delta=0.0,
                    reason="",
                    details={"availability": {"com": "yes"}},
                )
                db.record_validation_attempt(
                    conn,
                    job_id=int(job_row["id"]),
                    run_id=run_id,
                    candidate_id=int(candidate_row["id"]),
                    check_name="domain",
                    attempt_number=1,
                    status=ResultStatus.PASS.value,
                    reason="",
                    error_kind=ErrorKind.NONE.value,
                    retryable=False,
                    http_status=200,
                    retry_after_s=None,
                    headers={},
                    evidence={},
                    details={},
                )
                db.update_validation_job(
                    conn,
                    job_id=int(job_row["id"]),
                    status="retry_wait",
                    resume_check="domain",
                    next_retry_at=_iso_after(30.0),
                )
                conn.commit()

                self.assertEqual(_existing_result_keys(conn, candidate_id=int(candidate_row["id"])), {"domain"})
                self.assertEqual(_attempt_count_for_check(conn, job_id=int(job_row["id"]), check_name="domain"), 1)
                next_due = _next_due_retry_s(conn, run_id=run_id)
                self.assertIsNotNone(next_due)
                assert next_due is not None
                self.assertGreaterEqual(next_due, 0.0)
                self.assertLessEqual(next_due, 30.0)

    def test_state_helpers_cover_resume_lookup_mismatch_and_empty_retry_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain"])
            fingerprint = shortlist_fingerprint(names=["vantora"], config=config)

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)

                self.assertIsNone(find_resume_run(conn, fingerprint=fingerprint))
                self.assertFalse(detect_state_mismatch(conn, fingerprint=fingerprint))

                db.create_run(conn, title="manual-other", brief={"source": "test"}, config={})
                conn.commit()

                self.assertTrue(detect_state_mismatch(conn, fingerprint=fingerprint))

                run_id, created_new = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )

                self.assertTrue(created_new)
                self.assertEqual(find_resume_run(conn, fingerprint=fingerprint), run_id)
                self.assertIsNone(_next_due_retry_s(conn, run_id=run_id))

    def test_run_validation_jobs_waits_for_retry_then_resumes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain"])

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                fingerprint = shortlist_fingerprint(names=["vantora"], config=config)
                run_id, _ = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )
                job_row = db.list_validation_jobs(conn, run_id=run_id)[0]
                db.update_validation_job(
                    conn,
                    job_id=int(job_row["id"]),
                    status="retry_wait",
                    resume_check="domain",
                    attempt_count=1,
                    next_retry_at=_iso_after(30.0),
                    last_error_kind=ErrorKind.RATE_LIMITED.value,
                    last_error_message="retrying",
                )
                conn.commit()

                sleep_calls: list[float] = []

                def fake_sleep(delay_s: float) -> None:
                    sleep_calls.append(delay_s)
                    db.update_validation_job(
                        conn,
                        job_id=int(job_row["id"]),
                        status="retry_wait",
                        resume_check="domain",
                        attempt_count=1,
                        next_retry_at=None,
                        last_error_kind=ErrorKind.RATE_LIMITED.value,
                        last_error_message="retrying",
                    )
                    conn.commit()

                pass_probe = ProbeResult(
                    candidate_result=CandidateResult(
                        check_name="domain",
                        status=ResultStatus.PASS,
                        score_delta=0.0,
                        reason="",
                        details={"availability": {"com": "yes"}},
                    )
                )

                with mock.patch("brandpipe.validation_queue.probe_check", return_value=pass_probe):
                    summary = run_validation_jobs(
                        conn,
                        run_id=run_id,
                        config=config,
                        sleep_fn=fake_sleep,
                        mark_run_complete=True,
                    )

                self.assertEqual(summary["retry_wait_loops"], 1)
                self.assertEqual(sleep_calls, [1.0])
                self.assertEqual(summary["job_counts"]["completed"], 1)
                run_row = db.get_run(conn, run_id=run_id)
                assert run_row is not None
                self.assertEqual(str(run_row["status"]), "completed")

    def test_run_validation_jobs_marks_missing_candidate_rows_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain"])

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                fingerprint = shortlist_fingerprint(names=["vantora"], config=config)
                run_id, _ = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )

                with mock.patch("brandpipe.validation_queue.db.get_candidate", return_value=None):
                    summary = run_validation_jobs(conn, run_id=run_id, config=config)

                self.assertEqual(summary["job_counts"]["failed"], 1)
                job_row = db.list_validation_jobs(conn, run_id=run_id)[0]
                self.assertEqual(str(job_row["status"]), "failed")
                self.assertEqual(str(job_row["last_error_kind"]), "missing_candidate")
                self.assertEqual(str(job_row["last_error_message"]), "candidate_row_missing")

    def test_run_validation_jobs_skips_checks_until_resume_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain", "company", "web"])

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                fingerprint = shortlist_fingerprint(names=["vantora"], config=config)
                run_id, _ = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )
                job_row = db.list_validation_jobs(conn, run_id=run_id)[0]
                db.update_validation_job(conn, job_id=int(job_row["id"]), status="pending", resume_check="web")
                conn.commit()

                probed_checks: list[str] = []

                def fake_probe_check(*, check_name: str, name: str, config: ValidationConfig) -> ProbeResult:
                    del name, config
                    probed_checks.append(check_name)
                    return ProbeResult(
                        candidate_result=CandidateResult(
                            check_name=check_name,
                            status=ResultStatus.PASS,
                            score_delta=0.0,
                            reason="",
                            details={},
                        )
                    )

                with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
                    summary = run_validation_jobs(conn, run_id=run_id, config=config)

                self.assertEqual(probed_checks, ["web"])
                self.assertEqual(summary["job_counts"]["completed"], 1)
                candidate_row = db.list_candidates(conn, run_id=run_id)[0]
                result_rows = db.fetch_results_for_candidate(conn, candidate_id=int(candidate_row["id"]))
                self.assertEqual([str(row["result_key"]) for row in result_rows], ["web"])

    def test_run_validation_jobs_uses_existing_blocker_to_skip_followups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain", "company", "web"])

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                fingerprint = shortlist_fingerprint(names=["vantora"], config=config)
                run_id, _ = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )
                job_row = db.list_validation_jobs(conn, run_id=run_id)[0]
                candidate_row = db.list_candidates(conn, run_id=run_id)[0]
                db.upsert_result(
                    conn,
                    candidate_id=int(candidate_row["id"]),
                    result_key="domain",
                    status=ResultStatus.FAIL.value,
                    score_delta=-18.0,
                    reason="domain_unavailable_com",
                    details={"availability": {"com": "no"}},
                )
                db.update_validation_job(
                    conn,
                    job_id=int(job_row["id"]),
                    status="pending",
                    resume_check="domain",
                )
                conn.commit()

                with mock.patch("brandpipe.validation_queue.probe_check") as probe_check:
                    summary = run_validation_jobs(conn, run_id=run_id, config=config)

                self.assertFalse(probe_check.called)
                self.assertEqual(summary["job_counts"]["completed"], 1)
                result_rows = db.fetch_results_for_candidate(conn, candidate_id=int(candidate_row["id"]))
                statuses = {str(row["result_key"]): str(row["status"]) for row in result_rows}
                self.assertEqual(statuses["domain"], ResultStatus.FAIL.value)
                self.assertEqual(statuses["company"], ResultStatus.SKIPPED.value)
                self.assertEqual(statuses["web"], ResultStatus.SKIPPED.value)

    def test_run_validation_jobs_keeps_going_when_existing_early_exit_result_is_not_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain", "company"])

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                fingerprint = shortlist_fingerprint(names=["vantora"], config=config)
                run_id, _ = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )
                job_row = db.list_validation_jobs(conn, run_id=run_id)[0]
                candidate_row = db.list_candidates(conn, run_id=run_id)[0]
                db.upsert_result(
                    conn,
                    candidate_id=int(candidate_row["id"]),
                    result_key="domain",
                    status=ResultStatus.PASS.value,
                    score_delta=0.0,
                    reason="",
                    details={"availability": {"com": "yes"}},
                )
                db.update_validation_job(
                    conn,
                    job_id=int(job_row["id"]),
                    status="pending",
                    resume_check="domain",
                )
                conn.commit()

                probed_checks: list[str] = []

                def fake_probe_check(*, check_name: str, name: str, config: ValidationConfig) -> ProbeResult:
                    del name, config
                    probed_checks.append(check_name)
                    return ProbeResult(
                        candidate_result=CandidateResult(
                            check_name=check_name,
                            status=ResultStatus.PASS,
                            score_delta=0.0,
                            reason="",
                            details={},
                        )
                    )

                with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
                    summary = run_validation_jobs(conn, run_id=run_id, config=config)

                self.assertEqual(summary["job_counts"]["completed"], 1)
                self.assertEqual(probed_checks, ["company"])

    def test_run_validation_jobs_breaks_when_only_running_jobs_remain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain"])

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                fingerprint = shortlist_fingerprint(names=["vantora"], config=config)
                run_id, _ = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )
                job_row = db.list_validation_jobs(conn, run_id=run_id)[0]
                db.update_validation_job(conn, job_id=int(job_row["id"]), status="running")
                conn.commit()

                summary = run_validation_jobs(conn, run_id=run_id, config=config)

                self.assertEqual(summary["job_counts"]["running"], 1)
                self.assertEqual(summary["retry_wait_loops"], 0)

    def test_run_validation_jobs_wraps_probe_exceptions_as_unavailable_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["web"])

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                fingerprint = shortlist_fingerprint(names=["vantora"], config=config)
                run_id, _ = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )

                with mock.patch("brandpipe.validation_queue.probe_check", side_effect=RuntimeError("boom")):
                    summary = run_validation_jobs(conn, run_id=run_id, config=config)

                self.assertEqual(summary["job_counts"]["completed"], 1)
                candidate_row = db.list_candidates(conn, run_id=run_id)[0]
                result_rows = db.fetch_results_for_candidate(conn, candidate_id=int(candidate_row["id"]))
                self.assertEqual(len(result_rows), 1)
                self.assertEqual(str(result_rows[0]["status"]), ResultStatus.UNAVAILABLE.value)
                self.assertEqual(str(result_rows[0]["reason"]), "validation_probe_exception")

    def test_run_validation_jobs_skips_existing_non_blocking_non_early_exit_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["web"])

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                fingerprint = shortlist_fingerprint(names=["vantora"], config=config)
                run_id, _ = prepare_shortlist_run(
                    conn,
                    candidate_names=["vantora"],
                    config=config,
                    fingerprint=fingerprint,
                )
                job_row = db.list_validation_jobs(conn, run_id=run_id)[0]
                candidate_row = db.list_candidates(conn, run_id=run_id)[0]
                db.upsert_result(
                    conn,
                    candidate_id=int(candidate_row["id"]),
                    result_key="web",
                    status=ResultStatus.PASS.value,
                    score_delta=0.0,
                    reason="",
                    details={},
                )
                db.update_validation_job(
                    conn,
                    job_id=int(job_row["id"]),
                    status="pending",
                    resume_check="web",
                )
                conn.commit()

                with mock.patch("brandpipe.validation_queue.probe_check") as probe_check:
                    summary = run_validation_jobs(conn, run_id=run_id, config=config)

                self.assertFalse(probe_check.called)
                self.assertEqual(summary["job_counts"]["completed"], 1)

    def test_shortlist_validation_respects_explicit_empty_checks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=[])

            with mock.patch("brandpipe.validation_queue.probe_check") as probe_check:
                summary = run_shortlist_validation(db_path=db_path, candidate_names=["vantora"], config=config)

            self.assertFalse(probe_check.called)
            self.assertEqual(summary["validation_check_counts"], {})
            self.assertEqual(summary["job_counts"]["completed"], 1)
            with db.open_db(db_path) as conn:
                jobs = db.list_validation_jobs(conn, run_id=int(summary["run_id"]))
                self.assertEqual(len(jobs), 1)
                self.assertEqual(str(jobs[0]["status"]), "completed")
                candidate_row = db.list_candidates(conn, run_id=int(summary["run_id"]))[0]
                result_rows = db.fetch_results_for_candidate(conn, candidate_id=int(candidate_row["id"]))
                self.assertEqual(result_rows, [])

    def test_shortlist_validation_retries_rate_limited_probe_and_records_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain"])
            attempts = {"count": 0}

            def fake_probe_check(*, check_name: str, name: str, config: ValidationConfig) -> ProbeResult:
                del config
                self.assertEqual(check_name, "domain")
                self.assertEqual(name, "vantora")
                attempts["count"] += 1
                if attempts["count"] == 1:
                    return ProbeResult.unavailable(
                        check_name="domain",
                        score_delta=-2.0,
                        reason="domain_unknown_com",
                        error_kind=ErrorKind.RATE_LIMITED,
                        retryable=True,
                        transport="rdap",
                        http_status=429,
                        retry_after_s=0.0,
                        headers={"Retry-After": "0"},
                        details={"availability": {"com": "unknown"}},
                    )
                return ProbeResult(
                    candidate_result=CandidateResult(
                        check_name="domain",
                        status=ResultStatus.PASS,
                        score_delta=0.0,
                        reason="",
                        details={"availability": {"com": "yes"}},
                    )
                )

            with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
                summary = run_shortlist_validation(db_path=db_path, candidate_names=["vantora"], config=config)

            self.assertEqual(summary["job_counts"]["completed"], 1)
            self.assertEqual(attempts["count"], 2)
            with db.open_db(db_path) as conn:
                candidate_row = db.list_candidates(conn, run_id=int(summary["run_id"]))[0]
                result_rows = db.fetch_results_for_candidate(conn, candidate_id=int(candidate_row["id"]))
                self.assertEqual(len(result_rows), 1)
                self.assertEqual(str(result_rows[0]["status"]), ResultStatus.PASS.value)
                attempt_rows = db.fetch_validation_attempts(
                    conn,
                    job_id=int(db.list_validation_jobs(conn, run_id=int(summary["run_id"]))[0]["id"]),
                )
                self.assertEqual(len(attempt_rows), 2)
                self.assertEqual(str(attempt_rows[0]["status"]), ResultStatus.UNAVAILABLE.value)
                self.assertEqual(str(attempt_rows[1]["status"]), ResultStatus.PASS.value)

    def test_shortlist_validation_reuses_completed_state_for_same_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain", "company"])

            def fake_probe_check(*, check_name: str, name: str, config: ValidationConfig) -> ProbeResult:
                del name, config
                return ProbeResult(
                    candidate_result=CandidateResult(
                        check_name=check_name,
                        status=ResultStatus.PASS,
                        score_delta=0.0,
                        reason="",
                        details={},
                    )
                )

            with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
                first = run_shortlist_validation(db_path=db_path, candidate_names=["vantora"], config=config)
                second = run_shortlist_validation(db_path=db_path, candidate_names=["vantora"], config=config)

            self.assertTrue(first["created_new"])
            self.assertFalse(second["created_new"])
            self.assertEqual(first["run_id"], second["run_id"])
            with db.open_db(db_path) as conn:
                jobs = db.list_validation_jobs(conn, run_id=int(first["run_id"]))
                self.assertEqual(len(jobs), 1)
                self.assertEqual(str(jobs[0]["status"]), "completed")
                run_row = db.get_run(conn, run_id=int(first["run_id"]))
                self.assertIsNotNone(run_row)
                assert run_row is not None
                self.assertEqual(str(run_row["status"]), "completed")

    def test_shortlist_validation_records_skipped_followups_after_early_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain", "company", "web"])

            def fake_probe_check(*, check_name: str, name: str, config: ValidationConfig) -> ProbeResult:
                del name, config
                if check_name == "domain":
                    return ProbeResult(
                        candidate_result=CandidateResult(
                            check_name="domain",
                            status=ResultStatus.FAIL,
                            score_delta=-18.0,
                            reason="domain_unavailable_com",
                            details={"availability": {"com": "no"}},
                        )
                    )
                raise AssertionError(f"unexpected check execution: {check_name}")

            with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
                summary = run_shortlist_validation(db_path=db_path, candidate_names=["vantora"], config=config)

            self.assertEqual(summary["job_counts"]["completed"], 1)
            with db.open_db(db_path) as conn:
                candidate_row = db.list_candidates(conn, run_id=int(summary["run_id"]))[0]
                result_rows = db.fetch_results_for_candidate(conn, candidate_id=int(candidate_row["id"]))
                statuses = {str(row["result_key"]): str(row["status"]) for row in result_rows}
                reasons = {str(row["result_key"]): str(row["reason"]) for row in result_rows}
                self.assertEqual(statuses["domain"], ResultStatus.FAIL.value)
                self.assertEqual(statuses["company"], ResultStatus.SKIPPED.value)
                self.assertEqual(statuses["web"], ResultStatus.SKIPPED.value)
                self.assertEqual(reasons["company"], "skipped_due_to_blocker")

    def test_shortlist_validation_stops_retrying_after_max_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain"])
            attempts = {"count": 0}

            def fake_probe_check(*, check_name: str, name: str, config: ValidationConfig) -> ProbeResult:
                del check_name, name, config
                attempts["count"] += 1
                return ProbeResult.unavailable(
                    check_name="domain",
                    score_delta=-2.0,
                    reason="domain_unknown_com",
                    error_kind=ErrorKind.RATE_LIMITED,
                    retryable=True,
                    transport="rdap",
                    http_status=429,
                    retry_after_s=0.0,
                    headers={"Retry-After": "0"},
                    details={"availability": {"com": "unknown"}},
                )

            with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
                summary = run_shortlist_validation(db_path=db_path, candidate_names=["vantora"], config=config)

            self.assertEqual(attempts["count"], MAX_RETRY_ATTEMPTS + 1)
            self.assertEqual(summary["job_counts"]["completed"], 1)
            with db.open_db(db_path) as conn:
                jobs = db.list_validation_jobs(conn, run_id=int(summary["run_id"]))
                self.assertEqual(str(jobs[0]["status"]), "completed")
                candidate_row = db.list_candidates(conn, run_id=int(summary["run_id"]))[0]
                result_rows = db.fetch_results_for_candidate(conn, candidate_id=int(candidate_row["id"]))
                self.assertEqual(len(result_rows), 1)
                self.assertEqual(str(result_rows[0]["status"]), ResultStatus.UNAVAILABLE.value)
                attempt_rows = db.fetch_validation_attempts(conn, job_id=int(jobs[0]["id"]))
                self.assertEqual(len(attempt_rows), MAX_RETRY_ATTEMPTS + 1)

    def test_shortlist_validation_rejects_fingerprint_mismatch_in_existing_state_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            config = ValidationConfig(checks=["domain"])

            def fake_probe_check(*, check_name: str, name: str, config: ValidationConfig) -> ProbeResult:
                del name, config
                return ProbeResult(
                    candidate_result=CandidateResult(
                        check_name=check_name,
                        status=ResultStatus.PASS,
                        score_delta=0.0,
                        reason="",
                        details={},
                    )
                )

            with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
                run_shortlist_validation(db_path=db_path, candidate_names=["vantora"], config=config)

            with self.assertRaisesRegex(RuntimeError, "validation_state_mismatch"):
                run_shortlist_validation(db_path=db_path, candidate_names=["meridel"], config=config)


if __name__ == "__main__":
    unittest.main()
