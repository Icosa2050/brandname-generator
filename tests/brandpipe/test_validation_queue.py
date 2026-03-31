# ruff: noqa: E402
from __future__ import annotations

import json
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


class ValidationQueueTests(unittest.TestCase):
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
