# ruff: noqa: E402
from __future__ import annotations

import csv
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe import validate_cli as MODULE
from brandpipe.models import CandidateResult, ResultStatus


class RunBrandpipeValidateTests(unittest.TestCase):
    def test_load_candidate_result_rows_warns_and_recovers_from_invalid_details_json(self) -> None:
        fake_conn = object()
        open_db = mock.MagicMock()
        open_db.__enter__.return_value = fake_conn
        open_db.__exit__.return_value = False
        result_rows = [
            {
                "result_key": "web",
                "status": ResultStatus.WARN.value,
                "score_delta": -4.0,
                "reason": "web_near_warning",
                "details_json": "{broken",
            }
        ]

        with (
            mock.patch.object(MODULE.db, "open_db", return_value=open_db),
            mock.patch.object(MODULE.db, "ensure_schema"),
            mock.patch.object(MODULE.db, "list_candidates", return_value=[{"id": 9, "name": "vantora"}]),
            mock.patch.object(MODULE.db, "fetch_results_for_candidate", return_value=result_rows),
        ):
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                candidate_lookup, result_map = MODULE._load_candidate_result_rows(Path("/tmp/brandpipe.db"), run_id=21)

        self.assertEqual(candidate_lookup, {"vantora": 9})
        self.assertEqual(len(result_map[9]), 1)
        self.assertEqual(result_map[9][0].details, {})
        self.assertIn("validation_details_json_invalid", stderr.getvalue())

    def test_run_validate_command_preserves_digits_from_names_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir) / "validated"
            captured_names: list[str] = []

            def fake_run_shortlist_validation(*, db_path: Path, candidate_names: list[str], config: object):
                del db_path, config
                captured_names.extend(candidate_names)
                return {
                    "run_id": 11,
                    "fingerprint": "abc123",
                    "created_new": True,
                    "job_counts": {"completed": 1},
                    "validation_status_counts": {"pass": 2},
                    "validation_check_counts": {"domain": 1, "company": 1},
                }

            fake_results = {
                1: [
                    CandidateResult("domain", ResultStatus.PASS, 0.0, "", {}),
                    CandidateResult("company", ResultStatus.PASS, 0.0, "", {}),
                ]
            }

            args = MODULE.argparse.Namespace(
                input_csv="",
                names_file="",
                names="set4you",
                mode="keep_maybe",
                out_dir=str(out_dir),
                checks="domain,company",
                concurrency=1,
                timeout_s=5.0,
                required_domain_tlds="",
                store_countries="de,ch,us",
                company_top=8,
                social_unavailable_fail_threshold=3,
                web_search_order="serper,brave",
                web_browser_profile_dir="",
                web_browser_chrome_executable="",
                tmview_profile_dir="",
                tmview_chrome_executable="",
                reset_state=False,
            )

            with (
                mock.patch.object(MODULE, "run_shortlist_validation", side_effect=fake_run_shortlist_validation),
                mock.patch.object(MODULE, "_load_candidate_result_rows", return_value=({"set4you": 1}, fake_results)),
            ):
                exit_code = MODULE.run_validate_command(args)

            self.assertEqual(exit_code, 0)
            self.assertEqual(captured_names, ["set4you"])
            task_root = next(out_dir.iterdir())
            with (task_root / "exports" / "validated_survivors.csv").open("r", encoding="utf-8", newline="") as handle:
                survivors = list(csv.DictReader(handle))
            self.assertEqual([row["name"] for row in survivors], ["set4you"])

    def test_run_validate_command_buckets_survivor_review_and_rejected_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            input_csv = root / "review.csv"
            out_dir = root / "validated"
            with input_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "keep",
                        "maybe",
                        "drop",
                        "name_display",
                        "name_normalized",
                        "decision_notes",
                        "current_recommendation",
                        "score",
                        "risk",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "keep": "x",
                        "maybe": "",
                        "drop": "",
                        "name_display": "Vantora",
                        "name_normalized": "vantora",
                        "decision_notes": "top pick",
                        "current_recommendation": "strong",
                        "score": "120",
                        "risk": "0",
                    }
                )
                writer.writerow(
                    {
                        "keep": "",
                        "maybe": "x",
                        "drop": "",
                        "name_display": "Meridel",
                        "name_normalized": "meridel",
                        "decision_notes": "needs review",
                        "current_recommendation": "consider",
                        "score": "110",
                        "risk": "5",
                    }
                )
                writer.writerow(
                    {
                        "keep": "x",
                        "maybe": "",
                        "drop": "",
                        "name_display": "Certivo",
                        "name_normalized": "certivo",
                        "decision_notes": "company conflict",
                        "current_recommendation": "strong",
                        "score": "118",
                        "risk": "2",
                    }
                )

            fake_results = {
                1: [
                    CandidateResult("domain", ResultStatus.PASS, 0.0, "", {}),
                    CandidateResult("company", ResultStatus.PASS, 0.0, "", {}),
                ],
                2: [
                    CandidateResult("domain", ResultStatus.PASS, 0.0, "", {}),
                    CandidateResult("web", ResultStatus.WARN, -4.0, "web_near_warning", {}),
                ],
                3: [
                    CandidateResult("domain", ResultStatus.PASS, 0.0, "", {}),
                    CandidateResult("company", ResultStatus.FAIL, -16.0, "company_exact_active", {}),
                ],
            }

            args = MODULE.argparse.Namespace(
                input_csv=str(input_csv),
                names_file="",
                names="",
                mode="keep_maybe",
                out_dir=str(out_dir),
                checks="domain,company,web",
                concurrency=2,
                timeout_s=5.0,
                required_domain_tlds="",
                store_countries="de,ch,us",
                company_top=8,
                social_unavailable_fail_threshold=3,
                web_search_order="serper,brave",
                web_browser_profile_dir="",
                web_browser_chrome_executable="",
                tmview_profile_dir="",
                tmview_chrome_executable="",
                reset_state=False,
            )

            with (
                mock.patch.object(
                    MODULE,
                    "run_shortlist_validation",
                    return_value={
                        "run_id": 12,
                        "fingerprint": "def456",
                        "created_new": True,
                        "job_counts": {"completed": 3},
                        "validation_status_counts": {"pass": 4, "warn": 1, "fail": 1},
                        "validation_check_counts": {"domain": 3, "company": 2, "web": 1},
                    },
                ),
                mock.patch.object(
                    MODULE,
                    "_load_candidate_result_rows",
                    return_value=({"vantora": 1, "meridel": 2, "certivo": 3}, fake_results),
                ),
            ):
                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    exit_code = MODULE.run_validate_command(args)

            self.assertEqual(exit_code, 0)
            self.assertIn("validator_concurrency_deprecated requested=2 effective=1", stderr.getvalue())
            task_root = next(out_dir.iterdir())
            with (task_root / "exports" / "validated_survivors.csv").open("r", encoding="utf-8", newline="") as handle:
                survivors = list(csv.DictReader(handle))
            with (task_root / "exports" / "validated_review_queue.csv").open("r", encoding="utf-8", newline="") as handle:
                review = list(csv.DictReader(handle))
            with (task_root / "exports" / "validated_rejected.csv").open("r", encoding="utf-8", newline="") as handle:
                rejected = list(csv.DictReader(handle))
            summary = json.loads((task_root / "exports" / "validated_publish_summary.json").read_text(encoding="utf-8"))

            self.assertEqual([row["name"] for row in survivors], ["Vantora"])
            self.assertEqual([row["name"] for row in review], ["Meridel"])
            self.assertEqual([row["name"] for row in rejected], ["Certivo"])
            self.assertEqual(summary["survivor_count"], 1)
            self.assertEqual(summary["review_count"], 1)
            self.assertEqual(summary["rejected_count"], 1)


if __name__ == "__main__":
    unittest.main()
