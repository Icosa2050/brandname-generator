# ruff: noqa: E402
from __future__ import annotations

import contextlib
import csv
import io
import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe import db
from brandpipe.cli import main
from brandpipe.models import CandidateResult, ResultStatus
from brandpipe.tmview import TmviewProbeResult


class CliTests(unittest.TestCase):
    def test_init_db_command_creates_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(["init-db", "--db", str(db_path)])

            self.assertEqual(exit_code, 0)
            self.assertTrue(db_path.exists())
            self.assertIn("db=", stdout.getvalue())

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                row = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'runs'"
                ).fetchone()
                self.assertIsNotNone(row)

    def test_cli_day_in_life_run_status_and_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            export_path = root / "export.csv"
            config_path = root / "run.toml"
            fixture_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "candidates": [
                        {"name": "vantora"},
                        {"name": "baltera"},
                        {"name": "meridel"}
                      ]
                    }
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "cli-flow"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"
                    family_quotas = {{ smooth_blend = 3 }}

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                run_code = main(["run", "--config", str(config_path)])
            self.assertEqual(run_code, 0)
            output = stdout.getvalue().splitlines()
            run_id = int(next(line.split("=", 1)[1] for line in output if line.startswith("run_id=")))
            db_path = Path(next(line.split("=", 1)[1] for line in output if line.startswith("db=")))
            task_root = Path(next(line.split("=", 1)[1] for line in output if line.startswith("task_root=")))
            finalists_path = Path(next(line.split("=", 1)[1] for line in output if line.startswith("export_csv=")))
            self.assertTrue((task_root / "manifest.json").exists())
            self.assertEqual(db_path, task_root / "state" / "brandpipe.db")
            self.assertEqual(finalists_path, task_root / "exports" / f"finalists_{run_id}.csv")
            self.assertTrue(finalists_path.exists())

            status_stdout = io.StringIO()
            with contextlib.redirect_stdout(status_stdout):
                status_code = main(["status", "--db", str(db_path), "--run-id", str(run_id)])
            self.assertEqual(status_code, 0)
            self.assertIn("status=completed", status_stdout.getvalue())

            export_stdout = io.StringIO()
            with contextlib.redirect_stdout(export_stdout):
                export_code = main(
                    [
                        "export",
                        "--db",
                        str(db_path),
                        "--run-id",
                        str(run_id),
                        "--out-csv",
                        str(export_path),
                        "--top-n",
                        "5",
                    ]
                )
            self.assertEqual(export_code, 0)
            self.assertTrue(export_path.exists())
            self.assertIn("export_csv=", export_stdout.getvalue())

            with export_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]["rank"], "1")
            manifest = json.loads((task_root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["task"], "run")
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(manifest["db_path"], str(db_path))
            self.assertEqual(manifest["child_runs"], [{"run_id": run_id}])

    def test_status_command_errors_for_missing_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)

            with self.assertRaisesRegex(SystemExit, "run not found: 99"):
                main(["status", "--db", str(db_path), "--run-id", "99"])

    def test_validate_command_writes_standardized_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir) / "validate" / "reviewed-shortlist"
            fake_results = {
                1: [
                    CandidateResult("domain", ResultStatus.PASS, 0.0, "", {}),
                    CandidateResult("web", ResultStatus.PASS, 0.0, "", {}),
                ],
                2: [
                    CandidateResult("domain", ResultStatus.PASS, 0.0, "", {}),
                    CandidateResult("web", ResultStatus.WARN, -4.0, "web_near_warning", {}),
                ],
            }

            stdout = io.StringIO()
            with (
                contextlib.redirect_stdout(stdout),
                mock.patch(
                    "brandpipe.validate_cli.run_shortlist_validation",
                    return_value={
                        "run_id": 17,
                        "fingerprint": "validate-fixture",
                        "created_new": True,
                        "job_counts": {"completed": 2},
                        "validation_status_counts": {"pass": 3, "warn": 1},
                        "validation_check_counts": {"domain": 2, "web": 2},
                    },
                ),
                mock.patch(
                    "brandpipe.validate_cli._load_candidate_result_rows",
                    return_value=({"vantora": 1, "meridel": 2}, fake_results),
                ),
            ):
                exit_code = main(
                    [
                        "validate",
                        "--names",
                        "vantora,meridel",
                        "--out-dir",
                        str(out_dir),
                        "--checks",
                        "domain,web",
                    ]
                )

            self.assertEqual(exit_code, 0)
            output = stdout.getvalue()
            task_root_line = next(line for line in output.splitlines() if line.startswith("task_root="))
            task_root = Path(task_root_line.split("=", 1)[1].strip())
            self.assertEqual(task_root.parent, out_dir.resolve())
            self.assertTrue((task_root / "inputs").exists())
            self.assertTrue((task_root / "logs").exists())
            self.assertTrue((task_root / "state").exists())
            self.assertTrue((task_root / "exports").exists())
            self.assertTrue((task_root / "profiles").exists())

            manifest = json.loads((task_root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["task"], "validate")
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(manifest["db_path"], str(task_root / "state" / "brandpipe.db"))
            self.assertEqual(manifest["child_runs"], [{"validation_run_id": 17}])
            self.assertIn(str(task_root / "exports" / "validated_survivors.csv"), manifest["export_paths"])
            self.assertEqual(manifest["metrics_summary"]["input_count"], 2)
            self.assertEqual(manifest["metrics_summary"]["survivor_count"], 1)
            self.assertEqual(manifest["metrics_summary"]["review_count"], 1)

            with (task_root / "exports" / "validated_survivors.csv").open("r", encoding="utf-8", newline="") as handle:
                survivors = list(csv.DictReader(handle))
            with (task_root / "exports" / "validated_review_queue.csv").open("r", encoding="utf-8", newline="") as handle:
                review = list(csv.DictReader(handle))
            summary = json.loads((task_root / "exports" / "validated_publish_summary.json").read_text(encoding="utf-8"))

            self.assertEqual([row["name"] for row in survivors], ["vantora"])
            self.assertEqual([row["name"] for row in review], ["meridel"])
            self.assertEqual(summary["task_root"], str(task_root))
            self.assertEqual(summary["validation_run_id"], 17)
            self.assertEqual(summary["state_db"], str(task_root / "state" / "brandpipe.db"))

    def test_recheck_web_command_promotes_pending_watch_and_rewrites_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            export_path = root / "finalists_1.csv"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="pending-web",
                    brief={"product_core": "utility-cost settlement software"},
                    config={
                        "validation": {
                            "checks": ["domain", "package", "social", "web"],
                            "web_retry_attempts": 0,
                            "web_retry_backoff_s": 0.0,
                        },
                        "export": {
                            "out_csv": str(export_path),
                            "top_n": 10,
                        },
                    },
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["baltera"],
                    source_kind="fixture",
                    source_detail="{}",
                )
                candidate = db.list_candidates(conn, run_id=run_id)[0]
                candidate_id = int(candidate["id"])
                for key in ("domain", "package", "social"):
                    db.upsert_result(
                        conn,
                        candidate_id=candidate_id,
                        result_key=key,
                        status="pass",
                        score_delta=0.0,
                        reason="",
                        details={},
                    )
                db.upsert_result(
                    conn,
                    candidate_id=candidate_id,
                    result_key="web",
                    status="warn",
                    score_delta=-2.0,
                    reason="web_check_pending",
                    details={"pending_review": True},
                )
                db.upsert_ranking(
                    conn,
                    candidate_id=candidate_id,
                    total_score=98.0,
                    blocker_count=0,
                    unavailable_count=0,
                    unsupported_count=0,
                    warning_count=1,
                    decision="watch",
                )
                db.update_run_metrics(
                    conn,
                    run_id=run_id,
                    metrics={
                        "counts": {
                            "ideation_candidates": 1,
                            "validation_results": 4,
                            "ranked_candidates": 1,
                            "export_rows": 1,
                        },
                        "decision_counts": {"watch": 1},
                        "durations_ms": {},
                        "ideation": {},
                        "top_names": ["baltera"],
                        "export_path": str(export_path),
                    },
                )
                db.set_run_state(
                    conn,
                    run_id=run_id,
                    status="completed",
                    current_step="done",
                    completed=True,
                )
                conn.commit()

            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                with mock.patch(
                    "brandpipe.pipeline.validate_candidate",
                    return_value=[
                        CandidateResult(
                            check_name="web",
                            status=ResultStatus.PASS,
                            score_delta=0.0,
                            reason="",
                            details={"provider": "brave", "retried_web_attempts": 0},
                        )
                    ],
                ):
                    exit_code = main(["recheck-web", "--db", str(db_path), "--run-id", str(run_id)])

            self.assertEqual(exit_code, 0)
            output = stdout.getvalue()
            self.assertIn("retried=1 runs=1", output)
            self.assertIn("promoted_to_candidate=1", output)

            with db.open_db(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT r.status, r.reason
                    FROM candidate_results r
                    WHERE r.candidate_id = ? AND r.result_key = 'web'
                    """,
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["status"], "pass")
                self.assertEqual(row["reason"], "")

                ranking = conn.execute(
                    """
                    SELECT decision, warning_count, blocker_count
                    FROM candidate_rankings
                    WHERE candidate_id = ?
                    """,
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(ranking)
                assert ranking is not None
                self.assertEqual(ranking["decision"], "candidate")
                self.assertEqual(ranking["warning_count"], 0)
                self.assertEqual(ranking["blocker_count"], 0)

            self.assertTrue(export_path.exists())
            with export_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["name"], "baltera")
            self.assertEqual(rows[0]["decision"], "candidate")

    def test_recheck_tmview_command_blocks_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            export_path = root / "finalists_1.csv"
            profile_dir = root / "tmview-profile"
            profile_dir.mkdir()
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="tmview-cli",
                    brief={"product_core": "utility-cost settlement software"},
                    config={
                        "export": {
                            "out_csv": str(export_path),
                            "top_n": 10,
                        },
                    },
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["parclex"],
                    source_kind="fixture",
                    source_detail="{}",
                )
                candidate = db.list_candidates(conn, run_id=run_id)[0]
                candidate_id = int(candidate["id"])
                db.upsert_ranking(
                    conn,
                    candidate_id=candidate_id,
                    total_score=100.0,
                    blocker_count=0,
                    unavailable_count=0,
                    unsupported_count=0,
                    warning_count=0,
                    decision="candidate",
                )
                db.update_run_metrics(
                    conn,
                    run_id=run_id,
                    metrics={
                        "counts": {"ranked_candidates": 1, "export_rows": 1},
                        "decision_counts": {"candidate": 1},
                        "export_path": str(export_path),
                    },
                )
                db.set_run_state(conn, run_id=run_id, status="completed", current_step="done", completed=True)
                conn.commit()

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                with mock.patch(
                    "brandpipe.pipeline.probe_tmview_names",
                    return_value=[
                        TmviewProbeResult(
                            name="parclex",
                            url="https://example.test/tmview",
                            query_ok=True,
                            source="tmview_playwright",
                            exact_hits=0,
                            near_hits=3,
                            result_count=25,
                            sample_text="PARCELX",
                        )
                    ],
                ):
                    exit_code = main(
                        [
                            "recheck-tmview",
                            "--db",
                            str(db_path),
                            "--profile-dir",
                            str(profile_dir),
                            "--run-id",
                            str(run_id),
                        ]
                    )

            self.assertEqual(exit_code, 0)
            self.assertIn("retried=1 runs=1", stdout.getvalue())
            self.assertIn("blocked=1", stdout.getvalue())
            with db.open_db(db_path) as conn:
                ranking = conn.execute(
                    "SELECT decision FROM candidate_rankings WHERE candidate_id = ?",
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(ranking)
                assert ranking is not None
                self.assertEqual(ranking["decision"], "blocked")


if __name__ == "__main__":
    unittest.main()
