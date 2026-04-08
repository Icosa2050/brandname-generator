# ruff: noqa: E402
from __future__ import annotations

import contextlib
import csv
import io
import json
import runpy
import sys
import tempfile
import textwrap
import unittest
import warnings
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

    def test_status_command_lists_runs_and_prints_metrics_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_one = db.create_run(
                    conn,
                    title="first-run",
                    brief={"product_core": "utility-cost settlement software"},
                    config={},
                    batch_id="batch-42",
                )
                run_two = db.create_run(
                    conn,
                    title="second-run",
                    brief={"product_core": "utility-cost settlement software"},
                    config={},
                    batch_id="batch-42",
                )
                db.update_run_metrics(conn, run_id=run_one, metrics={"counts": {"ranked_candidates": 2}})
                db.update_run_metrics(conn, run_id=run_two, metrics={"counts": {"ranked_candidates": 1}})
                conn.commit()

            single_stdout = io.StringIO()
            with contextlib.redirect_stdout(single_stdout):
                exit_code = main(
                    ["status", "--db", str(db_path), "--run-id", str(run_one), "--show-metrics"]
                )
            self.assertEqual(exit_code, 0)
            self.assertIn("metrics=", single_stdout.getvalue())

            list_stdout = io.StringIO()
            with contextlib.redirect_stdout(list_stdout):
                exit_code = main(
                    [
                        "status",
                        "--db",
                        str(db_path),
                        "--batch-id",
                        "batch-42",
                        "--limit",
                        "5",
                        "--show-metrics",
                    ]
                )
            self.assertEqual(exit_code, 0)
            output = list_stdout.getvalue()
            self.assertIn(f"id={run_one}", output)
            self.assertIn(f"id={run_two}", output)
            self.assertIn("metrics=", output)
            self.assertIn("batch_id=batch-42 runs=2", output)

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

    def test_recheck_tmview_command_prints_nice_class_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            profile_dir = Path(tmp_dir) / "tmview-profile"
            profile_dir.mkdir()
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                with mock.patch(
                    "brandpipe.cli.recheck_tmview",
                    return_value={
                        "retried": 0,
                        "run_count": 1,
                        "runs": [
                            {
                                "run_id": 7,
                                "retried": 0,
                                "promoted_to_candidate": 0,
                                "promoted_to_watch": 0,
                                "blocked": 0,
                                "unchanged": 1,
                            }
                        ],
                    },
                ) as recheck_mock:
                    exit_code = main(
                        [
                            "recheck-tmview",
                            "--db",
                            str(db_path),
                            "--profile-dir",
                            str(profile_dir),
                            "--nice-class",
                            "9,OR,42",
                        ]
                    )

        self.assertEqual(exit_code, 0)
        self.assertIn("nice_class=9,OR,42", stdout.getvalue())
        self.assertEqual(recheck_mock.call_args.kwargs["nice_class"], "9,OR,42")

    def test_browser_profile_commands_print_expected_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            profile_dir = Path(tmp_dir) / "browser-profile"
            chrome_executable = Path(tmp_dir) / "chrome"
            smoke_stdout = io.StringIO()
            with contextlib.redirect_stdout(smoke_stdout):
                with mock.patch(
                    "brandpipe.cli.run_browser_profile_smoke",
                    return_value={
                        "profile_dir": str(profile_dir),
                        "title": "Search Results",
                        "final_url": "https://search.brave.com/search?q=vantora",
                        "cookies_count": 5,
                        "screenshot_path": str(profile_dir / "smoke.png"),
                        "storage_state_path": str(profile_dir / "state.json"),
                        "report_path": str(profile_dir / "report.json"),
                    },
                ) as smoke_mock:
                    exit_code = main(
                        [
                            "browser-profile-smoke",
                            "--profile-dir",
                            str(profile_dir),
                            "--chrome-executable",
                            str(chrome_executable),
                            "--query",
                            "vantora",
                            "--headed",
                            "--timeout-ms",
                            "4500",
                            "--settle-ms",
                            "750",
                        ]
                    )

            self.assertEqual(exit_code, 0)
            smoke_output = smoke_stdout.getvalue()
            self.assertIn("screenshot=", smoke_output)
            self.assertIn("storage_state=", smoke_output)
            self.assertIn("report=", smoke_output)
            self.assertEqual(smoke_mock.call_args.kwargs["profile_dir"], profile_dir.resolve())
            self.assertEqual(smoke_mock.call_args.kwargs["chrome_executable"], chrome_executable.resolve())
            self.assertTrue(smoke_mock.call_args.kwargs["headed"])

            warm_stdout = io.StringIO()
            with contextlib.redirect_stdout(warm_stdout):
                with mock.patch(
                    "brandpipe.cli.warm_browser_profile",
                    return_value={
                        "profile_dir": str(profile_dir),
                        "title": "Warm Browser",
                        "final_url": "https://search.brave.com/search?q=vantora",
                        "cookies_count": 2,
                        "storage_state_path": str(profile_dir / "state.json"),
                        "report_path": str(profile_dir / "warm-report.json"),
                    },
                ) as warm_mock:
                    exit_code = main(
                        [
                            "browser-profile-warmup",
                            "--profile-dir",
                            str(profile_dir),
                            "--chrome-executable",
                            str(chrome_executable),
                            "--query",
                            "vantora",
                            "--timeout-ms",
                            "5000",
                            "--settle-ms",
                            "900",
                        ]
                    )

            self.assertEqual(exit_code, 0)
            warm_output = warm_stdout.getvalue()
            self.assertIn("storage_state=", warm_output)
            self.assertIn("report=", warm_output)
            self.assertEqual(warm_mock.call_args.kwargs["profile_dir"], profile_dir.resolve())
            self.assertEqual(warm_mock.call_args.kwargs["chrome_executable"], chrome_executable.resolve())

    def test_tmview_probe_command_prints_results_and_optional_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            profile_dir = Path(tmp_dir) / "tmview-profile"
            profile_dir.mkdir()
            chrome_executable = Path(tmp_dir) / "chrome"
            output_json = Path(tmp_dir) / "tmview.json"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                with (
                    mock.patch(
                        "brandpipe.cli.probe_tmview_names",
                        return_value=[
                            TmviewProbeResult(
                                name="vantora",
                                url="https://example.test/tmview",
                                query_ok=True,
                                source="tmview_playwright",
                                exact_hits=1,
                                near_hits=0,
                                result_count=3,
                                sample_text="VANTORA",
                                error="",
                            )
                        ],
                    ) as probe_mock,
                    mock.patch(
                        "brandpipe.cli.write_tmview_results_json",
                        return_value=output_json,
                    ) as write_mock,
                ):
                    exit_code = main(
                        [
                            "tmview-probe",
                            "--names",
                            "vantora, meridel ,",
                            "--profile-dir",
                            str(profile_dir),
                            "--chrome-executable",
                            str(chrome_executable),
                            "--nice-class",
                            "9,OR,42",
                            "--output-json",
                            str(output_json),
                            "--headful",
                        ]
                    )

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue()
        self.assertIn("nice_class=9,OR,42", output)
        self.assertIn("tmview_probe name=vantora", output)
        self.assertIn(f"output_json={output_json}", output)
        self.assertEqual(probe_mock.call_args.kwargs["names"], ["vantora", "meridel"])
        self.assertEqual(probe_mock.call_args.kwargs["profile_dir"], profile_dir.resolve())
        self.assertEqual(probe_mock.call_args.kwargs["chrome_executable"], chrome_executable.resolve())
        self.assertFalse(probe_mock.call_args.kwargs["headless"])
        self.assertEqual(probe_mock.call_args.kwargs["nice_class"], "9,OR,42")
        self.assertEqual(write_mock.call_args.args[0], str(output_json))

    def test_main_rejects_mocked_unsupported_command(self) -> None:
        parser = mock.Mock()
        parser.parse_args.return_value = mock.Mock(command="mystery")
        with mock.patch("brandpipe.cli.build_parser", return_value=parser):
            with self.assertRaisesRegex(SystemExit, "unsupported command: mystery"):
                main([])

    def test_module_entrypoint_raises_system_exit_with_main_return_code(self) -> None:
        with mock.patch("sys.argv", ["brandpipe.cli", "run", "--config", "fixture.toml"]):
            with mock.patch("brandpipe.run_cli.run_config_command") as run_mock:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message=r"'brandpipe\.cli' found in sys\.modules",
                        category=RuntimeWarning,
                    )
                    with self.assertRaises(SystemExit) as exc:
                        runpy.run_module("brandpipe.cli", run_name="__main__")

        self.assertEqual(exc.exception.code, 0)
        run_mock.assert_called_once_with("fixture.toml")


if __name__ == "__main__":
    unittest.main()
