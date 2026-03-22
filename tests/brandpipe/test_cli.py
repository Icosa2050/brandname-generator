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
            db_path = root / "brandpipe.db"
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
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = ""

                    [export]
                    out_csv = "{root / 'finalists_{run_id}.csv'}"
                    top_n = 10
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                init_code = main(["init-db", "--db", str(db_path)])
                run_code = main(["run", "--config", str(config_path)])
            self.assertEqual(init_code, 0)
            self.assertEqual(run_code, 0)

            with db.open_db(db_path) as conn:
                latest = conn.execute("SELECT MAX(id) AS run_id FROM runs").fetchone()
                assert latest is not None
                run_id = int(latest["run_id"])

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

    def test_status_command_errors_for_missing_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)

            with self.assertRaisesRegex(SystemExit, "run not found: 99"):
                main(["status", "--db", str(db_path), "--run-id", "99"])

    def test_run_batch_command_executes_multiple_fixture_briefs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            fixture_path = root / "fixture.json"
            template_config_path = root / "template.toml"
            briefs_path = root / "briefs.toml"
            fixture_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "candidates": [
                        {"name": "vantora"},
                        {"name": "baltera"}
                      ]
                    }
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            template_config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "batch-fixture"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "placeholder"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = ""

                    [export]
                    out_csv = "{root / 'finalists_{run_id}.csv'}"
                    top_n = 10
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            briefs_path.write_text(
                textwrap.dedent(
                    """
                    [[briefs]]
                    slug = "alpha"
                    product_core = "utility-cost settlement software"
                    target_users = ["private landlords"]

                    [[briefs]]
                    slug = "beta"
                    product_core = "tenant ledger software"
                    target_users = ["property managers"]
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-batch",
                        "--template-config",
                        str(template_config_path),
                        "--briefs-file",
                        str(briefs_path),
                        "--batch-id",
                        "test-batch",
                    ]
                )

            self.assertEqual(exit_code, 0)
            output = stdout.getvalue()
            self.assertIn("batch_id=test-batch", output)
            self.assertIn("requested=2 succeeded=2 failed=0", output)

            with db.open_db(db_path) as conn:
                rows = db.list_runs(conn, limit=10, batch_id="test-batch")
                self.assertEqual(len(rows), 2)
                self.assertEqual([int(row["batch_index"]) for row in rows], [0, 1])
                for row in rows:
                    metrics = json.loads(str(row["metrics_json"]))
                    self.assertEqual(metrics["counts"]["ideation_candidates"], 2)

            status_stdout = io.StringIO()
            with contextlib.redirect_stdout(status_stdout):
                status_code = main(
                    [
                        "status",
                        "--db",
                        str(db_path),
                        "--batch-id",
                        "test-batch",
                        "--show-metrics",
                    ]
                )
            self.assertEqual(status_code, 0)
            self.assertIn("batch_id=test-batch runs=2", status_stdout.getvalue())
            self.assertIn("metrics=", status_stdout.getvalue())

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
