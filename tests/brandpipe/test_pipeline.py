# ruff: noqa: E402
from __future__ import annotations

import csv
import json
import sys
import tempfile
import textwrap
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe import db
from brandpipe.models import CandidateResult, ResultStatus
from brandpipe.pipeline import load_config, recheck_pending_web, recheck_tmview, run_pipeline
from brandpipe.scoring import build_attractiveness_result
from brandpipe.tmview import TmviewProbeResult


class PipelineTests(unittest.TestCase):
    def test_load_config_parses_broadside_ideation_knobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text('{"candidates":[{"name":"vantora"}]}\n', encoding="utf-8")
            config_path = root / "run.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "broadside-config"
                    db_path = "{root / 'brandpipe.db'}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"
                    rounds = 2
                    candidates_per_round = 18
                    overgenerate_factor = 2.8
                    round_seed_min = 6
                    round_seed_max = 12
                    seed_pool_multiplier = 12
                    seed_saturation_limit = 2
                    per_family_cap = 4
                    lexicon_core_limit = 10
                    lexicon_modifier_limit = 9
                    lexicon_associative_limit = 8
                    lexicon_morpheme_limit = 16
                    local_filter_saturation_limit = 2
                    local_filter_lead_fragment_limit = 1
                    local_filter_lead_fragment_length = 4
                    local_filter_lead_skeleton_limit = 2

                    [ideation.pseudoword]
                    language_plugin = "orthographic_english"
                    language_plugins = ["orthographic_english", "orthographic_german"]
                    seed_count = 12

                    [validation]
                    checks = ""
                    parallel_workers = 5

                    [export]
                    out_csv = "{root / 'finalists_{run_id}.csv'}"
                    top_n = 10
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.ideation.rounds, 2)
        self.assertEqual(config.ideation.candidates_per_round, 18)
        self.assertEqual(config.ideation.round_seed_min, 6)
        self.assertEqual(config.ideation.round_seed_max, 12)
        self.assertEqual(config.ideation.seed_pool_multiplier, 12)
        self.assertEqual(config.ideation.seed_saturation_limit, 2)
        self.assertEqual(config.ideation.per_family_cap, 4)
        self.assertEqual(config.ideation.lexicon_core_limit, 10)
        self.assertEqual(config.ideation.lexicon_modifier_limit, 9)
        self.assertEqual(config.ideation.lexicon_associative_limit, 8)
        self.assertEqual(config.ideation.lexicon_morpheme_limit, 16)
        self.assertEqual(config.ideation.local_filter_saturation_limit, 2)
        self.assertEqual(config.ideation.local_filter_lead_fragment_limit, 1)
        self.assertEqual(config.ideation.local_filter_lead_fragment_length, 4)
        self.assertEqual(config.ideation.local_filter_lead_skeleton_limit, 2)
        assert config.ideation.pseudoword is not None
        self.assertEqual(config.ideation.pseudoword.language_plugin, "orthographic_english")
        self.assertEqual(
            config.ideation.pseudoword.language_plugins,
            ("orthographic_english", "orthographic_german"),
        )
        self.assertEqual(config.validation.parallel_workers, 5)

    def test_fixture_run_exports_ranked_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
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
            config_path = root / "run.toml"
            db_path = root / "brandpipe.db"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "fixture-run"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"
                    rounds = 1
                    candidates_per_round = 3

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
            run_id = run_pipeline(config_path)
            export_path = root / f"finalists_{run_id}.csv"
            self.assertTrue(export_path.exists())
            with export_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3)
            self.assertIn("attractiveness_score", rows[0])
            self.assertIn("attractiveness_status", rows[0])
            self.assertIn("attractiveness_reasons", rows[0])

            with db.open_db(db_path) as conn:
                row = db.get_run(conn, run_id=run_id)
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["status"], "completed")
                metrics = json.loads(str(row["metrics_json"]))
                self.assertEqual(metrics["counts"]["ideation_candidates"], 3)
                self.assertEqual(metrics["counts"]["ranked_candidates"], 3)
                self.assertEqual(metrics["decision_counts"]["candidate"], 3)
                self.assertIn("ideation", metrics["durations_ms"])
                self.assertTrue(metrics["export_path"].endswith(f"finalists_{run_id}.csv"))

    def test_validation_failure_is_isolated_to_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "candidates": [
                        {"name": "vantora"},
                        {"name": "certivo"}
                      ]
                    }
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            config_path = root / "run.toml"
            db_path = root / "brandpipe.db"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "validation-isolation"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = "domain"
                    parallel_workers = 2
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            def fake_validate_candidate(*, name: str, config: object) -> list[object]:
                if name == "certivo":
                    raise RuntimeError("probe exploded")
                return []

            with mock.patch("brandpipe.pipeline.validate_candidate", side_effect=fake_validate_candidate):
                run_id = run_pipeline(config_path)

            with db.open_db(db_path) as conn:
                row = db.get_run(conn, run_id=run_id)
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["status"], "completed")
                runtime_row = conn.execute(
                    """
                    SELECT r.result_key, r.status, c.name
                    FROM candidate_results r
                    JOIN candidates c ON c.id = r.candidate_id
                    WHERE c.run_id = ?
                      AND r.result_key = 'validation_runtime'
                    """,
                    (run_id,),
                ).fetchone()
                self.assertIsNotNone(runtime_row)
                assert runtime_row is not None
                self.assertEqual(runtime_row["status"], "unavailable")
                self.assertEqual(runtime_row["name"], "certivo")
                metrics = json.loads(str(row["metrics_json"]))
                self.assertEqual(metrics["validation_status_counts"]["unavailable"], 1)
                self.assertEqual(metrics["validation_check_counts"]["validation_runtime"], 1)

    def test_pipeline_can_validate_candidates_concurrently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "candidates": [
                        {"name": "vantora"},
                        {"name": "certivo"},
                        {"name": "meridel"}
                      ]
                    }
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            config_path = root / "run.toml"
            db_path = root / "brandpipe.db"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "validation-concurrency"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = "domain"
                    parallel_workers = 3
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            active = 0
            max_active = 0
            lock = threading.Lock()

            def fake_validate_candidate(*, name: str, config: object) -> list[CandidateResult]:
                nonlocal active, max_active
                del name, config
                with lock:
                    active += 1
                    max_active = max(max_active, active)
                try:
                    time.sleep(0.05)
                    return [
                        CandidateResult(
                            check_name="domain",
                            status=ResultStatus.PASS,
                            score_delta=0.0,
                            reason="",
                            details={},
                        )
                    ]
                finally:
                    with lock:
                        active -= 1

            with mock.patch("brandpipe.pipeline.validate_candidate", side_effect=fake_validate_candidate):
                run_id = run_pipeline(config_path)

            self.assertGreaterEqual(max_active, 2)
            with db.open_db(db_path) as conn:
                row = db.get_run(conn, run_id=run_id)
                self.assertIsNotNone(row)
                assert row is not None
                metrics = json.loads(str(row["metrics_json"]))
                self.assertEqual(metrics["counts"]["validation_results"], 3)
                self.assertEqual(metrics["validation_check_counts"]["domain"], 3)

    def test_pipeline_ranking_can_downgrade_flat_names_on_attractiveness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "candidates": [
                        {"name": "baltera"},
                        {"name": "jaxqen"}
                      ]
                    }
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            config_path = root / "run.toml"
            db_path = root / "brandpipe.db"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "attractiveness-ranking"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = run_pipeline(config_path)

            with db.open_db(db_path) as conn:
                rankings = conn.execute(
                    """
                    SELECT c.name, rk.decision, rk.warning_count, rk.total_score
                    FROM candidate_rankings rk
                    JOIN candidates c ON c.id = rk.candidate_id
                    WHERE c.run_id = ?
                    ORDER BY rk.total_score DESC, c.name
                    """,
                    (run_id,),
                ).fetchall()
                self.assertEqual(len(rankings), 2)
                decisions = {str(row["name"]): str(row["decision"]) for row in rankings}
                self.assertEqual(decisions["baltera"], "candidate")
                self.assertEqual(decisions["jaxqen"], "watch")

    def test_pipeline_injects_recent_blocked_patterns_into_effective_brief(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                prior_run = db.create_run(
                    conn,
                    title="prior-blocked",
                    brief={"product_core": "utility settlement"},
                    config={"provider": "fixture"},
                )
                db.add_candidates(
                    conn,
                    run_id=prior_run,
                    names=["balaria", "chivaria", "claritea"],
                    source_kind="fixture",
                    source_detail="{}",
                )
                prior_rows = db.list_candidates(conn, run_id=prior_run)
                db.upsert_rankings(
                    conn,
                    rows=[
                        (int(row["id"]), -1.0, 1, 0, 0, 0, "blocked")
                        for row in prior_rows
                    ],
                )
                db.set_run_state(
                    conn,
                    run_id=prior_run,
                    status="completed",
                    current_step="done",
                    completed=True,
                )
                winner_run = db.create_run(
                    conn,
                    title="prior-winner",
                    brief={"product_core": "utility settlement"},
                    config={"provider": "fixture"},
                )
                db.add_candidates(
                    conn,
                    run_id=winner_run,
                    names=["baltera"],
                    source_kind="fixture",
                    source_detail="{}",
                )
                winner_rows = db.list_candidates(conn, run_id=winner_run)
                db.upsert_rankings(
                    conn,
                    rows=[(int(winner_rows[0]["id"]), 100.0, 0, 0, 0, 0, "candidate")],
                )
                db.upsert_result(
                    conn,
                    candidate_id=int(winner_rows[0]["id"]),
                    result_key="domain",
                    status="pass",
                    reason="domain_pass",
                    score_delta=0.0,
                    details={"mode": "default_any"},
                )
                db.upsert_result(
                    conn,
                    candidate_id=int(winner_rows[0]["id"]),
                    result_key="package",
                    status="pass",
                    reason="package_pass",
                    score_delta=0.0,
                    details={"store": "npm"},
                )
                db.upsert_result(
                    conn,
                    candidate_id=int(winner_rows[0]["id"]),
                    result_key="web",
                    status="pass",
                    reason="web_pass",
                    score_delta=0.0,
                    details={"provider": "brave"},
                )
                attractiveness = build_attractiveness_result("baltera")
                db.upsert_result(
                    conn,
                    candidate_id=int(winner_rows[0]["id"]),
                    result_key=attractiveness.check_name,
                    status=attractiveness.status.value,
                    reason=attractiveness.reason,
                    score_delta=attractiveness.score_delta,
                    details=attractiveness.details,
                )
                db.set_run_state(
                    conn, run_id=winner_run, status="completed", current_step="done", completed=True
                )
                watch_run = db.create_run(
                    conn,
                    title="prior-watch",
                    brief={"product_core": "utility settlement"},
                    config={"provider": "fixture"},
                )
                db.add_candidates(
                    conn,
                    run_id=watch_run,
                    names=["softalia"],
                    source_kind="fixture",
                    source_detail="{}",
                )
                watch_rows = db.list_candidates(conn, run_id=watch_run)
                db.upsert_rankings(
                    conn,
                    rows=[(int(watch_rows[0]["id"]), 100.0, 0, 0, 0, 1, "watch")],
                )
                db.upsert_result(
                    conn,
                    candidate_id=int(watch_rows[0]["id"]),
                    result_key="web",
                    status="warn",
                    reason="web_check_pending",
                    score_delta=-2.0,
                    details={"provider": "browser_google"},
                )
                watch_attractiveness = build_attractiveness_result("softalia")
                db.upsert_result(
                    conn,
                    candidate_id=int(watch_rows[0]["id"]),
                    result_key=watch_attractiveness.check_name,
                    status=watch_attractiveness.status.value,
                    reason=watch_attractiveness.reason,
                    score_delta=watch_attractiveness.score_delta,
                    details=watch_attractiveness.details,
                )
                db.set_run_state(
                    conn,
                    run_id=watch_run,
                    status="completed",
                    current_step="done",
                    completed=True,
                )
                self.assertEqual(db.recent_positive_feedback(conn)["names"], ["baltera"])
                conn.commit()

            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                json.dumps({"candidates": [{"name": "vantora"}, {"name": "clarien"}]}),
                encoding="utf-8",
            )
            config_path = root / "run.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "feedback-run"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = run_pipeline(config_path)

            with db.open_db(db_path) as conn:
                row = db.get_run(conn, run_id=run_id)
                self.assertIsNotNone(row)
                assert row is not None
                brief_payload = json.loads(str(row["brief_json"]))
                self.assertIn("aria", brief_payload["forbidden_directions"])
                self.assertIn("avoid repeating crowded suffix families", brief_payload["notes"])
                metrics = json.loads(str(row["metrics_json"]))
                self.assertEqual(metrics["ideation"]["feedback"]["applied"], True)
                self.assertIn("aria", metrics["ideation"]["feedback"]["suffixes"])

    def test_recheck_pending_web_uses_browser_provider_only_for_recheck(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            browser_profile_dir = root / "playwright-profile"
            browser_profile_dir.mkdir()

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="pending-web-recheck",
                    brief={"product_core": "utility-cost settlement software"},
                    config={
                        "validation": {
                            "checks": ["domain", "package", "social", "web"],
                            "web_search_order": "brave,google_cse",
                            "web_retry_attempts": 2,
                        }
                    },
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["fendrival"],
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
                    metrics={"counts": {"ranked_candidates": 1}, "export_path": ""},
                )
                db.set_run_state(conn, run_id=run_id, status="completed", current_step="done", completed=True)
                conn.commit()

            captured_configs: list[object] = []

            def fake_validate_candidate(*, name: str, config: object) -> list[CandidateResult]:
                _ = name
                captured_configs.append(config)
                return [
                    CandidateResult(
                        check_name="web",
                        status=ResultStatus.PASS,
                        score_delta=0.0,
                        reason="",
                        details={"provider": "browser_google"},
                    )
                ]

            with mock.patch("brandpipe.pipeline.validate_candidate", side_effect=fake_validate_candidate):
                summary = recheck_pending_web(
                    db_path=db_path,
                    run_id=run_id,
                    browser_profile_dir=browser_profile_dir,
                )

            self.assertEqual(summary["retried"], 1)
            self.assertEqual(len(captured_configs), 1)
            captured = captured_configs[0]
            self.assertEqual(captured.checks, ["web"])
            self.assertEqual(captured.web_search_order, "browser_google")
            self.assertEqual(captured.web_retry_attempts, 0)
            self.assertEqual(captured.web_browser_profile_dir, str(browser_profile_dir))

    def test_recheck_tmview_blocks_candidate_and_rewrites_export(self) -> None:
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
                    title="tmview-recheck",
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
                    names=["cordnix"],
                    source_kind="fixture",
                    source_detail="{}",
                )
                candidate = db.list_candidates(conn, run_id=run_id)[0]
                candidate_id = int(candidate["id"])
                db.upsert_result(
                    conn,
                    candidate_id=candidate_id,
                    result_key="web",
                    status="pass",
                    score_delta=0.0,
                    reason="",
                    details={},
                )
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
                        "counts": {
                            "ranked_candidates": 1,
                            "validation_results": 1,
                            "export_rows": 1,
                        },
                        "decision_counts": {"candidate": 1},
                        "export_path": str(export_path),
                    },
                )
                db.set_run_state(conn, run_id=run_id, status="completed", current_step="done", completed=True)
                conn.commit()

            with mock.patch(
                "brandpipe.pipeline.probe_tmview_names",
                return_value=[
                    TmviewProbeResult(
                        name="cordnix",
                        url="https://example.test/tmview",
                        query_ok=True,
                        source="tmview_playwright",
                        exact_hits=0,
                        near_hits=2,
                        result_count=6,
                        sample_text="Cordix",
                    )
                ],
            ):
                summary = recheck_tmview(
                    db_path=db_path,
                    profile_dir=profile_dir,
                    run_id=run_id,
                )

            self.assertEqual(summary["retried"], 1)
            self.assertEqual(summary["runs"][0]["blocked"], 1)
            with db.open_db(db_path) as conn:
                result_row = conn.execute(
                    """
                    SELECT status, reason
                    FROM candidate_results
                    WHERE candidate_id = ? AND result_key = 'tmview'
                    """,
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(result_row)
                assert result_row is not None
                self.assertEqual(result_row["status"], "fail")
                self.assertEqual(result_row["reason"], "tmview_near_collision")

                ranking = conn.execute(
                    """
                    SELECT decision, blocker_count
                    FROM candidate_rankings
                    WHERE candidate_id = ?
                    """,
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(ranking)
                assert ranking is not None
                self.assertEqual(ranking["decision"], "blocked")
                self.assertEqual(ranking["blocker_count"], 1)

            self.assertTrue(export_path.exists())
            with export_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["decision"], "blocked")

    def test_pipeline_applies_tmview_during_normal_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                json.dumps({"candidates": [{"name": "hefkora"}]}),
                encoding="utf-8",
            )
            db_path = root / "brandpipe.db"
            config_path = root / "run.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "tmview-inline"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = "tmview"
                    tmview_profile_dir = "{root / 'tmview-profile'}"
                    tmview_chrome_executable = "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with mock.patch(
                "brandpipe.pipeline.probe_tmview_names",
                return_value=[
                    TmviewProbeResult(
                        name="hefkora",
                        url="https://example.test/tmview",
                        query_ok=True,
                        source="tmview_playwright",
                        exact_hits=0,
                        near_hits=1,
                        result_count=7,
                        sample_text="HEFORA",
                    )
                ],
            ) as probe_mock:
                run_id = run_pipeline(config_path)

            probe_mock.assert_called_once()
            with db.open_db(db_path) as conn:
                candidate = db.list_candidates(conn, run_id=run_id)[0]
                candidate_id = int(candidate["id"])
                result_row = conn.execute(
                    """
                    SELECT status, reason
                    FROM candidate_results
                    WHERE candidate_id = ? AND result_key = 'tmview'
                    """,
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(result_row)
                assert result_row is not None
                self.assertEqual(result_row["status"], "fail")
                self.assertEqual(result_row["reason"], "tmview_near_collision")

    def test_pipeline_marks_tmview_unavailable_when_profile_is_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                json.dumps({"candidates": [{"name": "hefkora"}]}),
                encoding="utf-8",
            )
            db_path = root / "brandpipe.db"
            config_path = root / "run.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "tmview-unconfigured"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = "tmview"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = run_pipeline(config_path)

            with db.open_db(db_path) as conn:
                candidate = db.list_candidates(conn, run_id=run_id)[0]
                candidate_id = int(candidate["id"])
                result_row = conn.execute(
                    """
                    SELECT status, reason
                    FROM candidate_results
                    WHERE candidate_id = ? AND result_key = 'tmview'
                    """,
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(result_row)
                assert result_row is not None
                self.assertEqual(result_row["status"], "unavailable")
                self.assertEqual(result_row["reason"], "tmview_profile_unconfigured")

    def test_pipeline_applies_local_collision_filter_before_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                prior_run = db.create_run(
                    conn,
                    title="prior-corpus",
                    brief={"product_core": "utility settlement"},
                    config={"provider": "fixture"},
                )
                db.add_candidates(
                    conn,
                    run_id=prior_run,
                    names=["pryndex"],
                    source_kind="fixture",
                    source_detail="{}",
                )
                prior_rows = db.list_candidates(conn, run_id=prior_run)
                db.upsert_rankings(
                    conn,
                    rows=[(int(prior_rows[0]["id"]), 100.0, 0, 0, 0, 0, "candidate")],
                )
                db.set_run_state(
                    conn,
                    run_id=prior_run,
                    status="completed",
                    current_step="done",
                    completed=True,
                )
                conn.commit()

            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                json.dumps({"candidates": [{"name": "pryndel"}, {"name": "varkten"}]}),
                encoding="utf-8",
            )
            config_path = root / "run.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "local-filter-run"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = run_pipeline(config_path)

            with db.open_db(db_path) as conn:
                candidate_rows = db.list_candidates(conn, run_id=run_id)
                self.assertEqual([str(row["name"]) for row in candidate_rows], ["varkten"])
                row = db.get_run(conn, run_id=run_id)
                self.assertIsNotNone(row)
                assert row is not None
                metrics = json.loads(str(row["metrics_json"]))
                self.assertEqual(metrics["counts"]["local_filter_passed"], 1)
                self.assertEqual(metrics["counts"]["ideation_candidates"], 1)
                dropped = metrics["ideation"]["local_filter"]["dropped"]
                self.assertTrue(
                    "phonetic_corpus_collision" in dropped or "trigram_corpus_collision" in dropped
                )

    def test_pipeline_applies_taste_filter_before_local_collision_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                json.dumps({"candidates": [{"name": "krelixen"}, {"name": "baltera"}]}),
                encoding="utf-8",
            )
            config_path = root / "run.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "taste-filter-run"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = run_pipeline(config_path)

            with db.open_db(db_path) as conn:
                candidate_rows = db.list_candidates(conn, run_id=run_id)
                self.assertEqual([str(row["name"]) for row in candidate_rows], ["baltera"])
                row = db.get_run(conn, run_id=run_id)
                self.assertIsNotNone(row)
                assert row is not None
                metrics = json.loads(str(row["metrics_json"]))
                self.assertEqual(metrics["counts"]["taste_filter_passed"], 1)
                self.assertEqual(metrics["counts"]["local_filter_passed"], 1)
                self.assertEqual(metrics["counts"]["ideation_candidates"], 1)
                self.assertIn("banned_suffix_family", metrics["ideation"]["taste_filter"]["dropped"])

    def test_pipeline_passes_recent_avoidance_context_into_ideation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                prior_run = db.create_run(
                    conn,
                    title="prior-feedback",
                    brief={"product_core": "utility settlement"},
                    config={"provider": "fixture"},
                )
                db.add_candidates(
                    conn,
                    run_id=prior_run,
                    names=["baldex"],
                    source_kind="fixture",
                    source_detail="{}",
                )
                prior_rows = db.list_candidates(conn, run_id=prior_run)
                db.upsert_rankings(
                    conn,
                    rows=[(int(prior_rows[0]["id"]), 10.0, 1, 0, 0, 0, "blocked")],
                )
                db.upsert_result(
                    conn,
                    candidate_id=int(prior_rows[0]["id"]),
                    result_key="web",
                    status="fail",
                    score_delta=-10.0,
                    reason="web_near_collision",
                    details={},
                )
                db.update_run_metrics(
                    conn,
                    run_id=prior_run,
                    metrics={
                        "ideation": {
                            "local_filter": {
                                "dropped_examples": {
                                    "trigram_corpus_collision": ["precen:precerix"],
                                }
                            }
                        }
                    },
                )
                db.set_run_state(
                    conn,
                    run_id=prior_run,
                    status="completed",
                    current_step="done",
                    completed=True,
                )
                conn.commit()

            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                json.dumps({"candidates": [{"name": "vantora"}]}),
                encoding="utf-8",
            )
            config_path = root / "run.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "avoidance-run"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            captured: dict[str, object] = {}

            def fake_generate_candidates(*, brief, config, success_context=None, avoidance_context=None):  # type: ignore[no-untyped-def]
                captured["success_context"] = success_context
                captured["avoidance_context"] = avoidance_context
                return ["vantora"], {"provider": "fixture", "usage": {}, "rounds": 1}

            with mock.patch("brandpipe.pipeline.generate_candidates", side_effect=fake_generate_candidates):
                run_pipeline(config_path)

            avoidance_context = captured.get("avoidance_context")
            self.assertIsInstance(avoidance_context, dict)
            assert isinstance(avoidance_context, dict)
            self.assertIn("local_examples", avoidance_context)
            self.assertIn("local_patterns", avoidance_context)
            self.assertIn("external_failures", avoidance_context)
            self.assertIn("external_patterns", avoidance_context)
            self.assertIn("external_terminal_families", avoidance_context)
            self.assertIn("external_lead_hints", avoidance_context)
            self.assertIn("external_tail_hints", avoidance_context)
            self.assertIn("external_reason_patterns", avoidance_context)
            self.assertIn("external_avoid_names", avoidance_context)
            self.assertIn("external_terminal_skeletons", avoidance_context)
            self.assertIn("external_fragment_hints", avoidance_context)
            self.assertEqual(avoidance_context["local_examples"][0]["example"], "precen:precerix")
            self.assertIn("web_near_collision", avoidance_context["external_failures"])
            success_context = captured.get("success_context")
            self.assertIsInstance(success_context, dict)
            assert isinstance(success_context, dict)
            self.assertIn("names", success_context)
            self.assertIn("endings", success_context)


if __name__ == "__main__":
    unittest.main()
