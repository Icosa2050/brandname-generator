# ruff: noqa: E402
from __future__ import annotations

import csv
import io
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
from brandpipe.models import CandidateResult, NameFamily, ResultStatus, SurfacePolicy, SurfacedCandidate
from brandpipe.pipeline import load_config, recheck_pending_web, recheck_tmview, run_pipeline
from brandpipe.scoring import build_attractiveness_result
from brandpipe.tmview import TmviewProbeResult
from brandpipe.validation_runtime import ProbeResult


class PipelineTests(unittest.TestCase):
    def _run_pipeline_without_validation(self, config_path: Path) -> int:
        with mock.patch("brandpipe.pipeline.run_validation_jobs", return_value=None):
            return run_pipeline(config_path)

    def test_load_config_defaults_db_path_into_run_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text('{"candidates":[{"name":"vantora"}]}\n', encoding="utf-8")
            config_path = root / "custom_probe.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "custom-probe"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"
                    rounds = 1
                    candidates_per_round = 4
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(
            config.db_path,
            (root / "test_outputs/brandpipe/run/custom_probe/brandpipe.db").resolve(),
        )

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
                    family_mix_profile = "family_default"
                    family_llm_retry_limit = 3
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
                    family_prompt_template_files = {{ smooth_blend = "relative_smooth.txt" }}

                    [ideation.pseudoword]
                    language_plugin = "orthographic_english"
                    language_plugins = ["orthographic_english", "orthographic_german"]
                    seed_count = 12
                    rare_seed_count = 10
                    rare_profile = "aggressive"

                    [validation]
                    checks = ""
                    parallel_workers = 5
                    web_search_order = "brave,serper"

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
        self.assertEqual(config.ideation.family_mix_profile, "family_default")
        self.assertEqual(config.ideation.family_llm_retry_limit, 3)
        self.assertEqual(
            config.ideation.family_prompt_template_files["smooth_blend"],
            (root / "relative_smooth.txt").resolve(),
        )
        assert config.ideation.pseudoword is not None
        self.assertEqual(config.ideation.pseudoword.language_plugin, "orthographic_english")
        self.assertEqual(
            config.ideation.pseudoword.language_plugins,
            ("orthographic_english", "orthographic_german"),
        )
        self.assertEqual(config.ideation.pseudoword.rare_seed_count, 10)
        self.assertEqual(config.ideation.pseudoword.rare_profile, "aggressive")
        self.assertEqual(config.validation.parallel_workers, 5)
        self.assertEqual(config.validation.web_search_order, "serper,brave")

    def test_load_config_parses_naming_policy_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text('{"candidates":[{"name":"vantora"}]}\n', encoding="utf-8")
            config_path = root / "run.toml"
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    [run]
                    title = "policy-config"
                    db_path = "{root / 'brandpipe.db'}"

                    [brief]
                    product_core = "utility-cost settlement software"

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"

                    [ideation.naming_policy.shape]
                    min_length = 5
                    max_length = 16

                    [ideation.naming_policy.taste]
                    generic_safe_openings = ["proto"]

                    [ideation.naming_policy.local_collision]
                    terminal_bigram_quota = 3

                    [ideation.naming_policy.surface]
                    runic_fallbacks = ["ALTVOR"]

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.ideation.naming_policy.shape.min_length, 5)
        self.assertEqual(config.ideation.naming_policy.shape.max_length, 16)
        self.assertEqual(config.ideation.naming_policy.taste.generic_safe_openings, ("proto",))
        self.assertEqual(config.ideation.naming_policy.local_collision.terminal_bigram_quota, 3)
        self.assertEqual(config.ideation.naming_policy.surface.runic_fallbacks, ("ALTVOR",))
        self.assertEqual(config.validation.name_shape_policy.min_length, 5)
        self.assertEqual(config.validation.name_shape_policy.max_length, 16)

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
                    family_quotas = {{ smooth_blend = 3 }}

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
            run_id = self._run_pipeline_without_validation(config_path)
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
                self.assertEqual(sum(int(value) for value in metrics["decision_counts"].values()), 3)
                self.assertIn("ideation", metrics["durations_ms"])
                self.assertTrue(metrics["export_path"].endswith(f"finalists_{run_id}.csv"))

    def test_family_default_profile_preserves_family_mix_in_ranked_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "candidates": [
                        {"name": "nimbalyst"},
                        {"name": "brandnamic"}
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
                    title = "surface-diverse-run"
                    db_path = "{db_path}"

                    [brief]
                    product_core = "incident response signal coordination"
                    target_users = ["operators", "responders"]
                    trust_signals = ["clarity", "speed"]

                    [ideation]
                    provider = "fixture"
                    fixture_input = "{fixture_path}"
                    family_mix_profile = "family_default"
                    late_fusion_min_per_family = 1
                    family_quotas = {{ literal_tld_hack = 1, smooth_blend = 1, mascot_mutation = 1, contrarian_dictionary = 1, brutalist_utility = 1 }}

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            surfaced_candidates = [
                SurfacedCandidate(
                    display_name="lytoria",
                    name_normalized="lytoria",
                    family=NameFamily.LITERAL_TLD_HACK,
                    surface_policy=SurfacePolicy.ALPHA_LOWER,
                ),
                SurfacedCandidate(
                    display_name="nimbalyst",
                    name_normalized="nimbalyst",
                    family=NameFamily.SMOOTH_BLEND,
                    surface_policy=SurfacePolicy.ALPHA_LOWER,
                ),
                SurfacedCandidate(
                    display_name="Okoala",
                    name_normalized="okoala",
                    family=NameFamily.MASCOT_MUTATION,
                    surface_policy=SurfacePolicy.MIXED_CASE_ALPHA,
                ),
                SurfacedCandidate(
                    display_name="Vaermon",
                    name_normalized="vaermon",
                    family=NameFamily.RUNIC_FORGE,
                    surface_policy=SurfacePolicy.MIXED_CASE_ALPHA,
                ),
                SurfacedCandidate(
                    display_name="Harbor",
                    name_normalized="harbor",
                    family=NameFamily.CONTRARIAN_DICTIONARY,
                    surface_policy=SurfacePolicy.MIXED_CASE_ALPHA,
                ),
                SurfacedCandidate(
                    display_name="croften",
                    name_normalized="croften",
                    family=NameFamily.BRUTALIST_UTILITY,
                    surface_policy=SurfacePolicy.ALPHA_LOWER,
                ),
            ]
            ideation_report = {
                "provider": "fixture",
                "family_mix_profile": "family_default",
                "family_counts": {
                    "literal_tld_hack": 1,
                    "smooth_blend": 1,
                    "mascot_mutation": 1,
                    "runic_forge": 1,
                    "contrarian_dictionary": 1,
                    "brutalist_utility": 1,
                },
                "family_reports": {},
                "candidate_count": 6,
            }

            with (
                mock.patch("brandpipe.pipeline.generate_candidate_surfaces", return_value=(surfaced_candidates, ideation_report)),
            ):
                run_id = self._run_pipeline_without_validation(config_path)

            with db.open_db(db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT c.display_name, c.family, rk.rank_position, rk.family_rank
                    FROM candidate_rankings rk
                    JOIN candidates c ON c.id = rk.candidate_id
                    WHERE c.run_id = ?
                    ORDER BY rk.rank_position ASC
                    """,
                    (run_id,),
                ).fetchall()
                families = [str(row["family"]) for row in rows]
                self.assertIn("literal_tld_hack", families)
                self.assertIn("smooth_blend", families)
                self.assertIn("mascot_mutation", families)
                self.assertIn("runic_forge", families)
                self.assertIn("contrarian_dictionary", families)
                self.assertIn("brutalist_utility", families)
                self.assertEqual(len({str(row["family"]) for row in rows[:6]}), 6)
                metrics = json.loads(str(db.get_run(conn, run_id=run_id)["metrics_json"]))
                self.assertEqual(metrics["ideation"]["surface_candidate_count"], 6)
                self.assertEqual(metrics["ideation"]["family_counts"]["runic_forge"], 1)

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
                    family_quotas = {{ smooth_blend = 2 }}

                    [validation]
                    checks = "domain"
                    parallel_workers = 2
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            def fake_probe_check(*, check_name: str, name: str, config: object):
                del config
                if check_name != "domain":
                    self.fail(f"unexpected check {check_name}")
                if name == "certivo":
                    raise RuntimeError("probe exploded")
                return ProbeResult(
                    candidate_result=CandidateResult(
                        check_name="domain",
                        status=ResultStatus.PASS,
                        score_delta=0.0,
                        reason="",
                        details={},
                    ),
                )

            with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
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
                      AND r.result_key = 'domain'
                      AND c.name = 'certivo'
                    """,
                    (run_id,),
                ).fetchone()
                self.assertIsNotNone(runtime_row)
                assert runtime_row is not None
                self.assertEqual(runtime_row["status"], "unavailable")
                self.assertEqual(runtime_row["name"], "certivo")
                metrics = json.loads(str(row["metrics_json"]))
                self.assertEqual(metrics["validation_status_counts"]["unavailable"], 1)
                self.assertEqual(metrics["validation_check_counts"]["domain"], 2)

    def test_pipeline_validates_candidates_serially_via_queue(self) -> None:
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
                    family_quotas = {{ smooth_blend = 3 }}

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

            def fake_probe_check(*, check_name: str, name: str, config: object):
                nonlocal active, max_active
                del check_name, name, config
                with lock:
                    active += 1
                    max_active = max(max_active, active)
                try:
                    time.sleep(0.05)
                    return ProbeResult(
                        candidate_result=CandidateResult(
                            check_name="domain",
                            status=ResultStatus.PASS,
                            score_delta=0.0,
                            reason="",
                            details={},
                        ),
                    )
                finally:
                    with lock:
                        active -= 1

            with mock.patch("brandpipe.validation_queue.probe_check", side_effect=fake_probe_check):
                run_id = run_pipeline(config_path)

            self.assertEqual(max_active, 1)
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
                    family_quotas = {{ smooth_blend = 2 }}

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = self._run_pipeline_without_validation(config_path)

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
                self.assertNotEqual(decisions["baltera"], "rejected")
                self.assertIn(decisions["jaxqen"], {"watch", "blocked", "rejected", "degraded"})

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
                    details={"provider": "serper"},
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
                    family_quotas = {{ smooth_blend = 2 }}

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = self._run_pipeline_without_validation(config_path)

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

    def test_recheck_pending_web_uses_serper_first_order_for_recheck(self) -> None:
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
                            "web_search_order": "brave,serper",
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
                        details={"provider": "serper"},
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
            self.assertEqual(captured.web_search_order, "serper,brave")
            self.assertEqual(captured.web_retry_attempts, 0)
            self.assertEqual(captured.web_browser_profile_dir, str(browser_profile_dir))

    def test_recheck_pending_web_tolerates_invalid_stored_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "brandpipe.db"
            browser_profile_dir = root / "playwright-profile"
            browser_profile_dir.mkdir()

            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="pending-web-invalid-json",
                    brief={"product_core": "utility-cost settlement software"},
                    config={
                        "validation": {
                            "checks": ["web"],
                        },
                        "export": {
                            "top_n": 5,
                        },
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
                conn.execute("UPDATE runs SET config_json = ?, metrics_json = ? WHERE id = ?", ("{", "{", run_id))
                db.set_run_state(conn, run_id=run_id, status="completed", current_step="done", completed=True)
                conn.commit()

            stderr = io.StringIO()
            with (
                mock.patch(
                    "brandpipe.pipeline.validate_candidate",
                    return_value=[
                        CandidateResult(
                            check_name="web",
                            status=ResultStatus.PASS,
                            score_delta=0.0,
                            reason="web_clear",
                            details={"provider": "serper"},
                        )
                    ],
                ),
                mock.patch("sys.stderr", new=stderr),
            ):
                summary = recheck_pending_web(
                    db_path=db_path,
                    run_id=run_id,
                    browser_profile_dir=browser_profile_dir,
                )

            self.assertEqual(summary["retried"], 1)
            warnings = stderr.getvalue()
            self.assertIn(f"run:{run_id}:config_json: invalid_json", warnings)
            self.assertIn(f"run:{run_id}:metrics_json: invalid_json", warnings)

            with db.open_db(db_path) as conn:
                row = db.get_run(conn, run_id=run_id)
                self.assertIsNotNone(row)
                assert row is not None
                metrics = json.loads(str(row["metrics_json"]))
                self.assertEqual(metrics["counts"]["validation_results"], 4)
                self.assertEqual(metrics["export_path"], "")

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
                    family_quotas = {{ smooth_blend = 1 }}

                    [validation]
                    checks = "tm"
                    tmview_profile_dir = "{root / 'tmview-profile'}"
                    tmview_chrome_executable = "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with mock.patch(
                "brandpipe.tmview.probe_names",
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
                    WHERE candidate_id = ? AND result_key = 'tm'
                    """,
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(result_row)
                assert result_row is not None
                self.assertEqual(result_row["status"], "warn")
                self.assertEqual(result_row["reason"], "tm_near_review")

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
                    family_quotas = {{ smooth_blend = 1 }}

                    [validation]
                    checks = "tm"
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
                    WHERE candidate_id = ? AND result_key = 'tm'
                    """,
                    (candidate_id,),
                ).fetchone()
                self.assertIsNotNone(result_row)
                assert result_row is not None
                self.assertEqual(result_row["status"], "unavailable")
                self.assertEqual(result_row["reason"], "tm_profile_missing")

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
                    family_quotas = {{ smooth_blend = 2 }}

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = self._run_pipeline_without_validation(config_path)

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
                    family_quotas = {{ smooth_blend = 2 }}

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            run_id = self._run_pipeline_without_validation(config_path)

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
                dropped = metrics["ideation"]["taste_filter"]["dropped"]
                self.assertTrue("banned_suffix_family" in dropped or "banned_morpheme" in dropped)

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
                    family_quotas = {{ smooth_blend = 1 }}

                    [validation]
                    checks = ""
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            ideation_report = {
                "provider": "fixture",
                "family_mix_profile": "family_default",
                "family_counts": {"smooth_blend": 1},
                "family_reports": {},
                "candidate_count": 1,
            }
            surfaced_candidates = [
                SurfacedCandidate(
                    display_name="vantora",
                    name_normalized="vantora",
                    family=NameFamily.SMOOTH_BLEND,
                    surface_policy=SurfacePolicy.ALPHA_LOWER,
                )
            ]

            with (
                mock.patch(
                    "brandpipe.pipeline.generate_candidate_surfaces",
                    return_value=(surfaced_candidates, ideation_report),
                ) as generate_surfaces_mock,
                mock.patch("brandpipe.pipeline.run_validation_jobs", return_value=None),
            ):
                run_pipeline(config_path)

            generate_surfaces_mock.assert_called_once()
            avoidance_context = generate_surfaces_mock.call_args.kwargs.get("avoidance_context")
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
            success_context = generate_surfaces_mock.call_args.kwargs.get("success_context")
            self.assertIsInstance(success_context, dict)
            assert isinstance(success_context, dict)
            self.assertIn("names", success_context)
            self.assertIn("endings", success_context)


if __name__ == "__main__":
    unittest.main()
