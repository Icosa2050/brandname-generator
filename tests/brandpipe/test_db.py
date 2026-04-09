# ruff: noqa: E402
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe import db
from brandpipe.models import NameFamily, SurfacePolicy, SurfacedCandidate
from brandpipe.scoring import build_attractiveness_result


class DatabaseTests(unittest.TestCase):
    def test_init_and_create_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="test-run",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                )
                conn.commit()
                row = db.get_run(conn, run_id=run_id)
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["title"], "test-run")
                self.assertEqual(row["status"], "created")

    def test_candidates_are_unique_by_normalized_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="test-run",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["Vantora", " vantora ", "Certivo"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                conn.commit()
                rows = db.list_candidates(conn, run_id=run_id)
                self.assertEqual([row["name"] for row in rows], ["Certivo", "Vantora"])

    def test_candidate_surfaces_preserve_display_and_comparison_forms(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="surface-run",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture", "family_mix_profile": "family_default"}},
                )
                db.add_candidate_surfaces(
                    conn,
                    run_id=run_id,
                    candidates=[
                        SurfacedCandidate(
                            display_name="incident.io",
                            name_normalized="incidentio",
                            family=NameFamily.LITERAL_TLD_HACK,
                            surface_policy=SurfacePolicy.DOTTED_LOWER,
                        ),
                        SurfacedCandidate(
                            display_name="incident-io",
                            name_normalized="incidentio",
                            family=NameFamily.LITERAL_TLD_HACK,
                            surface_policy=SurfacePolicy.HYPHENATED_LOWER,
                        ),
                        SurfacedCandidate(
                            display_name="Royal TSX",
                            name_normalized="royaltsx",
                            family=NameFamily.BRUTALIST_UTILITY,
                            surface_policy=SurfacePolicy.TITLE_SPACED_ACRONYM,
                        ),
                    ],
                )
                conn.commit()
                rows = db.list_candidates(conn, run_id=run_id)
                self.assertEqual(
                    [str(row["display_name"]) for row in rows],
                    ["Royal TSX", "incident-io", "incident.io"],
                )
                normalized = {str(row["display_name"]): str(row["name_normalized"]) for row in rows}
                self.assertEqual(normalized["incident.io"], "incidentio")
                self.assertEqual(normalized["incident-io"], "incidentio")
                families = {str(row["display_name"]): str(row["family"]) for row in rows}
                self.assertEqual(families["Royal TSX"], NameFamily.BRUTALIST_UTILITY.value)

    def test_recent_positive_feedback_only_returns_clean_validated_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="validated-feedback",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["baltera", "beacona", "softalia"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                rows = {str(row["name"]): int(row["id"]) for row in db.list_candidates(conn, run_id=run_id)}
                db.upsert_rankings(
                    conn,
                    rows=[
                        (rows["baltera"], 120.0, 0, 0, 0, 0, "candidate"),
                        (rows["beacona"], 117.0, 0, 0, 0, 0, "candidate"),
                        (rows["softalia"], 107.0, 0, 0, 0, 1, "watch"),
                    ],
                )

                for result_key in ("domain", "package", "web"):
                    db.upsert_result(
                        conn,
                        candidate_id=rows["baltera"],
                        result_key=result_key,
                        status="pass",
                        score_delta=0.0,
                        reason=f"{result_key}_pass",
                        details={"provider": "test"},
                    )
                baltera_attractiveness = build_attractiveness_result("baltera")
                db.upsert_result(
                    conn,
                    candidate_id=rows["baltera"],
                    result_key=baltera_attractiveness.check_name,
                    status=baltera_attractiveness.status.value,
                    score_delta=baltera_attractiveness.score_delta,
                    reason=baltera_attractiveness.reason,
                    details=baltera_attractiveness.details,
                )

                for result_key in ("domain", "web"):
                    db.upsert_result(
                        conn,
                        candidate_id=rows["beacona"],
                        result_key=result_key,
                        status="pass",
                        score_delta=0.0,
                        reason=f"{result_key}_pass",
                        details={"provider": "test"},
                    )
                beacona_attractiveness = build_attractiveness_result("beacona")
                db.upsert_result(
                    conn,
                    candidate_id=rows["beacona"],
                    result_key=beacona_attractiveness.check_name,
                    status=beacona_attractiveness.status.value,
                    score_delta=beacona_attractiveness.score_delta,
                    reason=beacona_attractiveness.reason,
                    details=beacona_attractiveness.details,
                )

                db.upsert_result(
                    conn,
                    candidate_id=rows["softalia"],
                    result_key="web",
                    status="warn",
                    score_delta=-2.0,
                    reason="web_check_pending",
                    details={"provider": "serper"},
                )
                softalia_attractiveness = build_attractiveness_result("softalia")
                db.upsert_result(
                    conn,
                    candidate_id=rows["softalia"],
                    result_key=softalia_attractiveness.check_name,
                    status=softalia_attractiveness.status.value,
                    score_delta=softalia_attractiveness.score_delta,
                    reason=softalia_attractiveness.reason,
                    details=softalia_attractiveness.details,
                )

                db.set_run_state(
                    conn,
                    run_id=run_id,
                    status="completed",
                    current_step="done",
                    completed=True,
                )
                conn.commit()

                feedback = db.recent_positive_feedback(conn)
                self.assertEqual(feedback["names"], ["baltera"])

    def test_recent_external_fail_name_corpus_dedupes_recent_failed_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="failed-corpus",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["meridel", "sabiline"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                rows = {str(row["name"]): int(row["id"]) for row in db.list_candidates(conn, run_id=run_id)}
                db.upsert_result(
                    conn,
                    candidate_id=rows["meridel"],
                    result_key="web",
                    status="fail",
                    score_delta=-10.0,
                    reason="web_near_collision",
                    details={"provider": "test"},
                )
                db.upsert_result(
                    conn,
                    candidate_id=rows["meridel"],
                    result_key="social",
                    status="fail",
                    score_delta=-8.0,
                    reason="social_handle_crowded",
                    details={"provider": "test"},
                )
                db.upsert_result(
                    conn,
                    candidate_id=rows["sabiline"],
                    result_key="web",
                    status="fail",
                    score_delta=-10.0,
                    reason="web_near_collision",
                    details={"provider": "test"},
                )
                db.set_run_state(
                    conn,
                    run_id=run_id,
                    status="completed",
                    current_step="done",
                    completed=True,
                )
                conn.commit()

                corpus = db.recent_external_fail_name_corpus(conn)
                self.assertEqual([item["name"] for item in corpus], ["sabiline", "meridel"])
                self.assertTrue(all(item["decision"] == "external_fail" for item in corpus))

    def test_recent_avoidance_feedback_includes_external_fragment_hints(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="avoidance-feedback",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["samistra", "parclex", "tenvrik"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                rows = {str(row["name"]): int(row["id"]) for row in db.list_candidates(conn, run_id=run_id)}
                db.upsert_result(
                    conn,
                    candidate_id=rows["samistra"],
                    result_key="tmview",
                    status="fail",
                    score_delta=-25.0,
                    reason="tmview_near_collision",
                    details={},
                )
                db.upsert_result(
                    conn,
                    candidate_id=rows["parclex"],
                    result_key="web",
                    status="fail",
                    score_delta=-25.0,
                    reason="web_exact_collision",
                    details={},
                )
                db.upsert_result(
                    conn,
                    candidate_id=rows["tenvrik"],
                    result_key="social",
                    status="fail",
                    score_delta=-20.0,
                    reason="social_handle_crowded",
                    details={},
                )
                db.set_run_state(
                    conn,
                    run_id=run_id,
                    status="completed",
                    current_step="done",
                    completed=True,
                )
                conn.commit()

                feedback = db.recent_avoidance_feedback(conn)
                self.assertIn("external_fragment_hints", feedback)
                self.assertIn("external_lead_hints", feedback)
                self.assertIn("external_tail_hints", feedback)
                self.assertIn("external_reason_patterns", feedback)
                self.assertIn("external_avoid_names", feedback)
                self.assertIn("samis", feedback["external_fragment_hints"])
                self.assertIn("parcl", feedback["external_fragment_hints"])
                self.assertIn("tenvr", feedback["external_fragment_hints"])
                self.assertIn("stra", feedback["external_tail_hints"])
                self.assertIn("clex", feedback["external_tail_hints"])
                self.assertIn("vrik", feedback["external_tail_hints"])
                self.assertIn("tmview_near_collision", feedback["external_reason_patterns"])
                tmview_payload = feedback["external_reason_patterns"]["tmview_near_collision"]
                self.assertIn("samistra", tmview_payload["examples"])
                self.assertIn("samis", tmview_payload["lead_hints"])
                self.assertIn("stra", tmview_payload["tail_hints"])
                self.assertIn("samistra", feedback["external_avoid_names"])

    def test_candidate_result_ranking_and_run_query_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="helper-run",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="batch-a",
                    batch_index=2,
                )
                newer_run_id = db.create_run(
                    conn,
                    title="helper-run",
                    brief={"product_core": "y"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="batch-a",
                    batch_index=3,
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["Éclair!", "Baltera"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                db.add_candidate_surfaces(conn, run_id=run_id, candidates=[])
                conn.commit()

                candidate_rows = db.list_candidates(conn, run_id=run_id)
                ids = {str(row["name"]): int(row["id"]) for row in candidate_rows}
                self.assertEqual(db.list_candidates_by_ids(conn, candidate_ids=[]), [])
                by_ids = db.list_candidates_by_ids(
                    conn,
                    candidate_ids=[ids["Baltera"], ids["Éclair!"]],
                )
                self.assertEqual([int(row["id"]) for row in by_ids], sorted(ids.values()))
                eclair_row = db.get_candidate(conn, candidate_id=ids["Éclair!"])
                self.assertIsNotNone(eclair_row)
                assert eclair_row is not None
                self.assertEqual(str(eclair_row["name_normalized"]), "eclair")
                self.assertEqual(str(eclair_row["surface_key"]), "éclair!")
                self.assertIsNone(db.get_candidate(conn, candidate_id=999999))

                db.upsert_result(
                    conn,
                    candidate_id=ids["Éclair!"],
                    result_key="web",
                    status="pass",
                    score_delta=1.5,
                    reason="web_pass",
                    details={"provider": "test"},
                )
                db.upsert_result(
                    conn,
                    candidate_id=ids["Baltera"],
                    result_key="domain",
                    status="warn",
                    score_delta=-2.0,
                    reason="domain_taken",
                    details={"provider": "test"},
                )
                self.assertEqual(len(db.fetch_results_for_candidate(conn, candidate_id=ids["Éclair!"])), 1)
                self.assertEqual(len(db.fetch_results_for_run(conn, run_id=run_id)), 2)

                db.upsert_ranking(
                    conn,
                    candidate_id=ids["Éclair!"],
                    total_score=31.0,
                    family_score=5.0,
                    family_rank=1,
                    rank_position=2,
                    blocker_count=0,
                    unavailable_count=0,
                    unsupported_count=0,
                    warning_count=0,
                    decision="watch",
                )
                db.upsert_rankings(
                    conn,
                    rows=[
                        (ids["Baltera"], 44.0, 0, 0, 0, 1, "candidate"),
                    ],
                )
                with self.assertRaisesRegex(ValueError, "unsupported_ranking_row_shape:2"):
                    db.upsert_rankings(conn, rows=[(1, 2)])

                ranked_rows = db.fetch_ranked_rows(conn, run_id=run_id, limit=10)
                self.assertEqual([str(row["name"]) for row in ranked_rows], ["Éclair!", "Baltera"])
                self.assertEqual(db.count_ranked_rows(conn, run_id=run_id), 2)

                latest = db.find_latest_run_by_title(conn, title="helper-run")
                self.assertIsNotNone(latest)
                assert latest is not None
                self.assertEqual(int(latest["id"]), newer_run_id)
                self.assertEqual(
                    [int(row["id"]) for row in db.list_runs(conn, batch_id="batch-a", limit=10)],
                    [run_id, newer_run_id],
                )

                db.delete_results_for_run(conn, run_id=run_id)
                self.assertEqual(db.fetch_results_for_run(conn, run_id=run_id), [])
                self.assertEqual(db.count_ranked_rows(conn, run_id=run_id), 0)

    def test_validation_job_lifecycle_and_attempt_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_id = db.create_run(
                    conn,
                    title="validation-run",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                )
                db.add_candidates(
                    conn,
                    run_id=run_id,
                    names=["baltera", "meridel"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                candidate_rows = db.list_candidates(conn, run_id=run_id)
                candidate_ids = [int(row["id"]) for row in candidate_rows]
                db.ensure_validation_jobs(
                    conn,
                    run_id=run_id,
                    ordered_candidate_ids=list(reversed(candidate_ids)),
                    shortlist_fingerprint="fp-1",
                )
                db.ensure_validation_jobs(
                    conn,
                    run_id=run_id,
                    ordered_candidate_ids=list(reversed(candidate_ids)),
                    shortlist_fingerprint="fp-1",
                )
                jobs = db.list_validation_jobs(conn, run_id=run_id)
                self.assertEqual(len(jobs), 2)
                self.assertEqual([int(row["candidate_id"]) for row in jobs], list(reversed(candidate_ids)))
                self.assertEqual(db.count_validation_jobs(conn, run_id=run_id), {"pending": 2})

                first_job = db.claim_next_validation_job(conn, run_id=run_id, now="2026-04-08T10:00:00Z")
                self.assertIsNotNone(first_job)
                assert first_job is not None
                self.assertEqual(str(first_job["status"]), "running")
                db.update_validation_job(
                    conn,
                    job_id=int(first_job["id"]),
                    status="retry_wait",
                    resume_check="web",
                    attempt_count=1,
                    next_retry_at="2026-04-08T10:05:00Z",
                    last_error_kind="http",
                    last_error_message="retry later",
                )

                second_job = db.claim_next_validation_job(conn, run_id=run_id, now="2026-04-08T10:00:00Z")
                self.assertIsNotNone(second_job)
                assert second_job is not None
                db.update_validation_job(
                    conn,
                    job_id=int(second_job["id"]),
                    status="completed",
                    finished=True,
                )
                self.assertIsNone(db.claim_next_validation_job(conn, run_id=run_id, now="2026-04-08T10:01:00Z"))

                retried_job = db.claim_next_validation_job(conn, run_id=run_id, now="2026-04-08T10:05:00Z")
                self.assertIsNotNone(retried_job)
                assert retried_job is not None
                db.update_validation_job(
                    conn,
                    job_id=int(retried_job["id"]),
                    status="completed",
                    finished=True,
                )
                self.assertEqual(db.count_validation_jobs(conn, run_id=run_id), {"completed": 2})

                db.record_validation_attempt(
                    conn,
                    job_id=int(retried_job["id"]),
                    run_id=run_id,
                    candidate_id=int(retried_job["candidate_id"]),
                    check_name="web",
                    attempt_number=1,
                    status="retry_wait",
                    reason="http_429",
                    error_kind="rate_limited",
                    retryable=True,
                    http_status=429,
                    retry_after_s=2.5,
                    headers={"retry-after": "2.5"},
                    evidence={"provider": "test"},
                    details={"note": "slow down"},
                )
                attempts = db.fetch_validation_attempts(conn, job_id=int(retried_job["id"]))
                self.assertEqual(len(attempts), 1)
                self.assertEqual(int(attempts[0]["retryable"]), 1)
                self.assertEqual(int(attempts[0]["http_status"]), 429)
                self.assertIsNone(db.get_validation_job(conn, job_id=999999))
                with self.assertRaisesRegex(RuntimeError, "validation_job_not_found:999999"):
                    db.update_validation_job(conn, job_id=999999, status="failed")

    def test_recheck_queries_and_recent_corpora(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                run_a = db.create_run(
                    conn,
                    title="batch-a-run",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="batch-a",
                    batch_index=1,
                )
                run_b = db.create_run(
                    conn,
                    title="batch-b-run",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="batch-b",
                    batch_index=1,
                )
                db.add_candidates(
                    conn,
                    run_id=run_a,
                    names=["alphaweb", "betawarn", "gammatm", "deltatm"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                db.add_candidates(
                    conn,
                    run_id=run_b,
                    names=["omegafail"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                rows_a = {str(row["name"]): int(row["id"]) for row in db.list_candidates(conn, run_id=run_a)}
                rows_b = {str(row["name"]): int(row["id"]) for row in db.list_candidates(conn, run_id=run_b)}
                db.upsert_rankings(
                    conn,
                    rows=[
                        (rows_a["alphaweb"], 10.0, 0, 0, 0, 0, "blocked"),
                        (rows_a["betawarn"], 15.0, 0, 0, 0, 1, "watch"),
                        (rows_a["gammatm"], 25.0, 0, 0, 0, 0, "candidate"),
                        (rows_a["deltatm"], 12.0, 0, 0, 0, 1, "watch"),
                        (rows_b["omegafail"], 20.0, 0, 0, 0, 0, "candidate"),
                    ],
                )
                db.upsert_result(
                    conn,
                    candidate_id=rows_a["alphaweb"],
                    result_key="web",
                    status="unavailable",
                    score_delta=0.0,
                    reason="web_search_unavailable",
                    details={},
                )
                db.upsert_result(
                    conn,
                    candidate_id=rows_a["betawarn"],
                    result_key="web",
                    status="warn",
                    score_delta=-2.0,
                    reason="web_check_pending",
                    details={},
                )
                db.upsert_result(
                    conn,
                    candidate_id=rows_a["deltatm"],
                    result_key="tmview",
                    status="fail",
                    score_delta=-10.0,
                    reason="tmview_near_collision",
                    details={},
                )
                db.upsert_result(
                    conn,
                    candidate_id=rows_b["omegafail"],
                    result_key="web",
                    status="fail",
                    score_delta=-10.0,
                    reason="web_near_collision",
                    details={},
                )
                db.set_run_state(conn, run_id=run_a, status="completed", current_step="done", completed=True)
                db.set_run_state(conn, run_id=run_b, status="completed", current_step="done", completed=True)
                conn.commit()

                pending_rows = db.fetch_pending_web_rows(conn, run_id=run_a, limit=10)
                self.assertEqual([str(row["name"]) for row in pending_rows], ["alphaweb", "betawarn"])
                self.assertEqual(
                    [str(row["name"]) for row in db.fetch_pending_web_rows(conn, batch_id="batch-a", limit=10)],
                    ["alphaweb", "betawarn"],
                )

                tmview_rows = db.fetch_tmview_recheck_rows(conn, run_id=run_a, limit=10)
                self.assertEqual([str(row["name"]) for row in tmview_rows], ["gammatm", "betawarn"])
                forced_tmview_rows = db.fetch_tmview_recheck_rows(conn, run_id=run_a, force=True, limit=10)
                self.assertEqual(
                    [str(row["name"]) for row in forced_tmview_rows],
                    ["gammatm", "betawarn", "deltatm"],
                )

                ranked_corpus = db.recent_ranked_name_corpus(conn, exclude_batch_id="batch-a", limit=10)
                self.assertEqual([item["name"] for item in ranked_corpus], ["omegafail"])
                self.assertEqual(ranked_corpus[0]["decision"], "candidate")

                external_fail_corpus = db.recent_external_fail_name_corpus(conn, exclude_batch_id="batch-a", limit=10)
                self.assertEqual([item["name"] for item in external_fail_corpus], ["omegafail"])
                self.assertEqual(external_fail_corpus[0]["reason"], "web_near_collision")

    def test_feedback_and_pattern_helpers_cover_recent_aggregates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "brandpipe.db"
            with db.open_db(db_path) as conn:
                db.ensure_schema(conn)
                self.assertEqual(
                    db.recent_blocked_patterns(conn),
                    {"run_ids": [], "blocked_names": [], "suffixes": [], "stems": []},
                )
                self.assertEqual(db._normalize_pattern_name(" Parcl-ex! "), "parclex")
                self.assertEqual(db._terminal_skeleton("adara"), "dr")

                positive_run = db.create_run(
                    conn,
                    title="positive-include",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="include",
                    batch_index=1,
                )
                excluded_positive_run = db.create_run(
                    conn,
                    title="positive-exclude",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="exclude-me",
                    batch_index=1,
                )
                avoidance_run = db.create_run(
                    conn,
                    title="avoidance-rich",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="include",
                    batch_index=2,
                )
                blocked_run = db.create_run(
                    conn,
                    title="blocked-patterns",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="include",
                    batch_index=3,
                )
                invalid_metrics_run = db.create_run(
                    conn,
                    title="invalid-metrics",
                    brief={"product_core": "x"},
                    config={"ideation": {"provider": "fixture"}},
                    batch_id="include",
                    batch_index=4,
                )

                db.add_candidates(
                    conn,
                    run_id=positive_run,
                    names=["baltera"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                db.add_candidates(
                    conn,
                    run_id=excluded_positive_run,
                    names=["meridel"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                db.add_candidates(
                    conn,
                    run_id=avoidance_run,
                    names=["baldara", "meldara", "baldari"],
                    source_kind="fixture",
                    source_detail="fixture",
                )
                db.add_candidates(
                    conn,
                    run_id=blocked_run,
                    names=["baldara", "baldari", "geldara"],
                    source_kind="fixture",
                    source_detail="fixture",
                )

                positive_rows = {str(row["name"]): int(row["id"]) for row in db.list_candidates(conn, run_id=positive_run)}
                excluded_rows = {
                    str(row["name"]): int(row["id"])
                    for row in db.list_candidates(conn, run_id=excluded_positive_run)
                }
                avoidance_rows = {
                    str(row["name"]): int(row["id"])
                    for row in db.list_candidates(conn, run_id=avoidance_run)
                }
                blocked_rows = {str(row["name"]): int(row["id"]) for row in db.list_candidates(conn, run_id=blocked_run)}

                for run_id, candidate_id, name in (
                    (positive_run, positive_rows["baltera"], "baltera"),
                    (excluded_positive_run, excluded_rows["meridel"], "meridel"),
                ):
                    db.upsert_rankings(conn, rows=[(candidate_id, 90.0, 0, 0, 0, 0, "candidate")])
                    for result_key in ("domain", "package", "web"):
                        db.upsert_result(
                            conn,
                            candidate_id=candidate_id,
                            result_key=result_key,
                            status="pass",
                            score_delta=0.0,
                            reason=f"{result_key}_pass",
                            details={},
                        )
                    attractiveness = build_attractiveness_result(name)
                    db.upsert_result(
                        conn,
                        candidate_id=candidate_id,
                        result_key=attractiveness.check_name,
                        status=attractiveness.status.value,
                        score_delta=attractiveness.score_delta,
                        reason=attractiveness.reason,
                        details=attractiveness.details,
                    )
                    db.set_run_state(conn, run_id=run_id, status="completed", current_step="done", completed=True)

                db.upsert_rankings(
                    conn,
                    rows=[
                        (blocked_rows["baldara"], 10.0, 0, 0, 0, 0, "blocked"),
                        (blocked_rows["baldari"], 9.0, 0, 0, 0, 0, "blocked"),
                        (blocked_rows["geldara"], 8.0, 0, 0, 0, 0, "blocked"),
                    ],
                )
                db.set_run_state(conn, run_id=blocked_run, status="completed", current_step="done", completed=True)

                db.update_run_metrics(
                    conn,
                    run_id=avoidance_run,
                    metrics={
                        "ideation": {
                            "local_filter": {
                                "dropped_examples": {
                                    "prefix_collision": [
                                        "baltor:baltoria",
                                        "baltor:baltorix",
                                    ],
                                    "suffix_collision": [
                                        "baltor:baltoria",
                                    ],
                                }
                            }
                        }
                    },
                )
                db.upsert_result(
                    conn,
                    candidate_id=avoidance_rows["baldara"],
                    result_key="tmview",
                    status="fail",
                    score_delta=-10.0,
                    reason="tmview_near_collision",
                    details={},
                )
                db.upsert_result(
                    conn,
                    candidate_id=avoidance_rows["meldara"],
                    result_key="web",
                    status="fail",
                    score_delta=-10.0,
                    reason="web_near_collision",
                    details={},
                )
                db.upsert_result(
                    conn,
                    candidate_id=avoidance_rows["baldari"],
                    result_key="social",
                    status="fail",
                    score_delta=-10.0,
                    reason="social_handle_crowded",
                    details={},
                )
                db.set_run_state(conn, run_id=avoidance_run, status="completed", current_step="done", completed=True)
                conn.execute(
                    "UPDATE runs SET metrics_json = ? WHERE id = ?",
                    ("{", invalid_metrics_run),
                )
                db.set_run_state(conn, run_id=invalid_metrics_run, status="completed", current_step="done", completed=True)
                conn.commit()

                positive_feedback = db.recent_positive_feedback(conn, exclude_batch_id="exclude-me")
                self.assertEqual(positive_feedback["names"], ["baltera"])
                self.assertEqual(positive_feedback["endings"], ["era"])

                avoidance_feedback = db.recent_avoidance_feedback(conn)
                self.assertEqual(avoidance_feedback["local_examples"][0]["example"], "baltor:baltoria")
                self.assertIn("balt", avoidance_feedback["local_patterns"]["prefixes"])
                self.assertIn("tor", avoidance_feedback["local_patterns"]["suffixes"])
                self.assertIn("bal", avoidance_feedback["external_patterns"]["prefixes"])
                self.assertIn("ara", avoidance_feedback["external_patterns"]["suffixes"])
                self.assertIn("ra", avoidance_feedback["external_terminal_families"])
                self.assertIn("dr", avoidance_feedback["external_terminal_skeletons"])
                self.assertIn("balda", avoidance_feedback["external_lead_hints"])
                self.assertIn("dara", avoidance_feedback["external_tail_hints"])
                self.assertIn("baldara", avoidance_feedback["external_avoid_names"])
                self.assertIn("tmview_near_collision", avoidance_feedback["external_reason_patterns"])

                blocked_patterns = db.recent_blocked_patterns(conn, min_occurrences=2)
                self.assertEqual(blocked_patterns["suffixes"], ["dara"])
                self.assertEqual(blocked_patterns["stems"], ["balda"])


if __name__ == "__main__":
    unittest.main()
