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
                    config={"ideation": {"provider": "fixture", "family_mix_profile": "surface_diverse_v1"}},
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
                    details={"provider": "browser_google"},
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


if __name__ == "__main__":
    unittest.main()
