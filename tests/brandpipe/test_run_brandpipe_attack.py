from __future__ import annotations

import argparse
import importlib.util
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

SCRIPT_PATH = ROOT_DIR / "scripts/branding/run_brandpipe_attack.py"
SPEC = importlib.util.spec_from_file_location("run_brandpipe_attack", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RunBrandpipeAttackTests(unittest.TestCase):
    def test_auto_lane_cap_scales_with_top_n_and_lane_count(self) -> None:
        self.assertEqual(MODULE._auto_lane_cap(top_n=80, lane_count=7), 18)
        self.assertEqual(MODULE._auto_lane_cap(top_n=80, lane_count=6), 20)
        self.assertEqual(MODULE._auto_lane_cap(top_n=0, lane_count=6), 0)

    def test_merge_rows_caps_lane_share_but_backfills_to_top_n(self) -> None:
        rows = [
            {"lane": "angular", "decision": "candidate", "name": "anglex", "total_score": 120.0, "blocker_count": 0, "unavailable_count": 0, "unsupported_count": 0, "warning_count": 0},
            {"lane": "angular", "decision": "candidate", "name": "anglor", "total_score": 119.0, "blocker_count": 0, "unavailable_count": 0, "unsupported_count": 0, "warning_count": 0},
            {"lane": "angular", "decision": "candidate", "name": "anglix", "total_score": 118.0, "blocker_count": 0, "unavailable_count": 0, "unsupported_count": 0, "warning_count": 0},
            {"lane": "expressive", "decision": "candidate", "name": "brinolas", "total_score": 117.0, "blocker_count": 0, "unavailable_count": 0, "unsupported_count": 0, "warning_count": 0},
            {"lane": "plosive", "decision": "candidate", "name": "tydrac", "total_score": 116.0, "blocker_count": 0, "unavailable_count": 0, "unsupported_count": 0, "warning_count": 0},
        ]

        merged = MODULE._merge_rows(rows, top_n=4, lane_cap=2)

        self.assertEqual([row["name"] for row in merged], ["anglex", "anglor", "brinolas", "tydrac"])
        self.assertEqual(sum(1 for row in merged if row["lane"] == "angular"), 2)

    def test_run_step_progress_tracks_validation_and_completion(self) -> None:
        self.assertEqual(
            MODULE._run_step_progress(
                current_step="validation",
                candidates=10,
                validation_results=20,
                expected_validation_results=40,
                rankings=0,
            ),
            0.5,
        )
        self.assertEqual(
            MODULE._run_step_progress(
                current_step="complete",
                candidates=10,
                validation_results=40,
                expected_validation_results=40,
                rankings=10,
            ),
            1.0,
        )

    def test_format_progress_line_shows_validation_counts(self) -> None:
        line = MODULE._format_progress_line(
            lane="plosive",
            snapshot={
                "requested": 3,
                "effective_requested": 3,
                "succeeded": 1,
                "failed": 0,
                "overall_progress": 0.42,
                "current": {
                    "title": "openrouter-attack-plosive:utility-settlement",
                    "current_step": "validation",
                    "candidates": 51,
                    "validation_results": 40,
                    "expected_validation_results": 204,
                    "rankings": 0,
                },
            },
        )

        self.assertIn("lane=plosive", line)
        self.assertIn("current=utility-settlement", line)
        self.assertIn("validation=40/204", line)

    def test_format_progress_line_uses_effective_requested_after_early_stop(self) -> None:
        line = MODULE._format_progress_line(
            lane="expressive",
            snapshot={
                "requested": 3,
                "effective_requested": 2,
                "succeeded": 1,
                "failed": 1,
                "overall_progress": 1.0,
                "current": {
                    "title": "openrouter-attack-expressive:tenant-ledger",
                    "current_step": "failed",
                    "candidates": 0,
                    "validation_results": 0,
                    "expected_validation_results": 0,
                    "rankings": 0,
                },
            },
        )

        self.assertIn("done=1/2 failed=1", line)

    def test_effective_requested_count_uses_attempted_when_stop_on_error_finishes_early(self) -> None:
        self.assertEqual(
            MODULE._effective_requested_count(
                requested=3,
                succeeded=1,
                failed=1,
                current_status="failed",
                stop_on_error=True,
            ),
            2,
        )
        self.assertEqual(
            MODULE._effective_requested_count(
                requested=3,
                succeeded=1,
                failed=1,
                current_status="failed",
                stop_on_error=False,
            ),
            3,
        )

    def test_dry_run_does_not_create_output_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir) / "attack_runs"
            args = argparse.Namespace(
                briefs_file=str(ROOT_DIR / "resources/brandpipe/example_batch_briefs.toml"),
                lanes="all",
                top_n=80,
                lane_cap=-1,
                out_dir=str(out_dir),
                progress_poll_s=5.0,
                progress_log="",
                progress_json="",
                dry_run=True,
                stop_on_error=False,
                lane_workers=1,
            )

            with mock.patch.object(MODULE, "parse_args", return_value=args):
                result = MODULE.main()

            self.assertEqual(result, 0)
            self.assertFalse(out_dir.exists())

    def test_main_can_run_multiple_lanes_concurrently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir) / "attack_runs"
            args = argparse.Namespace(
                briefs_file=str(ROOT_DIR / "resources/brandpipe/example_batch_briefs.toml"),
                lanes="expressive,plosive,angular",
                top_n=20,
                lane_cap=-1,
                out_dir=str(out_dir),
                progress_poll_s=0.1,
                progress_log="",
                progress_json="",
                dry_run=False,
                stop_on_error=False,
                lane_workers=2,
            )

            active = 0
            max_active = 0
            lock = threading.Lock()

            def fake_run_lane(**kwargs):
                nonlocal active, max_active
                lane = str(kwargs["lane"])
                with lock:
                    active += 1
                    max_active = max(max_active, active)
                try:
                    time.sleep(0.05)
                    return {
                        "lane": lane,
                        "batch_id": f"attack-test-{lane}",
                        "lane_rows": [],
                        "lane_summary": {
                            "lane": lane,
                            "batch_id": f"attack-test-{lane}",
                            "requested": 3,
                            "succeeded": 3,
                            "failed": 0,
                            "run_ids": [1, 2, 3],
                            "unique_survivors": 0,
                            "diversity": MODULE._summarize_diversity([]),
                        },
                    }
                finally:
                    with lock:
                        active -= 1

            with (
                mock.patch.object(MODULE, "parse_args", return_value=args),
                mock.patch.object(MODULE, "_run_lane", side_effect=fake_run_lane),
            ):
                result = MODULE.main()

            self.assertEqual(result, 0)
            self.assertGreaterEqual(max_active, 2)


if __name__ == "__main__":
    unittest.main()
