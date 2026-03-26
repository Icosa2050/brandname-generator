from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("run_brandpipe_attack.py")
MODULE_SPEC = importlib.util.spec_from_file_location("run_brandpipe_attack", MODULE_PATH)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"unable to load module spec from {MODULE_PATH}")
run_brandpipe_attack = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(run_brandpipe_attack)


class RunBrandpipeAttackIsolationTest(unittest.TestCase):
    def test_build_isolated_batch_run_configs_uses_lane_state_paths(self) -> None:
        run_out_dir = Path("/tmp/brandpipe-attack-isolated")
        _, run_configs = run_brandpipe_attack._build_isolated_batch_run_configs(
            lane="balanced",
            config_path=run_brandpipe_attack.LANE_CONFIGS["balanced"],
            briefs_file=run_brandpipe_attack.ROOT_DIR / "resources/brandpipe/example_batch_briefs.toml",
            run_out_dir=run_out_dir,
        )

        self.assertTrue(run_configs)
        expected_db_path = run_out_dir / "lane_state" / "balanced" / "brandpipe.db"
        expected_out_csv = run_out_dir / "lane_state" / "balanced" / "finalists_{run_id}.csv"

        for run_config in run_configs:
            self.assertEqual(run_config.db_path, expected_db_path)
            self.assertEqual(run_config.export.out_csv, expected_out_csv)

    def test_isolated_paths_differ_between_lanes(self) -> None:
        run_out_dir = Path("/tmp/brandpipe-attack-isolated")
        balanced_db = run_brandpipe_attack._lane_db_path(run_out_dir=run_out_dir, lane="balanced")
        crossmarket_db = run_brandpipe_attack._lane_db_path(run_out_dir=run_out_dir, lane="crossmarket")

        self.assertNotEqual(balanced_db, crossmarket_db)
        self.assertEqual(balanced_db.parent.name, "balanced")
        self.assertEqual(crossmarket_db.parent.name, "crossmarket")


if __name__ == "__main__":
    unittest.main()
