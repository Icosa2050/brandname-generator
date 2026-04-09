# ruff: noqa: E402
from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.models import Brief, ExportConfig, IdeationConfig, RunConfig, ValidationConfig
from brandpipe.run_cli import run_config_command
from brandpipe.task_io import prepare_task_paths


def _loaded_config(db_path: Path) -> RunConfig:
    return RunConfig(
        db_path=db_path,
        title="fixture-run",
        brief=Brief(product_core="utility-cost settlement software"),
        ideation=IdeationConfig(provider="fixture"),
        validation=ValidationConfig(),
        export=ExportConfig(),
    )


class RunCliTests(unittest.TestCase):
    def test_run_config_command_writes_completed_manifest_with_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            config_path = root / "fixture.toml"
            config_path.write_text("[run]\ntitle='fixture-run'\n", encoding="utf-8")
            task_paths = prepare_task_paths(task="run", label="fixture", out_dir=root / "outputs")
            stdout = io.StringIO()

            with (
                contextlib.redirect_stdout(stdout),
                mock.patch("brandpipe.run_cli.load_config", return_value=_loaded_config(root / "ignored.db")),
                mock.patch("brandpipe.run_cli.prepare_task_paths", return_value=task_paths),
                mock.patch("brandpipe.run_cli.run_loaded_config", return_value=17) as run_mock,
                mock.patch(
                    "brandpipe.run_cli.db.get_run",
                    return_value={
                        "metrics_json": json.dumps(
                            {
                                "counts": {"ranked_candidates": 3},
                                "decision_counts": {"candidate": 2, "watch": 1},
                            }
                        )
                    },
                ),
            ):
                exit_code = run_config_command(config_path)

            self.assertEqual(exit_code, 17)
            runtime_config = (
                run_mock.call_args.kwargs["runtime_config"]
                if "runtime_config" in run_mock.call_args.kwargs
                else run_mock.call_args.args[0]
            )
            self.assertEqual(runtime_config.db_path, task_paths.db_path)
            self.assertEqual(runtime_config.export.out_csv, task_paths.exports_dir / "finalists_{run_id}.csv")
            self.assertTrue((task_paths.inputs_dir / "fixture.toml").exists())

            manifest = json.loads(task_paths.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(manifest["child_runs"], [{"run_id": 17}])
            self.assertEqual(manifest["metrics_summary"]["title"], "fixture-run")
            self.assertEqual(manifest["metrics_summary"]["run_id"], 17)
            self.assertEqual(manifest["metrics_summary"]["counts"], {"ranked_candidates": 3})
            self.assertEqual(manifest["metrics_summary"]["decision_counts"], {"candidate": 2, "watch": 1})
            self.assertIn(f"task_root={task_paths.root}", stdout.getvalue())
            self.assertIn(f"manifest={task_paths.manifest_path}", stdout.getvalue())

    def test_run_config_command_tolerates_invalid_and_non_dict_metrics(self) -> None:
        scenarios = [
            ("invalid-json", "{not-json"),
            ("non-dict-sections", json.dumps({"counts": [], "decision_counts": 1})),
        ]

        for label, raw_metrics in scenarios:
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    root = Path(tmp_dir)
                    config_path = root / "fixture.toml"
                    config_path.write_text("[run]\ntitle='fixture-run'\n", encoding="utf-8")
                    task_paths = prepare_task_paths(task="run", label=label, out_dir=root / "outputs")

                    with (
                        mock.patch("brandpipe.run_cli.load_config", return_value=_loaded_config(root / "ignored.db")),
                        mock.patch("brandpipe.run_cli.prepare_task_paths", return_value=task_paths),
                        mock.patch("brandpipe.run_cli.run_loaded_config", return_value=23),
                        mock.patch("brandpipe.run_cli.db.get_run", return_value={"metrics_json": raw_metrics}),
                    ):
                        exit_code = run_config_command(config_path)

                    self.assertEqual(exit_code, 23)
                    manifest = json.loads(task_paths.manifest_path.read_text(encoding="utf-8"))
                    self.assertEqual(manifest["status"], "completed")
                    self.assertEqual(manifest["metrics_summary"], {"title": "fixture-run", "run_id": 23})

    def test_run_config_command_finalizes_failed_manifest_before_reraising(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            config_path = root / "fixture.toml"
            config_path.write_text("[run]\ntitle='fixture-run'\n", encoding="utf-8")
            task_paths = prepare_task_paths(task="run", label="failing", out_dir=root / "outputs")

            with (
                mock.patch("brandpipe.run_cli.load_config", return_value=_loaded_config(root / "ignored.db")),
                mock.patch("brandpipe.run_cli.prepare_task_paths", return_value=task_paths),
                mock.patch("brandpipe.run_cli.run_loaded_config", side_effect=RuntimeError("boom")),
            ):
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    run_config_command(config_path)

            manifest = json.loads(task_paths.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["db_path"], str(task_paths.db_path))
            self.assertEqual(manifest["metrics_summary"], {"title": "fixture-run"})
            self.assertEqual(manifest["config_paths"], [str(task_paths.inputs_dir / "fixture.toml")])


if __name__ == "__main__":
    unittest.main()
