from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

from . import db
from .pipeline import load_config, run_loaded_config
from .task_io import copy_inputs, finalize_manifest, init_manifest, prepare_task_paths, slugify_label


def run_config_command(config_path: str | Path) -> int:
    resolved_config_path = Path(config_path).expanduser().resolve()
    loaded_config = load_config(resolved_config_path)
    task_paths = prepare_task_paths(
        task="run",
        label=slugify_label(resolved_config_path.stem, default="manual"),
    )
    copied_inputs = copy_inputs(task_paths, files=[resolved_config_path])
    runtime_config = replace(
        loaded_config,
        db_path=task_paths.db_path,
        export=replace(
            loaded_config.export,
            out_csv=task_paths.exports_dir / "finalists_{run_id}.csv",
        ),
    )
    init_manifest(
        task_paths,
        config_paths=copied_inputs,
        db_path=str(task_paths.db_path),
        export_paths=[],
        child_runs=[],
        metrics_summary={"title": runtime_config.title},
    )

    try:
        run_id = run_loaded_config(runtime_config, config_path=resolved_config_path)
        export_path = task_paths.exports_dir / f"finalists_{run_id}.csv"
        metrics_summary: dict[str, object] = {"title": runtime_config.title, "run_id": run_id}
        with db.open_db(task_paths.db_path) as conn:
            db.ensure_schema(conn)
            row = db.get_run(conn, run_id=run_id)
            if row is not None:
                try:
                    metrics = json.loads(str(row["metrics_json"] or "{}"))
                except json.JSONDecodeError:
                    metrics = {}
                if isinstance(metrics, dict):
                    counts = metrics.get("counts")
                    if isinstance(counts, dict):
                        metrics_summary["counts"] = counts
                    decision_counts = metrics.get("decision_counts")
                    if isinstance(decision_counts, dict):
                        metrics_summary["decision_counts"] = decision_counts
        finalize_manifest(
            task_paths,
            status="completed",
            config_paths=copied_inputs,
            db_path=str(task_paths.db_path),
            export_paths=[str(export_path)],
            child_runs=[{"run_id": run_id}],
            metrics_summary=metrics_summary,
        )
        print(f"task_root={task_paths.root}")
        print(f"manifest={task_paths.manifest_path}")
        return run_id
    except Exception:
        finalize_manifest(
            task_paths,
            status="failed",
            config_paths=copied_inputs,
            db_path=str(task_paths.db_path),
            metrics_summary={"title": runtime_config.title},
        )
        raise
