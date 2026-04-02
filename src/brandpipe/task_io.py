from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
from typing import Iterable


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_ROOT = ROOT_DIR / "test_outputs" / "brandpipe"
STATE_DB_NAME = "brandpipe.db"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _invocation_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def slugify_label(raw: object, *, default: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(raw or "").strip().lower()).strip("-")
    return text or default


@dataclass(frozen=True)
class TaskPaths:
    task: str
    label: str
    invocation_id: str
    label_root: Path
    root: Path
    manifest_path: Path
    inputs_dir: Path
    logs_dir: Path
    state_dir: Path
    exports_dir: Path
    profiles_dir: Path

    @property
    def db_path(self) -> Path:
        return self.state_dir / STATE_DB_NAME


def prepare_task_paths(
    *,
    task: str,
    label: str,
    out_dir: str | Path | None = None,
) -> TaskPaths:
    task_name = slugify_label(task, default="task")
    label_name = slugify_label(label, default="manual")
    label_root = (
        Path(out_dir).expanduser().resolve()
        if out_dir is not None and str(out_dir).strip()
        else (DEFAULT_OUTPUT_ROOT / task_name / label_name).resolve()
    )
    invocation_id = _invocation_id()
    root = label_root / invocation_id
    inputs_dir = root / "inputs"
    logs_dir = root / "logs"
    state_dir = root / "state"
    exports_dir = root / "exports"
    profiles_dir = root / "profiles"
    for path in (inputs_dir, logs_dir, state_dir, exports_dir, profiles_dir):
        path.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "manifest.json"
    return TaskPaths(
        task=task_name,
        label=label_name,
        invocation_id=invocation_id,
        label_root=label_root,
        root=root,
        manifest_path=manifest_path,
        inputs_dir=inputs_dir,
        logs_dir=logs_dir,
        state_dir=state_dir,
        exports_dir=exports_dir,
        profiles_dir=profiles_dir,
    )


def copy_inputs(
    task_paths: TaskPaths,
    *,
    files: Iterable[Path] = (),
    text_blobs: dict[str, str] | None = None,
) -> list[str]:
    copied: list[str] = []
    for path in files:
        source = Path(path).expanduser().resolve()
        if not source.exists():
            continue
        destination = task_paths.inputs_dir / source.name
        shutil.copy2(source, destination)
        copied.append(str(destination))
    for name, content in (text_blobs or {}).items():
        destination = task_paths.inputs_dir / str(name).strip()
        destination.write_text(str(content), encoding="utf-8")
        copied.append(str(destination))
    return copied


def load_manifest(task_paths: TaskPaths) -> dict[str, object]:
    if not task_paths.manifest_path.exists():
        return {}
    try:
        payload = json.loads(task_paths.manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def write_manifest(task_paths: TaskPaths, payload: dict[str, object]) -> None:
    task_paths.manifest_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def init_manifest(
    task_paths: TaskPaths,
    *,
    config_paths: Iterable[str] = (),
    db_path: str | None = None,
    export_paths: Iterable[str] = (),
    child_runs: Iterable[object] = (),
    metrics_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "task": task_paths.task,
        "label": task_paths.label,
        "invocation_id": task_paths.invocation_id,
        "status": "running",
        "started_at": _utc_now(),
        "completed_at": "",
        "config_paths": list(config_paths),
        "db_path": str(task_paths.db_path if db_path is None else db_path),
        "export_paths": list(export_paths),
        "child_runs": list(child_runs),
        "metrics_summary": metrics_summary or {},
    }
    write_manifest(task_paths, payload)
    return payload


def finalize_manifest(
    task_paths: TaskPaths,
    *,
    status: str,
    config_paths: Iterable[str] | None = None,
    db_path: str | None = None,
    export_paths: Iterable[str] | None = None,
    child_runs: Iterable[object] | None = None,
    metrics_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = load_manifest(task_paths)
    payload.update(
        {
            "task": payload.get("task") or task_paths.task,
            "label": payload.get("label") or task_paths.label,
            "invocation_id": payload.get("invocation_id") or task_paths.invocation_id,
            "status": str(status).strip() or "completed",
            "completed_at": _utc_now(),
            "config_paths": list(config_paths) if config_paths is not None else list(payload.get("config_paths") or []),
            "db_path": str(payload.get("db_path") if db_path is None else db_path),
            "export_paths": list(export_paths) if export_paths is not None else list(payload.get("export_paths") or []),
            "child_runs": list(child_runs) if child_runs is not None else list(payload.get("child_runs") or []),
            "metrics_summary": metrics_summary if metrics_summary is not None else dict(payload.get("metrics_summary") or {}),
        }
    )
    if not payload.get("started_at"):
        payload["started_at"] = _utc_now()
    write_manifest(task_paths, payload)
    return payload
