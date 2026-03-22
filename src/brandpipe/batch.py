from __future__ import annotations

import json
import re
import tomllib
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from .models import Brief, RunConfig
from .pipeline import _cfg_str, _list_of_strings, load_config, run_loaded_config


def _timestamp_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("batch-%Y%m%dT%H%M%SZ")


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return cleaned.strip("-") or "brief"


def _brief_from_payload(payload: dict[str, object]) -> Brief:
    return Brief(
        product_core=_cfg_str(payload.get("product_core"), ""),
        target_users=_list_of_strings(payload.get("target_users")),
        trust_signals=_list_of_strings(payload.get("trust_signals")),
        forbidden_directions=_list_of_strings(payload.get("forbidden_directions")),
        language_market=_cfg_str(payload.get("language_market"), ""),
        notes=_cfg_str(payload.get("notes"), ""),
    )


def load_batch_briefs(path: Path) -> list[dict[str, object]]:
    suffix = path.suffix.lower()
    if suffix == ".toml":
        with path.open("rb") as handle:
            payload = tomllib.load(handle)
        raw_briefs = payload.get("briefs")
        if not isinstance(raw_briefs, list):
            raise ValueError("batch briefs TOML must contain [[briefs]] entries")
        return [item for item in raw_briefs if isinstance(item, dict)]
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = payload.get("briefs")
        if not isinstance(payload, list):
            raise ValueError("batch briefs JSON must be a list or contain a 'briefs' list")
        return [item for item in payload if isinstance(item, dict)]
    if suffix == ".jsonl":
        entries: list[dict[str, object]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                entries.append(payload)
        return entries
    raise ValueError(f"unsupported briefs file format: {path.suffix}")


def _title_for_entry(base_title: str, payload: dict[str, object], index: int) -> str:
    label = _cfg_str(payload.get("title"), "") or _cfg_str(payload.get("slug"), "")
    if not label:
        label = _cfg_str(payload.get("product_core"), f"brief-{index + 1}")
    return f"{base_title}:{_slugify(label)}"


def build_batch_run_configs(*, template_config_path: Path, briefs_file_path: Path) -> tuple[str, list[RunConfig]]:
    base_config = load_config(template_config_path)
    entries = load_batch_briefs(briefs_file_path)
    configs: list[RunConfig] = []
    for index, payload in enumerate(entries):
        brief = _brief_from_payload(payload)
        configs.append(
            replace(
                base_config,
                title=_title_for_entry(base_config.title, payload, index),
                brief=brief,
            )
        )
    return base_config.title, configs


def run_batch(
    *,
    template_config_path: Path,
    briefs_file_path: Path,
    batch_id: str = "",
    stop_on_error: bool = False,
) -> dict[str, object]:
    resolved_batch_id = batch_id.strip() or _timestamp_batch_id()
    base_title, run_configs = build_batch_run_configs(
        template_config_path=template_config_path,
        briefs_file_path=briefs_file_path,
    )
    succeeded = 0
    failed = 0
    run_ids: list[int] = []
    failures: list[dict[str, object]] = []
    for index, run_config in enumerate(run_configs):
        try:
            run_id = run_loaded_config(
                run_config,
                config_path=template_config_path,
                batch_id=resolved_batch_id,
                batch_index=index,
            )
            run_ids.append(run_id)
            succeeded += 1
        except Exception as exc:
            failed += 1
            failures.append(
                {
                    "index": index,
                    "title": run_config.title,
                    "error_class": exc.__class__.__name__,
                    "error_message": str(exc),
                }
            )
            if stop_on_error:
                break
    return {
        "batch_id": resolved_batch_id,
        "title": base_title,
        "requested": len(run_configs),
        "attempted": succeeded + failed,
        "succeeded": succeeded,
        "failed": failed,
        "stopped_early": bool(stop_on_error and failed > 0 and (succeeded + failed) < len(run_configs)),
        "run_ids": run_ids,
        "failures": failures,
    }
