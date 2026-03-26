#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
import math
from pathlib import Path
import sys
import threading

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe import db
from brandpipe.batch import build_batch_run_configs, load_batch_briefs
from brandpipe.pipeline import load_config, run_loaded_config


LANE_CONFIGS = {
    "expressive": ROOT_DIR / "resources/brandpipe/openrouter_attack_expressive.toml",
    "plosive": ROOT_DIR / "resources/brandpipe/openrouter_attack_plosive.toml",
    "angular": ROOT_DIR / "resources/brandpipe/openrouter_attack_angular.toml",
    "balanced": ROOT_DIR / "resources/brandpipe/openrouter_attack_balanced.toml",
    "crossmarket": ROOT_DIR / "resources/brandpipe/openrouter_attack_crossmarket.toml",
    "short_recovery": ROOT_DIR / "resources/brandpipe/openrouter_attack_short_recovery.toml",
    "short": ROOT_DIR / "resources/brandpipe/openrouter_attack_short.toml",
}
ENDING_FAMILY_RULES: tuple[tuple[str, str], ...] = (
    ("aria", "aria"),
    ("eria", "eria"),
    ("ia", "ia"),
    ("ea", "ea"),
    ("en", "en"),
    ("er", "er"),
    ("el", "el"),
    ("et", "et"),
    ("is", "is"),
    ("il", "il"),
    ("in", "in"),
    ("ix", "ix"),
    ("ex", "ex"),
    ("um", "um"),
    ("an", "an"),
    ("ar", "ar"),
    ("a", "a"),
    ("e", "e"),
    ("i", "i"),
    ("n", "n"),
    ("r", "r"),
    ("l", "l"),
    ("s", "s"),
    ("x", "x"),
)
VOWELS = frozenset("aeiouy")
DECISION_ORDER = {"candidate": 0, "watch": 1, "blocked": 2}


def _timestamp_local() -> str:
    return datetime.now().astimezone().strftime("%H:%M:%S")


def _normalize_name(value: object) -> str:
    return "".join(ch for ch in str(value or "").strip().lower() if ch.isalpha())


def _ending_family(name: str) -> str:
    lowered = _normalize_name(name)
    for suffix, family in ENDING_FAMILY_RULES:
        if lowered.endswith(suffix):
            return family
    return lowered[-2:] if len(lowered) >= 2 else lowered


def _lead_skeleton(name: str) -> str:
    lowered = _normalize_name(name)[:6]
    consonants = "".join(ch for ch in lowered if ch not in VOWELS)
    return consonants[:3] or lowered[:3]


def _progress_bar(progress: float, *, width: int = 18) -> str:
    bounded = max(0.0, min(1.0, float(progress)))
    filled = int(round(width * bounded))
    return f"[{'#' * filled}{'.' * (width - filled)}]"


def _effective_requested_count(
    *,
    requested: int,
    succeeded: int,
    failed: int,
    current_status: str,
    stop_on_error: bool,
) -> int:
    requested_count = max(0, int(requested))
    attempted_count = max(0, int(succeeded) + int(failed))
    if requested_count <= 0:
        return 0
    if (
        stop_on_error
        and failed > 0
        and attempted_count > 0
        and attempted_count < requested_count
        and str(current_status or "").strip().lower() in {"failed", "complete", "completed"}
    ):
        return attempted_count
    return requested_count


def _rank_key(row: dict[str, object]) -> tuple[object, ...]:
    return (
        DECISION_ORDER.get(str(row.get("decision") or "").strip(), 99),
        int(row.get("blocker_count") or 0),
        int(row.get("unavailable_count") or 0),
        int(row.get("unsupported_count") or 0),
        int(row.get("warning_count") or 0),
        -float(row.get("total_score") or 0.0),
        str(row.get("name") or ""),
    )


def _dominance(counter: Counter[str], total: int) -> dict[str, object]:
    if not counter or total <= 0:
        return {"value": "", "count": 0, "share": 0.0}
    value, count = counter.most_common(1)[0]
    return {"value": value, "count": int(count), "share": round(count / total, 4)}


def _summarize_diversity(rows: list[dict[str, object]]) -> dict[str, object]:
    names = [_normalize_name(row.get("name")) for row in rows if _normalize_name(row.get("name"))]
    prefix3 = Counter(name[:3] for name in names if len(name) >= 3)
    lead_skeletons = Counter(_lead_skeleton(name) for name in names if name)
    ending_families = Counter(_ending_family(name) for name in names if name)
    total = len(names)
    return {
        "count": total,
        "unique_prefix3": len(prefix3),
        "unique_leading_skeletons": len(lead_skeletons),
        "unique_ending_families": len(ending_families),
        "top_prefix3": _dominance(prefix3, total),
        "top_leading_skeleton": _dominance(lead_skeletons, total),
        "top_ending_family": _dominance(ending_families, total),
    }


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "lane",
        "run_id",
        "title",
        "decision",
        "name",
        "total_score",
        "blocker_count",
        "unavailable_count",
        "unsupported_count",
        "warning_count",
        "batch_id",
        "export_path",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _write_markdown(path: Path, summary: dict[str, object]) -> None:
    lines = [
        "# Brandpipe Attack Summary",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- briefs_file: {summary['briefs_file']}",
        f"- lanes: {', '.join(summary['lanes'])}",
        f"- merged_unique_survivors: {summary['merged_unique_survivors']}",
        f"- merged_top_n: {summary['merged_top_n']}",
        f"- merged_lane_cap: {summary['merged_lane_cap']}",
        "",
        "## Diversity",
        "",
        f"- merged_all_unique_prefix3: {summary['merged_diversity']['unique_prefix3']}",
        f"- merged_all_unique_leading_skeletons: {summary['merged_diversity']['unique_leading_skeletons']}",
        f"- merged_all_unique_ending_families: {summary['merged_diversity']['unique_ending_families']}",
        f"- merged_all_top_prefix3_share: {summary['merged_diversity']['top_prefix3']['share']}",
        f"- merged_top_n_unique_prefix3: {summary['merged_top_n_diversity']['unique_prefix3']}",
        f"- merged_top_n_unique_leading_skeletons: {summary['merged_top_n_diversity']['unique_leading_skeletons']}",
        f"- merged_top_n_unique_ending_families: {summary['merged_top_n_diversity']['unique_ending_families']}",
        "",
        "## Lane Totals",
        "",
    ]
    for lane_summary in summary["lane_summaries"]:
        lines.extend(
            [
                f"### {lane_summary['lane']}",
                f"- batch_id: {lane_summary['batch_id']}",
                f"- succeeded: {lane_summary['succeeded']}",
                f"- failed: {lane_summary['failed']}",
                f"- unique_survivors: {lane_summary['unique_survivors']}",
                f"- merged_top_n_kept: {lane_summary.get('merged_top_n_kept', 0)}",
                f"- top_prefix3_share: {lane_summary['diversity']['top_prefix3']['share']}",
                f"- top_ending_family_share: {lane_summary['diversity']['top_ending_family']['share']}",
                "",
            ]
        )
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _timestamp_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("batch-%Y%m%dT%H%M%SZ")


def _lane_state_dir(*, run_out_dir: Path, lane: str) -> Path:
    return run_out_dir / "lane_state" / str(lane).strip()


def _lane_db_path(*, run_out_dir: Path, lane: str) -> Path:
    return _lane_state_dir(run_out_dir=run_out_dir, lane=lane) / "brandpipe.db"


def _lane_export_template(*, run_out_dir: Path, lane: str) -> Path:
    return _lane_state_dir(run_out_dir=run_out_dir, lane=lane) / "finalists_{run_id}.csv"


def _build_isolated_batch_run_configs(
    *,
    lane: str,
    config_path: Path,
    briefs_file: Path,
    run_out_dir: Path,
    pseudoword_rare_seed_count: int = 0,
    pseudoword_rare_profile: str = "off",
) -> tuple[str, list[object]]:
    base_title, run_configs = build_batch_run_configs(
        template_config_path=config_path,
        briefs_file_path=briefs_file,
    )
    lane_db = _lane_db_path(run_out_dir=run_out_dir, lane=lane)
    lane_export = _lane_export_template(run_out_dir=run_out_dir, lane=lane)
    resolved_rare_profile = (
        str(pseudoword_rare_profile).strip().lower()
        if str(pseudoword_rare_profile).strip()
        else ("balanced" if int(pseudoword_rare_seed_count) > 0 else "off")
    )
    isolated_configs = [
        replace(
            run_config,
            db_path=lane_db,
            ideation=replace(
                run_config.ideation,
                pseudoword=(
                    replace(
                        run_config.ideation.pseudoword,
                        rare_seed_count=max(0, int(pseudoword_rare_seed_count)),
                        rare_profile=resolved_rare_profile if int(pseudoword_rare_seed_count) > 0 else "off",
                    )
                    if run_config.ideation.pseudoword is not None
                    else None
                ),
            ),
            export=replace(run_config.export, out_csv=lane_export),
        )
        for run_config in run_configs
    ]
    return base_title, isolated_configs


def _run_batch_isolated(
    *,
    lane: str,
    config_path: Path,
    briefs_file: Path,
    run_out_dir: Path,
    batch_id: str = "",
    stop_on_error: bool = False,
    pseudoword_rare_seed_count: int = 0,
    pseudoword_rare_profile: str = "off",
) -> dict[str, object]:
    resolved_batch_id = batch_id.strip() or _timestamp_batch_id()
    base_title, run_configs = _build_isolated_batch_run_configs(
        lane=lane,
        config_path=config_path,
        briefs_file=briefs_file,
        run_out_dir=run_out_dir,
        pseudoword_rare_seed_count=pseudoword_rare_seed_count,
        pseudoword_rare_profile=pseudoword_rare_profile,
    )
    succeeded = 0
    failed = 0
    run_ids: list[int] = []
    failures: list[dict[str, object]] = []
    for index, run_config in enumerate(run_configs):
        try:
            run_id = run_loaded_config(
                run_config,
                config_path=config_path,
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


def _auto_lane_cap(*, top_n: int, lane_count: int) -> int:
    if top_n <= 0 or lane_count <= 0:
        return 0
    fair_share = top_n / lane_count
    return max(6, int(math.ceil(fair_share * 1.5)))


def _merge_rows(
    rows: list[dict[str, object]],
    *,
    top_n: int,
    lane_cap: int,
) -> list[dict[str, object]]:
    merged: list[dict[str, object]] = []
    seen: set[str] = set()
    lane_counts: Counter[str] = Counter()
    backlog: list[dict[str, object]] = []

    for row in sorted(rows, key=_rank_key):
        normalized = _normalize_name(row["name"])
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        lane = str(row.get("lane") or "")
        if lane_cap > 0 and lane_counts[lane] >= lane_cap:
            backlog.append(row)
            continue
        merged.append(row)
        lane_counts[lane] += 1
        if len(merged) >= top_n:
            return merged

    if len(merged) >= top_n:
        return merged[:top_n]

    for row in backlog:
        lane = str(row.get("lane") or "")
        merged.append(row)
        lane_counts[lane] += 1
        if len(merged) >= top_n:
            break

    return merged[:top_n]


def _run_step_progress(
    *,
    current_step: str,
    candidates: int,
    validation_results: int,
    expected_validation_results: int,
    rankings: int,
) -> float:
    step = str(current_step or "").strip().lower()
    if step in {"complete", "completed"}:
        return 1.0
    if step == "failed":
        return 1.0
    if step == "created":
        return 0.02
    if step == "ideation":
        return 0.15
    if step == "validation":
        if expected_validation_results > 0:
            validation_fraction = max(0.0, min(1.0, validation_results / expected_validation_results))
        else:
            validation_fraction = 0.0
        return 0.2 + (0.6 * validation_fraction)
    if step == "ranking":
        if candidates > 0:
            ranking_fraction = max(0.0, min(1.0, rankings / candidates))
        else:
            ranking_fraction = 0.0
        return 0.82 + (0.13 * ranking_fraction)
    if step == "export":
        return 0.97
    return 0.0


def _run_counts(conn, *, run_id: int, validation_check_count: int) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
          count(distinct c.id) as candidates,
          sum(case when r.result_key != 'attractiveness' then 1 else 0 end) as validation_results,
          count(distinct rk.id) as rankings
        FROM candidates c
        LEFT JOIN candidate_results r ON r.candidate_id = c.id
        LEFT JOIN candidate_rankings rk ON rk.candidate_id = c.id
        WHERE c.run_id = ?
        """,
        (run_id,),
    ).fetchone()
    candidates = int((row["candidates"] if row is not None else 0) or 0)
    validation_results = int((row["validation_results"] if row is not None else 0) or 0)
    rankings = int((row["rankings"] if row is not None else 0) or 0)
    expected_validation_results = candidates * max(0, int(validation_check_count))
    return {
        "candidates": candidates,
        "validation_results": validation_results,
        "expected_validation_results": expected_validation_results,
        "rankings": rankings,
    }


def _lane_progress_snapshot(
    *,
    db_path: Path,
    batch_id: str,
    requested: int,
    validation_check_count: int,
    stop_on_error: bool,
) -> dict[str, object]:
    if not db_path.exists():
        return {}
    try:
        with db.open_db(db_path) as conn:
            db.ensure_schema(conn)
            runs = db.list_runs(conn, limit=max(1, requested + 8), batch_id=batch_id)
            if not runs:
                return {
                    "requested": int(requested),
                    "succeeded": 0,
                    "failed": 0,
                    "overall_progress": 0.0,
                    "current": None,
                }
            runs_sorted = sorted(
                runs,
                key=lambda row: (
                    int(row["batch_index"]) if row["batch_index"] is not None else 999999,
                    int(row["id"]),
                ),
            )
            succeeded = sum(1 for row in runs_sorted if str(row["status"]) == "completed")
            failed = sum(1 for row in runs_sorted if str(row["status"]) == "failed")
            current_row = next(
                (row for row in runs_sorted if str(row["status"]) in {"created", "running"}),
                runs_sorted[-1],
            )
            run_id = int(current_row["id"])
            counts = _run_counts(conn, run_id=run_id, validation_check_count=validation_check_count)
            current_step = str(current_row["current_step"] or "")
            step_progress = _run_step_progress(
                current_step=current_step,
                candidates=counts["candidates"],
                validation_results=counts["validation_results"],
                expected_validation_results=counts["expected_validation_results"],
                rankings=counts["rankings"],
            )
            effective_requested = _effective_requested_count(
                requested=requested,
                succeeded=succeeded,
                failed=failed,
                current_status=str(current_row["status"] or ""),
                stop_on_error=stop_on_error,
            )
            overall_progress = (
                (succeeded + failed + (0.0 if str(current_row["status"]) == "completed" else step_progress))
                / max(1, effective_requested)
            )
            return {
                "requested": int(requested),
                "effective_requested": int(effective_requested),
                "succeeded": int(succeeded),
                "failed": int(failed),
                "overall_progress": round(max(0.0, min(1.0, overall_progress)), 4),
                "current": {
                    "run_id": run_id,
                    "title": str(current_row["title"] or ""),
                    "status": str(current_row["status"] or ""),
                    "current_step": current_step,
                    "updated_at": str(current_row["updated_at"] or ""),
                    **counts,
                    "step_progress": round(step_progress, 4),
                },
            }
    except Exception as exc:
        return {
            "requested": int(requested),
            "succeeded": 0,
            "failed": 0,
            "overall_progress": 0.0,
            "current": None,
            "error": f"{exc.__class__.__name__}: {exc}",
        }


def _progress_signature(snapshot: dict[str, object]) -> tuple[object, ...]:
    current = snapshot.get("current") or {}
    if not isinstance(current, dict):
        current = {}
    return (
        snapshot.get("succeeded"),
        snapshot.get("failed"),
        snapshot.get("overall_progress"),
        current.get("run_id"),
        current.get("status"),
        current.get("current_step"),
        current.get("candidates"),
        current.get("validation_results"),
        current.get("expected_validation_results"),
        current.get("rankings"),
    )


def _format_progress_line(*, lane: str, snapshot: dict[str, object]) -> str:
    overall_progress = float(snapshot.get("overall_progress") or 0.0)
    bar = _progress_bar(overall_progress)
    succeeded = int(snapshot.get("succeeded") or 0)
    failed = int(snapshot.get("failed") or 0)
    requested = int(snapshot.get("requested") or 0)
    effective_requested = int(snapshot.get("effective_requested") or requested)
    current = snapshot.get("current") or {}
    if not isinstance(current, dict) or not current:
        return f"[{_timestamp_local()}] lane={lane} {bar} {succeeded}/{effective_requested} complete failed={failed}"
    current_title = str(current.get("title") or "")
    title_label = current_title.split(":", 1)[-1] if ":" in current_title else current_title
    step = str(current.get("current_step") or "")
    candidates = int(current.get("candidates") or 0)
    validation_results = int(current.get("validation_results") or 0)
    expected_validation_results = int(current.get("expected_validation_results") or 0)
    rankings = int(current.get("rankings") or 0)
    detail = ""
    if step == "validation":
        detail = f" validation={validation_results}/{expected_validation_results}"
    elif step == "ranking":
        detail = f" rankings={rankings}/{max(candidates, 1)}"
    elif step == "ideation":
        detail = f" candidates={candidates}"
    return (
        f"[{_timestamp_local()}] lane={lane} {bar} "
        f"done={succeeded}/{effective_requested} failed={failed} "
        f"current={title_label} step={step}{detail}"
    )


def _append_log(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip() + "\n")


def _write_progress_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _update_active_lanes(
    *,
    lane: str,
    active: bool,
    progress_payload: dict[str, object],
    progress_json_path: Path,
    progress_lock: threading.Lock,
) -> None:
    with progress_lock:
        current = progress_payload.get("active_lanes")
        active_lanes = [str(item).strip() for item in current if str(item).strip()] if isinstance(current, list) else []
        if active:
            if lane not in active_lanes:
                active_lanes.append(lane)
        else:
            active_lanes = [item for item in active_lanes if item != lane]
        progress_payload["active_lanes"] = active_lanes
        if not active_lanes:
            progress_payload.pop("current_lane", None)
        elif len(active_lanes) == 1:
            progress_payload["current_lane"] = active_lanes[0]
        else:
            progress_payload["current_lane"] = "multiple"
        _write_progress_json(progress_json_path, progress_payload)


def _monitor_lane_progress(
    *,
    lane: str,
    db_path: Path,
    batch_id: str,
    requested: int,
    validation_check_count: int,
    stop_on_error: bool,
    poll_s: float,
    stop_event: threading.Event,
    progress_log_path: Path,
    progress_json_path: Path,
    progress_payload: dict[str, object],
    progress_lock: threading.Lock,
) -> None:
    last_signature: tuple[object, ...] | None = None
    while not stop_event.is_set():
        snapshot = _lane_progress_snapshot(
            db_path=db_path,
            batch_id=batch_id,
            requested=requested,
            validation_check_count=validation_check_count,
            stop_on_error=stop_on_error,
        )
        if snapshot:
            with progress_lock:
                progress_payload.setdefault("lanes", {})
                lanes_payload = progress_payload["lanes"]
                if isinstance(lanes_payload, dict):
                    lanes_payload[lane] = snapshot
                _write_progress_json(progress_json_path, progress_payload)
            signature = _progress_signature(snapshot)
            if signature != last_signature:
                line = _format_progress_line(lane=lane, snapshot=snapshot)
                print(line, flush=True)
                _append_log(progress_log_path, line)
                last_signature = signature
        stop_event.wait(max(0.5, float(poll_s)))


def _run_lane(
    *,
    lane: str,
    config_path: Path,
    briefs_file: Path,
    run_out_dir: Path,
    generated_at: str,
    requested_briefs: int,
    stop_on_error: bool,
    progress_poll_s: float,
    progress_log_path: Path,
    progress_json_path: Path,
    progress_payload: dict[str, object],
    progress_lock: threading.Lock,
    pseudoword_rare_seed_count: int,
    pseudoword_rare_profile: str,
) -> dict[str, object]:
    lane_db_path = _lane_db_path(run_out_dir=run_out_dir, lane=lane)
    validation_check_count = len(load_config(config_path).validation.checks)
    batch_id = f"attack-{generated_at}-{lane}"

    _update_active_lanes(
        lane=lane,
        active=True,
        progress_payload=progress_payload,
        progress_json_path=progress_json_path,
        progress_lock=progress_lock,
    )
    start_line = f"[{_timestamp_local()}] lane_started lane={lane} batch_id={batch_id} requested={requested_briefs} db={lane_db_path}"
    print(start_line, flush=True)
    _append_log(progress_log_path, start_line)

    stop_event = threading.Event()
    monitor = threading.Thread(
        target=_monitor_lane_progress,
        kwargs={
            "lane": lane,
            "db_path": lane_db_path,
            "batch_id": batch_id,
            "requested": requested_briefs,
            "validation_check_count": validation_check_count,
            "stop_on_error": stop_on_error,
            "poll_s": progress_poll_s,
            "stop_event": stop_event,
            "progress_log_path": progress_log_path,
            "progress_json_path": progress_json_path,
            "progress_payload": progress_payload,
            "progress_lock": progress_lock,
        },
        daemon=True,
    )
    monitor.start()
    try:
        summary = _run_batch_isolated(
            lane=lane,
            config_path=config_path,
            briefs_file=briefs_file,
            run_out_dir=run_out_dir,
            batch_id=batch_id,
            stop_on_error=stop_on_error,
            pseudoword_rare_seed_count=pseudoword_rare_seed_count,
            pseudoword_rare_profile=pseudoword_rare_profile,
        )
    finally:
        stop_event.set()
        monitor.join(timeout=max(1.0, float(progress_poll_s) + 1.0))
        final_snapshot = _lane_progress_snapshot(
            db_path=lane_db_path,
            batch_id=batch_id,
            requested=requested_briefs,
            validation_check_count=validation_check_count,
            stop_on_error=stop_on_error,
        )
        if final_snapshot:
            with progress_lock:
                progress_payload.setdefault("lanes", {})
                lanes_payload = progress_payload["lanes"]
                if isinstance(lanes_payload, dict):
                    lanes_payload[lane] = final_snapshot
                _write_progress_json(progress_json_path, progress_payload)
        _update_active_lanes(
            lane=lane,
            active=False,
            progress_payload=progress_payload,
            progress_json_path=progress_json_path,
            progress_lock=progress_lock,
        )

    lane_rows, lane_summary = _collect_lane_rows(
        lane=lane,
        db_path=lane_db_path,
        batch_summary=summary,
    )
    complete_line = (
        f"[{_timestamp_local()}] lane_completed lane={lane} "
        f"succeeded={summary['succeeded']} failed={summary['failed']} "
        f"unique_survivors={lane_summary['unique_survivors']}"
    )
    print(complete_line, flush=True)
    _append_log(progress_log_path, complete_line)
    return {
        "lane": lane,
        "batch_id": batch_id,
        "lane_rows": lane_rows,
        "lane_summary": lane_summary,
    }


def _collect_lane_rows(*, lane: str, db_path: Path, batch_summary: dict[str, object]) -> tuple[list[dict[str, object]], dict[str, object]]:
    lane_rows: list[dict[str, object]] = []
    with db.open_db(db_path) as conn:
        db.ensure_schema(conn)
        for run_id in batch_summary["run_ids"]:
            run_row = db.get_run(conn, run_id=int(run_id))
            if run_row is None:
                continue
            metrics = json.loads(str(run_row["metrics_json"] or "{}"))
            export_path = str(metrics.get("export_path") or "")
            for row in db.fetch_ranked_rows(conn, run_id=int(run_id), limit=9999):
                decision = str(row["decision"] or "")
                if decision not in {"candidate", "watch"}:
                    continue
                lane_rows.append(
                    {
                        "lane": lane,
                        "run_id": int(run_id),
                        "title": str(run_row["title"] or ""),
                        "decision": decision,
                        "name": str(row["name"] or ""),
                        "total_score": float(row["total_score"] or 0.0),
                        "blocker_count": int(row["blocker_count"] or 0),
                        "unavailable_count": int(row["unavailable_count"] or 0),
                        "unsupported_count": int(row["unsupported_count"] or 0),
                        "warning_count": int(row["warning_count"] or 0),
                        "batch_id": str(batch_summary["batch_id"]),
                        "export_path": export_path,
                    }
                )
    lane_rows = sorted(lane_rows, key=_rank_key)
    deduped: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in lane_rows:
        normalized = _normalize_name(row["name"])
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(row)
    lane_summary = {
        "lane": lane,
        "batch_id": batch_summary["batch_id"],
        "requested": int(batch_summary["requested"]),
        "succeeded": int(batch_summary["succeeded"]),
        "failed": int(batch_summary["failed"]),
        "run_ids": list(batch_summary["run_ids"]),
        "unique_survivors": len(deduped),
        "diversity": _summarize_diversity(deduped),
    }
    return deduped, lane_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a six-lane brandpipe brute-force attack and merge survivors.")
    parser.add_argument(
        "--briefs-file",
        default=str(ROOT_DIR / "resources/brandpipe/example_batch_briefs.toml"),
        help="Batch briefs file (.toml, .json, .jsonl).",
    )
    parser.add_argument(
        "--lanes",
        default="all",
        help="Comma-separated lane names or 'all'.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=80,
        help="Merged top-N survivors to export for review.",
    )
    parser.add_argument(
        "--lane-cap",
        type=int,
        default=-1,
        help="Per-lane cap inside the merged top-N. Use 0 to disable, -1 for auto.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "test_outputs/brandpipe/attack_runs"),
        help="Directory for merged attack summaries.",
    )
    parser.add_argument(
        "--progress-poll-s",
        type=float,
        default=5.0,
        help="Polling interval in seconds for live progress updates.",
    )
    parser.add_argument(
        "--progress-log",
        default="",
        help="Optional path for the live progress log. Defaults to <run_dir>/attack_progress.log.",
    )
    parser.add_argument(
        "--progress-json",
        default="",
        help="Optional path for the live progress JSON. Defaults to <run_dir>/attack_progress.json.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned lane/config matrix without running it.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop the lane's batch on the first failed brief.",
    )
    parser.add_argument(
        "--lane-workers",
        type=int,
        default=1,
        help="Number of lanes to run concurrently. Use 1 for serial lane execution.",
    )
    parser.add_argument(
        "--pseudoword-rare-seed-count",
        type=int,
        default=0,
        help="Extra phase-1 low-collision pseudoword seeds to synthesize per brief.",
    )
    parser.add_argument(
        "--pseudoword-rare-profile",
        default="off",
        choices=("off", "balanced", "aggressive"),
        help="Rarity profile for the synthetic phase-1 pseudoword generator.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    briefs_file = Path(args.briefs_file).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    rare_seed_count = max(0, int(getattr(args, "pseudoword_rare_seed_count", 0) or 0))
    rare_profile = str(getattr(args, "pseudoword_rare_profile", "off") or "").strip().lower()
    if not rare_profile:
        rare_profile = "balanced" if rare_seed_count > 0 else "off"
    if str(args.lanes).strip().lower() == "all":
        lanes = list(LANE_CONFIGS.keys())
    else:
        lanes = [item.strip() for item in str(args.lanes).split(",") if item.strip()]
    unknown = [lane for lane in lanes if lane not in LANE_CONFIGS]
    if unknown:
        raise SystemExit(f"unknown lanes: {', '.join(unknown)}")

    generated_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_out_dir = out_dir / generated_at
    progress_log_path = (
        Path(args.progress_log).expanduser().resolve()
        if str(args.progress_log).strip()
        else run_out_dir / "attack_progress.log"
    )
    progress_json_path = (
        Path(args.progress_json).expanduser().resolve()
        if str(args.progress_json).strip()
        else run_out_dir / "attack_progress.json"
    )
    if args.dry_run:
        print(f"briefs_file={briefs_file}")
        top_n = max(1, int(args.top_n))
        lane_cap = _auto_lane_cap(top_n=top_n, lane_count=len(lanes)) if int(args.lane_cap) < 0 else max(0, int(args.lane_cap))
        print(f"lane_cap={lane_cap}")
        print(f"lane_workers={max(1, int(args.lane_workers))}")
        print(f"pseudoword_rare_seed_count={rare_seed_count}")
        print(f"pseudoword_rare_profile={rare_profile}")
        print(f"progress_log={progress_log_path}")
        print(f"progress_json={progress_json_path}")
        for lane in lanes:
            print(
                f"lane={lane} config={LANE_CONFIGS[lane]} "
                f"db={_lane_db_path(run_out_dir=run_out_dir, lane=lane)} "
                f"csv={_lane_export_template(run_out_dir=run_out_dir, lane=lane)}"
            )
        return 0

    run_out_dir.mkdir(parents=True, exist_ok=True)

    requested_briefs = len(load_batch_briefs(briefs_file))
    progress_payload: dict[str, object] = {
        "generated_at": generated_at,
        "briefs_file": str(briefs_file),
        "started_at": datetime.now().astimezone().isoformat(),
        "status": "running",
        "lane_workers": max(1, int(args.lane_workers)),
        "active_lanes": [],
        "lanes": {},
    }
    progress_lock = threading.Lock()
    with progress_lock:
        _write_progress_json(progress_json_path, progress_payload)
    _append_log(progress_log_path, f"[{_timestamp_local()}] attack_started generated_at={generated_at} briefs={requested_briefs} lanes={','.join(lanes)}")

    all_rows: list[dict[str, object]] = []
    lane_summaries: list[dict[str, object]] = []
    batch_ids: dict[str, str] = {}
    lane_results: dict[str, dict[str, object]] = {}
    lane_workers = max(1, min(int(args.lane_workers), len(lanes)))
    if lane_workers <= 1:
        for lane in lanes:
            lane_results[lane] = _run_lane(
                lane=lane,
                config_path=LANE_CONFIGS[lane],
                briefs_file=briefs_file,
                run_out_dir=run_out_dir,
                generated_at=generated_at,
                requested_briefs=requested_briefs,
                stop_on_error=bool(args.stop_on_error),
                progress_poll_s=float(args.progress_poll_s),
                progress_log_path=progress_log_path,
                progress_json_path=progress_json_path,
                progress_payload=progress_payload,
                progress_lock=progress_lock,
                pseudoword_rare_seed_count=rare_seed_count,
                pseudoword_rare_profile=rare_profile,
            )
    else:
        with ThreadPoolExecutor(max_workers=lane_workers) as executor:
            futures = {
                executor.submit(
                    _run_lane,
                    lane=lane,
                    config_path=LANE_CONFIGS[lane],
                    briefs_file=briefs_file,
                    run_out_dir=run_out_dir,
                    generated_at=generated_at,
                    requested_briefs=requested_briefs,
                    stop_on_error=bool(args.stop_on_error),
                    progress_poll_s=float(args.progress_poll_s),
                    progress_log_path=progress_log_path,
                    progress_json_path=progress_json_path,
                    progress_payload=progress_payload,
                    progress_lock=progress_lock,
                    pseudoword_rare_seed_count=rare_seed_count,
                    pseudoword_rare_profile=rare_profile,
                ): lane
                for lane in lanes
            }
            try:
                for future in as_completed(futures):
                    lane = futures[future]
                    lane_results[lane] = future.result()
            except Exception:
                for future in futures:
                    future.cancel()
                raise

    for lane in lanes:
        result = lane_results[lane]
        batch_ids[lane] = str(result["batch_id"])
        all_rows.extend(list(result["lane_rows"]))
        lane_summaries.append(dict(result["lane_summary"]))

    top_n = max(1, int(args.top_n))
    lane_cap = _auto_lane_cap(top_n=top_n, lane_count=len(lanes)) if int(args.lane_cap) < 0 else max(0, int(args.lane_cap))
    merged_rows = _merge_rows(all_rows, top_n=max(len(all_rows), top_n), lane_cap=0)
    merged_top = _merge_rows(all_rows, top_n=top_n, lane_cap=lane_cap)
    csv_path = run_out_dir / f"merged_review_top{top_n}.csv"
    json_path = run_out_dir / "attack_summary.json"
    md_path = run_out_dir / "attack_summary.md"
    _write_csv(csv_path, merged_top)

    summary_payload = {
        "generated_at": generated_at,
        "briefs_file": str(briefs_file),
        "lanes": lanes,
        "lane_workers": lane_workers,
        "batch_ids": batch_ids,
        "merged_unique_survivors": len(merged_rows),
        "merged_top_n": len(merged_top),
        "merged_lane_cap": lane_cap,
        "merged_diversity": _summarize_diversity(merged_rows),
        "merged_top_n_diversity": _summarize_diversity(merged_top),
        "lane_summaries": lane_summaries,
        "merged_csv": str(csv_path),
    }
    top_counts = Counter(str(row.get("lane") or "") for row in merged_top)
    for lane_summary in summary_payload["lane_summaries"]:
        lane_summary["merged_top_n_kept"] = int(top_counts.get(str(lane_summary["lane"]), 0))
    json_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    _write_markdown(md_path, summary_payload)
    with progress_lock:
        progress_payload["status"] = "completed"
        progress_payload["completed_at"] = datetime.now().astimezone().isoformat()
        progress_payload["summary_json"] = str(json_path)
        progress_payload["summary_md"] = str(md_path)
        progress_payload["merged_csv"] = str(csv_path)
        _write_progress_json(progress_json_path, progress_payload)
    _append_log(progress_log_path, f"[{_timestamp_local()}] attack_completed summary_json={json_path}")

    print(f"summary_json={json_path}")
    print(f"summary_md={md_path}")
    print(f"merged_csv={csv_path}")
    print(f"progress_log={progress_log_path}")
    print(f"progress_json={progress_json_path}")
    print(f"merged_unique_survivors={len(merged_rows)}")
    print(f"merged_top_prefix3_share={summary_payload['merged_diversity']['top_prefix3']['share']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
