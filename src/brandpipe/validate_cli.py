from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
import shutil
import sys

from . import db
from .models import CandidateResult, ResultStatus, ValidationConfig
from .pipeline import run_shortlist_validation
from .task_io import copy_inputs, finalize_manifest, init_manifest, prepare_task_paths, slugify_label
from .validation_checks import normalize_name as normalize_validation_name


DEFAULT_CHECKS = ("domain", "package", "company", "web", "app_store", "social", "tm")
REVIEW_COLUMNS = ("keep", "maybe", "drop")


@dataclass(frozen=True)
class ShortlistRow:
    shortlist_rank: int
    name_display: str
    name_normalized: str
    shortlist_bucket: str
    shortlist_reason: str
    recommendation: str
    total_score: float
    risk: float


def _normalize_name(raw: object) -> str:
    return normalize_validation_name(str(raw or ""))


def _is_x(raw: object) -> bool:
    return str(raw or "").strip().lower() == "x"


def _to_float(raw: object, default: float = 0.0) -> float:
    try:
        if raw is None or str(raw).strip() == "":
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default


def _parse_checks(raw: str) -> list[str]:
    checks = [part.strip() for part in str(raw or "").split(",") if part.strip()]
    return checks or list(DEFAULT_CHECKS)


def _read_names_file(path: Path) -> list[ShortlistRow]:
    rows: list[ShortlistRow] = []
    seen: set[str] = set()
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        normalized = _normalize_name(line)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        rows.append(
            ShortlistRow(
                shortlist_rank=index,
                name_display=str(line).strip() or normalized,
                name_normalized=normalized,
                shortlist_bucket="manual",
                shortlist_reason="names_file_input",
                recommendation="",
                total_score=0.0,
                risk=0.0,
            )
        )
    return rows


def _read_review_csv(path: Path, *, mode: str) -> list[ShortlistRow]:
    rows: list[ShortlistRow] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = set(reader.fieldnames or [])
        has_review_marks = all(column in headers for column in REVIEW_COLUMNS)
        for index, row in enumerate(reader, start=1):
            keep = _is_x(row.get("keep"))
            maybe = _is_x(row.get("maybe"))
            include = True
            shortlist_bucket = "selected"
            if has_review_marks:
                if mode == "keep":
                    include = keep
                elif mode == "keep_maybe":
                    include = keep or maybe
                shortlist_bucket = "keep" if keep else ("maybe" if maybe else "drop")
            elif str(row.get("shortlist_selected") or "").strip():
                include = str(row.get("shortlist_selected") or "").strip().lower() in {"1", "true", "yes", "y", "x"}
                shortlist_bucket = "selected" if include else "drop"
            if not include:
                continue
            name_display = str(
                row.get("name_display") or row.get("name_normalized") or row.get("name") or ""
            ).strip()
            normalized = _normalize_name(
                row.get("name_normalized") or row.get("name") or row.get("name_display") or ""
            )
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            rows.append(
                ShortlistRow(
                    shortlist_rank=int(float(row.get("rank") or index)),
                    name_display=name_display or normalized,
                    name_normalized=normalized,
                    shortlist_bucket=shortlist_bucket,
                    shortlist_reason=str(row.get("decision_notes") or row.get("shortlist_reason") or "").strip(),
                    recommendation=str(
                        row.get("current_recommendation") or row.get("recommendation") or ""
                    ).strip().lower(),
                    total_score=_to_float(row.get("score"), _to_float(row.get("total_score"))),
                    risk=_to_float(row.get("risk"), _to_float(row.get("current_risk"))),
                )
            )
    rows.sort(key=lambda item: (0 if item.shortlist_bucket == "keep" else 1, item.shortlist_rank, item.name_normalized))
    return rows


def _build_config(args: argparse.Namespace, *, browser_profile_dir: Path) -> ValidationConfig:
    concurrency = max(1, int(args.concurrency))
    if concurrency > 1:
        print(f"validator_concurrency_deprecated requested={concurrency} effective=1", file=sys.stderr)
    resolved_browser_dir = (
        Path(args.web_browser_profile_dir).expanduser().resolve()
        if str(args.web_browser_profile_dir or "").strip()
        else browser_profile_dir
    )
    resolved_tmview_dir = (
        Path(args.tmview_profile_dir).expanduser().resolve()
        if str(args.tmview_profile_dir or "").strip()
        else resolved_browser_dir
    )
    return ValidationConfig(
        checks=_parse_checks(args.checks),
        parallel_workers=1,
        required_domain_tlds=str(args.required_domain_tlds or "").strip(),
        store_countries=str(args.store_countries or "de,ch,us").strip(),
        timeout_s=max(0.5, float(args.timeout_s)),
        company_top=max(1, int(args.company_top)),
        social_unavailable_fail_threshold=max(1, int(args.social_unavailable_fail_threshold)),
        web_search_order=str(args.web_search_order or "serper,brave").strip(),
        web_browser_profile_dir=str(resolved_browser_dir),
        web_browser_chrome_executable=str(args.web_browser_chrome_executable or "").strip(),
        tmview_profile_dir=str(resolved_tmview_dir),
        tmview_chrome_executable=str(args.tmview_chrome_executable or "").strip(),
    )


def _classify_results(statuses: dict[str, str], *, checks: list[str]) -> str:
    blocker_checks = {"domain", "package", "company", "web", "app_store", "tm"}
    selected_blockers = blocker_checks.intersection(checks)
    if any(statuses.get(check_name) == ResultStatus.FAIL.value for check_name in selected_blockers):
        return "rejected"
    if any(
        statuses.get(check_name) in {ResultStatus.WARN.value, ResultStatus.UNAVAILABLE.value, ResultStatus.UNSUPPORTED.value}
        for check_name in checks
    ):
        return "review"
    return "survivor"


def _reason_csv(results: list[CandidateResult], *, statuses: set[ResultStatus]) -> str:
    parts: list[str] = []
    for item in results:
        if item.status not in statuses:
            continue
        reason = str(item.reason or "").strip() or item.status.value
        parts.append(f"{item.check_name}:{item.status.value}:{reason}")
    return ";".join(parts)


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _parse_result_details(raw: object, *, candidate_id: int, result_key: str) -> dict[str, object]:
    payload = str(raw or "").strip()
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        print(
            "validation_details_json_invalid "
            f"candidate_id={candidate_id} result_key={result_key} error={exc.msg}",
            file=sys.stderr,
        )
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {"value": parsed}


def _load_candidate_result_rows(db_path: Path, *, run_id: int) -> tuple[dict[str, int], dict[int, list[CandidateResult]]]:
    candidate_lookup: dict[str, int] = {}
    result_map: dict[int, list[CandidateResult]] = {}
    with db.open_db(db_path) as conn:
        db.ensure_schema(conn)
        for row in db.list_candidates(conn, run_id=run_id):
            candidate_id = int(row["id"])
            row_keys = set(row.keys()) if hasattr(row, "keys") else set()
            display_name = row["display_name"] if "display_name" in row_keys else None
            raw_name = display_name or (row["name"] if "name" in row_keys else None)
            normalized = _normalize_name(raw_name)
            if not normalized:
                continue
            candidate_lookup[normalized] = candidate_id
            result_rows = db.fetch_results_for_candidate(conn, candidate_id=candidate_id)
            result_map[candidate_id] = [
                CandidateResult(
                    check_name=str(result_row["result_key"] or "").strip(),
                    status=ResultStatus(str(result_row["status"] or "").strip()),
                    score_delta=float(result_row["score_delta"] or 0.0),
                    reason=str(result_row["reason"] or "").strip(),
                    details=_parse_result_details(
                        result_row["details_json"],
                        candidate_id=candidate_id,
                        result_key=str(result_row["result_key"] or "").strip(),
                    ),
                )
                for result_row in result_rows
            ]
    return candidate_lookup, result_map


def build_validate_parser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("validate", help="Run queue-backed shortlist validation")
    parser.add_argument("--input-csv", default="", help="Compatible shortlist/review CSV input.")
    parser.add_argument("--names-file", default="", help="Plain newline-delimited names file.")
    parser.add_argument("--names", default="", help="Comma-separated names.")
    parser.add_argument("--mode", choices=("all", "keep", "keep_maybe"), default="keep_maybe")
    parser.add_argument("--out-dir", required=True, help="Validation label root. A new invocation directory is created beneath it.")
    parser.add_argument("--checks", default=",".join(DEFAULT_CHECKS))
    parser.add_argument("--concurrency", type=int, default=1, help="Deprecated compatibility flag; effective value is always 1.")
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--required-domain-tlds", default="")
    parser.add_argument("--store-countries", default="de,ch,us")
    parser.add_argument("--company-top", type=int, default=8)
    parser.add_argument("--social-unavailable-fail-threshold", type=int, default=3)
    parser.add_argument("--web-search-order", default="serper,brave")
    parser.add_argument("--web-browser-profile-dir", default="")
    parser.add_argument("--web-browser-chrome-executable", default="")
    parser.add_argument("--tmview-profile-dir", default="")
    parser.add_argument("--tmview-chrome-executable", default="")
    parser.add_argument("--reset-state", action="store_true", help="Compatibility flag retained; invocations now isolate validation state automatically.")
    return parser


def _load_rows(args: argparse.Namespace) -> list[ShortlistRow]:
    rows: list[ShortlistRow] = []
    if str(args.input_csv).strip():
        rows.extend(_read_review_csv(Path(args.input_csv).expanduser().resolve(), mode=args.mode))
    if str(args.names_file).strip():
        rows.extend(_read_names_file(Path(args.names_file).expanduser().resolve()))
    if str(args.names).strip():
        for index, token in enumerate(str(args.names).split(","), start=len(rows) + 1):
            normalized = _normalize_name(token)
            if not normalized:
                continue
            rows.append(
                ShortlistRow(
                    shortlist_rank=index,
                    name_display=normalized,
                    name_normalized=normalized,
                    shortlist_bucket="manual",
                    shortlist_reason="names_arg_input",
                    recommendation="",
                    total_score=0.0,
                    risk=0.0,
                )
            )
    deduped: list[ShortlistRow] = []
    seen: set[str] = set()
    for row in rows:
        if row.name_normalized in seen:
            continue
        seen.add(row.name_normalized)
        deduped.append(row)
    if not deduped:
        raise SystemExit("no validation input rows found")
    return deduped


def run_validate_command(args: argparse.Namespace) -> int:
    input_rows = _load_rows(args)
    out_root = Path(args.out_dir).expanduser().resolve()
    task_paths = prepare_task_paths(
        task="validate",
        label=slugify_label(out_root.name, default="manual"),
        out_dir=out_root,
    )
    browser_profile_dir = task_paths.profiles_dir / "playwright-profile"
    browser_profile_dir.mkdir(parents=True, exist_ok=True)
    input_files: list[Path] = []
    if str(args.input_csv).strip():
        input_files.append(Path(args.input_csv).expanduser().resolve())
    if str(args.names_file).strip():
        input_files.append(Path(args.names_file).expanduser().resolve())
    copied_inputs = copy_inputs(
        task_paths,
        files=input_files,
        text_blobs=(
            {"names.txt": "\n".join(row.name_display for row in input_rows)}
            if str(args.names).strip()
            else {}
        ),
    )
    init_manifest(
        task_paths,
        config_paths=copied_inputs,
        db_path=str(task_paths.db_path),
        export_paths=[],
        child_runs=[],
        metrics_summary={"input_count": len(input_rows), "mode": args.mode},
    )

    if args.reset_state and task_paths.db_path.parent.exists():
        shutil.rmtree(task_paths.db_path.parent, ignore_errors=True)
        task_paths.state_dir.mkdir(parents=True, exist_ok=True)

    try:
        config = _build_config(args, browser_profile_dir=browser_profile_dir)
        summary = run_shortlist_validation(
            db_path=task_paths.db_path,
            candidate_names=[row.name_display for row in input_rows],
            config=config,
        )
        validation_run_id = int(summary["run_id"])
        candidate_lookup, result_map = _load_candidate_result_rows(task_paths.db_path, run_id=validation_run_id)

        survivors: list[dict[str, object]] = []
        review_rows: list[dict[str, object]] = []
        rejected_rows: list[dict[str, object]] = []
        all_rows: list[dict[str, object]] = []
        fieldnames = [
            "rank",
            "name",
            "name_normalized",
            "shortlist_bucket",
            "shortlist_reason",
            "recommendation",
            "total_score",
            "risk",
            "publish_bucket",
            "check_statuses_json",
            "blocker_reasons",
            "review_reasons",
        ]

        for row in input_rows:
            candidate_id = candidate_lookup.get(row.name_normalized)
            results = result_map.get(candidate_id or -1, [])
            statuses = {result.check_name: result.status.value for result in results}
            publish_bucket = _classify_results(statuses, checks=config.checks)
            record = {
                "rank": row.shortlist_rank,
                "name": row.name_display,
                "name_normalized": row.name_normalized,
                "shortlist_bucket": row.shortlist_bucket,
                "shortlist_reason": row.shortlist_reason,
                "recommendation": row.recommendation,
                "total_score": row.total_score,
                "risk": row.risk,
                "publish_bucket": publish_bucket,
                "check_statuses_json": json.dumps(statuses, ensure_ascii=False, sort_keys=True),
                "blocker_reasons": _reason_csv(results, statuses={ResultStatus.FAIL}),
                "review_reasons": _reason_csv(
                    results,
                    statuses={ResultStatus.WARN, ResultStatus.UNAVAILABLE, ResultStatus.UNSUPPORTED},
                ),
            }
            all_rows.append(record)
            if publish_bucket == "survivor":
                survivors.append(record)
            elif publish_bucket == "review":
                review_rows.append(record)
            else:
                rejected_rows.append(record)

        validated_all = task_paths.exports_dir / "validated_all.csv"
        validated_survivors = task_paths.exports_dir / "validated_survivors.csv"
        validated_review = task_paths.exports_dir / "validated_review_queue.csv"
        validated_rejected = task_paths.exports_dir / "validated_rejected.csv"
        summary_json_path = task_paths.exports_dir / "validated_publish_summary.json"

        _write_csv(validated_all, all_rows, fieldnames)
        _write_csv(validated_survivors, survivors, fieldnames)
        _write_csv(validated_review, review_rows, fieldnames)
        _write_csv(validated_rejected, rejected_rows, fieldnames)

        publish_summary = {
            "task_root": str(task_paths.root),
            "validation_run_id": validation_run_id,
            "state_db": str(task_paths.db_path),
            "input_count": len(all_rows),
            "survivor_count": len(survivors),
            "review_count": len(review_rows),
            "rejected_count": len(rejected_rows),
            "total_jobs": int(sum((summary.get("job_counts") or {}).values())),
            "validation_status_counts": summary.get("validation_status_counts") or {},
            "validation_check_counts": summary.get("validation_check_counts") or {},
            "validated_all_csv": str(validated_all),
            "validated_survivors_csv": str(validated_survivors),
            "validated_review_queue_csv": str(validated_review),
            "validated_rejected_csv": str(validated_rejected),
        }
        summary_json_path.write_text(
            json.dumps(publish_summary, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        finalize_manifest(
            task_paths,
            status="completed",
            db_path=str(task_paths.db_path),
            export_paths=[
                str(validated_all),
                str(validated_survivors),
                str(validated_review),
                str(validated_rejected),
                str(summary_json_path),
            ],
            child_runs=[{"validation_run_id": validation_run_id}],
            metrics_summary={
                "input_count": len(all_rows),
                "survivor_count": len(survivors),
                "review_count": len(review_rows),
                "rejected_count": len(rejected_rows),
            },
        )
        print(f"task_root={task_paths.root}")
        print(f"validation_run_id={validation_run_id}")
        print(f"state_db={task_paths.db_path}")
        print(f"validated_survivors_csv={validated_survivors}")
        print(f"summary_json={summary_json_path}")
        return 0
    except Exception:
        finalize_manifest(
            task_paths,
            status="failed",
            db_path=str(task_paths.db_path),
            metrics_summary={"input_count": len(input_rows)},
        )
        raise
