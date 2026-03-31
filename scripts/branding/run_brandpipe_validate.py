#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
import shutil
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe import db
from brandpipe.models import CandidateResult, ResultStatus, ValidationConfig
from brandpipe.pipeline import run_shortlist_validation
from brandpipe.validation_checks import normalize_name as normalize_validation_name


DEFAULT_CHECKS = ("domain", "package", "company", "web", "app_store", "social", "tm")
REVIEW_COLUMNS = ("keep", "maybe", "drop")
STATE_DIRNAME = "validation_state"
STATE_DB_NAME = "brandpipe.db"


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


def _build_config(args: argparse.Namespace) -> ValidationConfig:
    concurrency = max(1, int(args.concurrency))
    if concurrency > 1:
        print(f"validator_concurrency_deprecated requested={concurrency} effective=1", file=sys.stderr)
    return ValidationConfig(
        checks=_parse_checks(args.checks),
        parallel_workers=1,
        required_domain_tlds=str(args.required_domain_tlds or "").strip(),
        store_countries=str(args.store_countries or "de,ch,us").strip(),
        timeout_s=max(0.5, float(args.timeout_s)),
        company_top=max(1, int(args.company_top)),
        social_unavailable_fail_threshold=max(1, int(args.social_unavailable_fail_threshold)),
        web_search_order=str(args.web_search_order or "brave,browser_google").strip(),
        web_browser_profile_dir=str(args.web_browser_profile_dir or "").strip(),
        web_browser_chrome_executable=str(args.web_browser_chrome_executable or "").strip(),
        tmview_profile_dir=str(args.tmview_profile_dir or "").strip(),
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


def _reason_csv(results: list[object], *, statuses: set[ResultStatus]) -> str:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run shortlist validation with the queue-backed brandpipe validation stack.")
    parser.add_argument("--input-csv", default="", help="Compatible shortlist/review CSV input.")
    parser.add_argument("--names-file", default="", help="Plain newline-delimited names file.")
    parser.add_argument("--names", default="", help="Comma-separated names.")
    parser.add_argument("--mode", choices=("all", "keep", "keep_maybe"), default="keep_maybe")
    parser.add_argument("--out-dir", required=True, help="Output directory.")
    parser.add_argument("--checks", default=",".join(DEFAULT_CHECKS))
    parser.add_argument("--concurrency", type=int, default=1, help="Deprecated compatibility flag; effective value is always 1.")
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--required-domain-tlds", default="")
    parser.add_argument("--store-countries", default="de,ch,us")
    parser.add_argument("--company-top", type=int, default=8)
    parser.add_argument("--social-unavailable-fail-threshold", type=int, default=3)
    parser.add_argument("--web-search-order", default="brave,browser_google")
    parser.add_argument("--web-browser-profile-dir", default="")
    parser.add_argument("--web-browser-chrome-executable", default="")
    parser.add_argument("--tmview-profile-dir", default="")
    parser.add_argument("--tmview-chrome-executable", default="")
    parser.add_argument("--reset-state", action="store_true", help="Delete existing validation state in the out-dir before running.")
    return parser.parse_args()


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


def _state_db_path(out_dir: Path) -> Path:
    return out_dir / STATE_DIRNAME / STATE_DB_NAME


def _load_candidate_result_rows(db_path: Path, *, run_id: int) -> tuple[dict[str, int], dict[int, list[object]]]:
    with db.open_db(db_path) as conn:
        db.ensure_schema(conn)
        candidate_rows = db.list_candidates(conn, run_id=run_id)
        candidate_lookup = {str(row["name"]): int(row["id"]) for row in candidate_rows}
        result_map: dict[int, list[object]] = {}
        for candidate_id in candidate_lookup.values():
            result_map[candidate_id] = [
                CandidateResult(
                    check_name=str(row["result_key"]),
                    status=ResultStatus(str(row["status"])),
                    score_delta=float(row["score_delta"]),
                    reason=str(row["reason"] or ""),
                    details=json.loads(str(row["details_json"] or "{}")),
                )
                for row in db.fetch_results_for_candidate(conn, candidate_id=candidate_id)
            ]
        return candidate_lookup, result_map


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    state_db_path = _state_db_path(out_dir)
    if bool(args.reset_state) and state_db_path.parent.exists():
        shutil.rmtree(state_db_path.parent)
    shortlist_rows = _load_rows(args)
    config = _build_config(args)

    state_db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        run_summary = run_shortlist_validation(
            db_path=state_db_path,
            candidate_names=[row.name_normalized for row in shortlist_rows],
            config=config,
        )
    except RuntimeError as exc:
        if str(exc) == "validation_state_mismatch":
            raise SystemExit(
                f"validation state mismatch in {state_db_path.parent}; use a fresh --out-dir or pass --reset-state"
            ) from exc
        raise

    candidate_lookup, result_map = _load_candidate_result_rows(state_db_path, run_id=int(run_summary["run_id"]))

    all_rows: list[dict[str, object]] = []
    for row in shortlist_rows:
        candidate_id = candidate_lookup.get(row.name_normalized)
        if candidate_id is None:
            continue
        results = result_map.get(candidate_id, [])
        status_map = {item.check_name: item.status.value for item in results}
        publish_bucket = _classify_results(status_map, checks=config.checks)
        all_rows.append(
            {
                "name": row.name_normalized,
                "name_display": row.name_display,
                "shortlist_selected": True,
                "recommendation": row.recommendation,
                "total_score": f"{row.total_score:.2f}",
                "risk": f"{row.risk:.2f}",
                "shortlist_rank": row.shortlist_rank,
                "shortlist_bucket": row.shortlist_bucket,
                "shortlist_reason": row.shortlist_reason,
                "publish_bucket": publish_bucket,
                "blocker_reasons": _reason_csv(results, statuses={ResultStatus.FAIL}),
                "review_reasons": _reason_csv(
                    results,
                    statuses={ResultStatus.WARN, ResultStatus.UNAVAILABLE, ResultStatus.UNSUPPORTED},
                ),
                "check_statuses_json": json.dumps(status_map, sort_keys=True),
            }
        )

    all_rows.sort(
        key=lambda item: (
            {"survivor": 0, "review": 1, "rejected": 2}.get(str(item.get("publish_bucket") or ""), 9),
            0 if str(item.get("shortlist_bucket") or "") == "keep" else 1,
            int(item.get("shortlist_rank") or 0),
            str(item.get("name") or ""),
        )
    )

    survivors = [row for row in all_rows if row["publish_bucket"] == "survivor"]
    review = [row for row in all_rows if row["publish_bucket"] == "review"]
    rejected = [row for row in all_rows if row["publish_bucket"] == "rejected"]

    fieldnames = [
        "name",
        "name_display",
        "shortlist_selected",
        "recommendation",
        "total_score",
        "risk",
        "shortlist_rank",
        "shortlist_bucket",
        "shortlist_reason",
        "publish_bucket",
        "blocker_reasons",
        "review_reasons",
        "check_statuses_json",
    ]
    _write_csv(out_dir / "validated_all.csv", all_rows, fieldnames)
    _write_csv(out_dir / "validated_survivors.csv", survivors, fieldnames)
    _write_csv(out_dir / "validated_review_queue.csv", review, fieldnames)
    _write_csv(out_dir / "validated_rejected.csv", rejected, fieldnames)

    summary = {
        "input_count": len(shortlist_rows),
        "survivor_count": len(survivors),
        "review_count": len(review),
        "rejected_count": len(rejected),
        "checks": list(config.checks),
        "validation_run_id": int(run_summary["run_id"]),
        "validation_fingerprint": str(run_summary["fingerprint"]),
        "validation_db": str(state_db_path.resolve()),
        "validation_job_counts": dict(run_summary["job_counts"]),
        "status_counts": dict(run_summary["validation_status_counts"]),
        "tier_result_counts": {
            "survivor": len(survivors),
            "review": len(review),
            "rejected": len(rejected),
        },
        "total_jobs": int(sum(int(value) for value in run_summary["validation_status_counts"].values())),
        "validated_all_csv": str((out_dir / "validated_all.csv").resolve()),
        "validated_survivors_csv": str((out_dir / "validated_survivors.csv").resolve()),
        "validated_review_queue_csv": str((out_dir / "validated_review_queue.csv").resolve()),
        "validated_rejected_csv": str((out_dir / "validated_rejected.csv").resolve()),
    }
    (out_dir / "validated_publish_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(f"validated_input_count={summary['input_count']}")
    print(f"validated_survivor_count={summary['survivor_count']}")
    print(f"validated_review_count={summary['review_count']}")
    print(f"validated_rejected_count={summary['rejected_count']}")
    print(f"validated_out_dir={out_dir}")
    print(f"validated_db={state_db_path}")
    print(f"run_summary={json.dumps({'status_counts': summary['status_counts'], 'tier_result_counts': summary['tier_result_counts'], 'total_jobs': summary['total_jobs']}, ensure_ascii=False, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
