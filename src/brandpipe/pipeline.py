from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
import time
import tomllib
from collections import Counter
from dataclasses import replace
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path

from . import db
from .diversity import filter_local_collisions
from .lexicon import build_lexicon
from .models import (
    DEFAULT_FAMILY_MIX_PROFILE,
    Brief,
    CandidateResult,
    ExportConfig,
    IdeationConfig,
    IdeationRoleConfig,
    PseudowordConfig,
    ResultStatus,
    RunConfig,
    RunStatus,
    SurfacedCandidate,
    ValidationConfig,
)
from .naming_policy import build_naming_policy, build_validation_name_shape_policy
from .name_normalization import normalize_brand_token
from .ranking import group_results, rank_candidate_surfaces
from .scoring import build_attractiveness_result
from .surface_ideation import generate_candidate_surfaces
from .taste import build_blocked_fragments, filter_names as filter_taste_names
from .tmview import normalize_alpha as normalize_tmview_name, probe_names as probe_tmview_names
from .validation import validate_candidate
from .validation_queue import (
    detect_state_mismatch,
    prepare_shortlist_run,
    run_validation_jobs,
    shortlist_fingerprint,
)
from .scoring import score_name_attractiveness


def _resolve_path(base_dir: Path, raw: str) -> Path:
    path = Path(str(raw).strip()).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def _list_of_strings(raw: object) -> list[str]:
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, str):
        return [part.strip() for part in raw.split(",") if part.strip()]
    return []


def _cfg_str(raw: object, default: str) -> str:
    if raw is None:
        return default
    value = str(raw).strip()
    return value if value else default


def _cfg_int(raw: object, default: int, minimum: int | None = None) -> int:
    value = default if raw is None else int(raw)
    if minimum is not None:
        value = max(minimum, value)
    return value


def _cfg_float(raw: object, default: float, minimum: float | None = None) -> float:
    value = default if raw is None else float(raw)
    if minimum is not None:
        value = max(minimum, value)
    return value


def _warn_runtime_issue(message: str) -> None:
    print(f"[brandpipe] {message}", file=sys.stderr)


def _load_json_dict(raw: object, *, context: str) -> dict[str, object]:
    blob = str(raw or "{}")
    try:
        parsed = json.loads(blob)
    except json.JSONDecodeError as exc:
        _warn_runtime_issue(f"{context}: invalid_json ({exc})")
        return {}
    if isinstance(parsed, dict):
        return parsed
    _warn_runtime_issue(f"{context}: expected_json_object_got_{type(parsed).__name__}")
    return {}


def _cfg_bool(raw: object, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return bool(raw)


def _cfg_int_map(raw: object) -> dict[str, int]:
    if isinstance(raw, dict):
        parsed: dict[str, int] = {}
        for key, value in raw.items():
            token = str(key).strip()
            if not token:
                continue
            parsed[token] = max(0, int(value))
        return parsed
    if isinstance(raw, str):
        parsed = {}
        for part in raw.split(","):
            token = str(part).strip()
            if not token or ":" not in token:
                continue
            key, value = token.split(":", 1)
            key = key.strip()
            if not key:
                continue
            parsed[key] = max(0, int(value.strip() or "0"))
        return parsed
    return {}


def _cfg_path_map(base_dir: Path, raw: object) -> dict[str, Path]:
    if not isinstance(raw, dict):
        return {}
    parsed: dict[str, Path] = {}
    for key, value in raw.items():
        token = str(key or "").strip()
        raw_path = str(value or "").strip()
        if not token or not raw_path:
            continue
        parsed[token] = _resolve_path(base_dir, raw_path)
    return parsed


def _canonical_web_search_order(raw: object, default: str = "serper,brave") -> str:
    order: list[str] = []
    tokens = [part.strip().lower() for part in _cfg_str(raw, default).split(",") if part.strip()]
    for token in tokens:
        if token in {"serper", "brave"} and token not in order:
            order.append(token)
    if "serper" in order:
        order = ["serper", *[item for item in order if item != "serper"]]
    if not order:
        return default
    return ",".join(order)


def load_config(config_path: Path) -> RunConfig:
    base_dir = config_path.parent
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)

    run_cfg = payload.get("run") or {}
    brief_cfg = payload.get("brief") or {}
    ideation_cfg = payload.get("ideation") or {}
    validation_cfg = payload.get("validation") or {}
    export_cfg = payload.get("export") or {}

    title = _cfg_str(run_cfg.get("title"), config_path.stem)
    default_db_path = f"test_outputs/brandpipe/run/{config_path.stem}/brandpipe.db"
    db_path = _resolve_path(base_dir, _cfg_str(run_cfg.get("db_path"), default_db_path))
    brief = Brief(
        product_core=_cfg_str(brief_cfg.get("product_core"), ""),
        target_users=_list_of_strings(brief_cfg.get("target_users")),
        trust_signals=_list_of_strings(brief_cfg.get("trust_signals")),
        forbidden_directions=_list_of_strings(brief_cfg.get("forbidden_directions")),
        language_market=_cfg_str(brief_cfg.get("language_market"), ""),
        notes=_cfg_str(brief_cfg.get("notes"), ""),
    )
    prompt_template_file = None
    if _cfg_str(ideation_cfg.get("prompt_template_file"), ""):
        prompt_template_file = _resolve_path(base_dir, _cfg_str(ideation_cfg.get("prompt_template_file"), ""))
    fixture_input = None
    if _cfg_str(ideation_cfg.get("fixture_input"), ""):
        fixture_input = _resolve_path(base_dir, _cfg_str(ideation_cfg.get("fixture_input"), ""))
    roles: tuple[IdeationRoleConfig, ...] = ()
    raw_roles = ideation_cfg.get("roles")
    if isinstance(raw_roles, list):
        parsed_roles: list[IdeationRoleConfig] = []
        for item in raw_roles:
            if not isinstance(item, dict):
                continue
            model = _cfg_str(item.get("model"), "")
            if not model:
                continue
            parsed_roles.append(
                IdeationRoleConfig(
                    model=model,
                    role=_cfg_str(item.get("role"), "creative_divergence"),
                    temperature=_cfg_float(item.get("temperature"), _cfg_float(ideation_cfg.get("temperature"), 0.8)),
                    weight=_cfg_int(item.get("weight"), 1, minimum=1),
                )
            )
        roles = tuple(parsed_roles)
    pseudoword_cfg = ideation_cfg.get("pseudoword") if isinstance(ideation_cfg.get("pseudoword"), dict) else {}
    pseudoword = None
    if pseudoword_cfg:
        pseudoword = PseudowordConfig(
            language_plugin=_cfg_str(pseudoword_cfg.get("language_plugin"), "orthographic_english"),
            language_plugins=tuple(_list_of_strings(pseudoword_cfg.get("language_plugins"))),
            seed_count=_cfg_int(pseudoword_cfg.get("seed_count"), 18, minimum=1),
            rare_seed_count=_cfg_int(pseudoword_cfg.get("rare_seed_count"), 0, minimum=0),
            rare_profile=_cfg_str(pseudoword_cfg.get("rare_profile"), "off"),
        )
    ideation = IdeationConfig(
        provider=_cfg_str(ideation_cfg.get("provider"), "fixture"),
        model=_cfg_str(ideation_cfg.get("model"), ""),
        rounds=_cfg_int(ideation_cfg.get("rounds"), 1, minimum=1),
        candidates_per_round=_cfg_int(ideation_cfg.get("candidates_per_round"), 12, minimum=1),
        overgenerate_factor=_cfg_float(ideation_cfg.get("overgenerate_factor"), 2.0, minimum=1.0),
        round_seed_min=_cfg_int(ideation_cfg.get("round_seed_min"), 3, minimum=1),
        round_seed_max=_cfg_int(ideation_cfg.get("round_seed_max"), 6, minimum=1),
        seed_pool_multiplier=_cfg_int(ideation_cfg.get("seed_pool_multiplier"), 8, minimum=1),
        seed_saturation_limit=_cfg_int(ideation_cfg.get("seed_saturation_limit"), 1, minimum=1),
        per_family_cap=_cfg_int(ideation_cfg.get("per_family_cap"), 2, minimum=1),
        lexicon_core_limit=_cfg_int(ideation_cfg.get("lexicon_core_limit"), 6, minimum=1),
        lexicon_modifier_limit=_cfg_int(ideation_cfg.get("lexicon_modifier_limit"), 6, minimum=1),
        lexicon_associative_limit=_cfg_int(ideation_cfg.get("lexicon_associative_limit"), 6, minimum=1),
        lexicon_morpheme_limit=_cfg_int(ideation_cfg.get("lexicon_morpheme_limit"), 8, minimum=1),
        local_filter_saturation_limit=_cfg_int(ideation_cfg.get("local_filter_saturation_limit"), 1, minimum=1),
        local_filter_lead_fragment_limit=_cfg_int(ideation_cfg.get("local_filter_lead_fragment_limit"), 0, minimum=0),
        local_filter_lead_fragment_length=_cfg_int(ideation_cfg.get("local_filter_lead_fragment_length"), 4, minimum=2),
        local_filter_lead_skeleton_limit=_cfg_int(ideation_cfg.get("local_filter_lead_skeleton_limit"), 0, minimum=0),
        temperature=_cfg_float(ideation_cfg.get("temperature"), 0.8),
        timeout_ms=_cfg_int(ideation_cfg.get("timeout_ms"), 60000, minimum=1000),
        strict_json=_cfg_bool(ideation_cfg.get("strict_json"), True),
        prompt_template_file=prompt_template_file,
        fixture_input=fixture_input,
        openai_base_url=_cfg_str(ideation_cfg.get("openai_base_url"), "http://127.0.0.1:1234/v1"),
        api_key_env=_cfg_str(ideation_cfg.get("api_key_env"), "OPENROUTER_API_KEY"),
        input_price_per_1k=_cfg_float(ideation_cfg.get("input_price_per_1k"), 0.0),
        output_price_per_1k=_cfg_float(ideation_cfg.get("output_price_per_1k"), 0.0),
        pseudoword=pseudoword,
        roles=roles,
        family_mix_profile=_cfg_str(ideation_cfg.get("family_mix_profile"), DEFAULT_FAMILY_MIX_PROFILE),
        family_prompt_template_files=_cfg_path_map(base_dir, ideation_cfg.get("family_prompt_template_files")),
        family_llm_retry_limit=_cfg_int(ideation_cfg.get("family_llm_retry_limit"), 2, minimum=0),
        family_quotas=_cfg_int_map(ideation_cfg.get("family_quotas")),
        late_fusion_min_per_family=_cfg_int(ideation_cfg.get("late_fusion_min_per_family"), 1, minimum=0),
        naming_policy=build_naming_policy(ideation_cfg.get("naming_policy")),
    )
    validation_shape_default = replace(
        ideation.naming_policy.shape,
        allow_digits=True,
        require_letter=True,
    )
    validation = ValidationConfig(
        checks=_list_of_strings(validation_cfg.get("checks")),
        parallel_workers=_cfg_int(validation_cfg.get("parallel_workers"), 1, minimum=1),
        scope=_cfg_str(validation_cfg.get("scope"), "global"),
        required_domain_tlds=_cfg_str(validation_cfg.get("required_domain_tlds"), ""),
        store_countries=_cfg_str(validation_cfg.get("store_countries"), "de,ch,it"),
        timeout_s=_cfg_float(validation_cfg.get("timeout_s"), 8.0, minimum=0.1),
        company_top=_cfg_int(validation_cfg.get("company_top"), 8, minimum=1),
        social_unavailable_fail_threshold=_cfg_int(validation_cfg.get("social_unavailable_fail_threshold"), 3, minimum=1),
        web_search_order=_canonical_web_search_order(validation_cfg.get("web_search_order")),
        web_brave_top=_cfg_int(validation_cfg.get("web_brave_top"), 8, minimum=1),
        web_brave_api_env=_cfg_str(validation_cfg.get("web_brave_api_env"), "BRAVE_API_KEY"),
        web_brave_country=_cfg_str(validation_cfg.get("web_brave_country"), "DE"),
        web_brave_search_lang=_cfg_str(validation_cfg.get("web_brave_search_lang"), "en"),
        web_google_top=_cfg_int(validation_cfg.get("web_google_top"), 8, minimum=1),
        web_google_api_env=_cfg_str(validation_cfg.get("web_google_api_env"), "SERPER_API_KEY"),
        web_google_cx_env=_cfg_str(validation_cfg.get("web_google_cx_env"), "GOOGLE_CSE_CX"),
        web_google_gl=_cfg_str(validation_cfg.get("web_google_gl"), "de"),
        web_google_hl=_cfg_str(validation_cfg.get("web_google_hl"), "en"),
        web_browser_profile_dir=_cfg_str(validation_cfg.get("web_browser_profile_dir"), ""),
        web_browser_chrome_executable=_cfg_str(validation_cfg.get("web_browser_chrome_executable"), ""),
        web_retry_attempts=_cfg_int(validation_cfg.get("web_retry_attempts"), 2, minimum=0),
        web_retry_backoff_s=_cfg_float(validation_cfg.get("web_retry_backoff_s"), 1.0, minimum=0.0),
        tm_registry_top=_cfg_int(validation_cfg.get("tm_registry_top"), 12, minimum=1),
        tmview_profile_dir=_cfg_str(validation_cfg.get("tmview_profile_dir"), ""),
        tmview_chrome_executable=_cfg_str(validation_cfg.get("tmview_chrome_executable"), ""),
        name_shape_policy=build_validation_name_shape_policy(
            validation_cfg.get("name_shape_policy"),
            default=validation_shape_default,
        ),
    )
    out_csv = None
    if _cfg_str(export_cfg.get("out_csv"), ""):
        out_csv = _resolve_path(base_dir, _cfg_str(export_cfg.get("out_csv"), ""))
    export = ExportConfig(
        out_csv=out_csv,
        top_n=_cfg_int(export_cfg.get("top_n"), 25, minimum=1),
    )
    return RunConfig(
        db_path=db_path,
        title=title,
        brief=brief,
        ideation=ideation,
        validation=validation,
        export=export,
    )


def _serialize_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {field.name: _serialize_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    return value


def _serialize_run_config(config: RunConfig) -> dict[str, object]:
    return _serialize_value(config)  # type: ignore[return-value]


def build_run_config(
    *,
    base_config: RunConfig,
    brief: Brief | None = None,
    title: str | None = None,
) -> RunConfig:
    return replace(
        base_config,
        brief=brief if brief is not None else base_config.brief,
        title=title if title is not None else base_config.title,
    )


def export_ranked_csv(*, conn, run_id: int, out_path: Path, limit: int) -> Path:
    rows = db.fetch_ranked_rows(conn, run_id=run_id, limit=limit)
    run_row = db.get_run(conn, run_id=run_id)
    config_payload = _load_json_dict(run_row["config_json"], context=f"run:{run_id}:config_json") if run_row is not None else {}
    ideation_payload = config_payload.get("ideation") or {}
    naming_policy = build_naming_policy(ideation_payload.get("naming_policy"))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="",
            dir=out_path.parent,
            prefix=f"{out_path.stem}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "rank",
                    "name",
                    "family",
                    "surface_policy",
                    "total_score",
                    "family_score",
                    "family_rank",
                    "attractiveness_score",
                    "attractiveness_status",
                    "attractiveness_reasons",
                    "blocker_count",
                    "unavailable_count",
                    "unsupported_count",
                    "warning_count",
                    "decision",
                ],
            )
            writer.writeheader()
            for rank, row in enumerate(rows, start=1):
                display_name = str(row["display_name"] or row["name"])
                attractiveness = score_name_attractiveness(display_name, policy=naming_policy)
                writer.writerow(
                    {
                        "rank": rank,
                    "name": _sanitize_csv_value(display_name),
                    "family": _sanitize_csv_value(row["family"]),
                    "surface_policy": _sanitize_csv_value(row["surface_policy"]),
                    "total_score": row["total_score"],
                    "family_score": row["family_score"],
                    "family_rank": row["family_rank"],
                    "attractiveness_score": attractiveness.score_delta,
                    "attractiveness_status": attractiveness.status,
                    "attractiveness_reasons": _sanitize_csv_value(",".join(attractiveness.reasons)),
                    "blocker_count": row["blocker_count"],
                    "unavailable_count": row["unavailable_count"],
                    "unsupported_count": row["unsupported_count"],
                    "warning_count": row["warning_count"],
                    "decision": row["decision"],
                }
            )
        os.replace(temp_path, out_path)
    finally:
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink(missing_ok=True)
            except OSError as exc:
                _warn_runtime_issue(
                    f"export_cleanup:{out_path}:temp_file_unlink_failed ({exc.__class__.__name__}: {exc})"
                )
    return out_path


def _format_export_path(template: Path | None, run_id: int) -> Path | None:
    if template is None:
        return None
    rendered = str(template).replace("{run_id}", str(run_id))
    return Path(rendered).expanduser().resolve()


def _runtime_unavailable_result(check_name: str, exc: Exception) -> CandidateResult:
    return CandidateResult(
        check_name=check_name,
        status=ResultStatus.UNAVAILABLE,
        score_delta=0.0,
        reason=f"{exc.__class__.__name__}: {exc}",
        details={
            "error_class": exc.__class__.__name__,
            "error_message": str(exc),
        },
    )


def _validation_worker_count(*, config: ValidationConfig, candidate_count: int) -> int:
    if candidate_count <= 0:
        return 1
    return max(1, min(int(config.parallel_workers), candidate_count))


def _validate_candidate_safe(*, candidate_name: str, config: ValidationConfig) -> list[CandidateResult]:
    try:
        return validate_candidate(name=candidate_name, config=config)
    except Exception as exc:
        return [_runtime_unavailable_result("validation_runtime", exc)]


def _sanitize_csv_value(value: object) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ")
    if text.startswith(("=", "+", "-", "@")):
        return f"'{text}"
    return text


def _candidate_results_from_rows(rows: list[object]) -> list[CandidateResult]:
    parsed: list[CandidateResult] = []
    for row in rows:
        details = _load_json_dict(
            row["details_json"],
            context=f"candidate_result:{row['result_key'] or 'unknown'}",
        )
        parsed.append(
            CandidateResult(
                check_name=str(row["result_key"] or "").strip(),
                status=ResultStatus(str(row["status"] or "").strip()),
                score_delta=float(row["score_delta"] or 0.0),
                reason=str(row["reason"] or "").strip(),
                details=details,
            )
        )
    return parsed


def _json_string(value: object) -> str:
    try:
        return json.dumps(value)
    except (TypeError, ValueError) as exc:
        return json.dumps(
            {
                "serialization_error": f"{exc.__class__.__name__}: {exc}",
            }
        )


def _normalize_names(names: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_name in names:
        name = str(raw_name).strip()
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(name)
    return normalized


def _merge_unique_strings(*groups: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for raw in group:
            value = str(raw).strip()
            if not value:
                continue
            key = value.casefold()
            if key in seen:
                continue
            seen.add(key)
            merged.append(value)
    return merged


def _combine_notes(*parts: str) -> str:
    notes = [str(part).strip() for part in parts if str(part).strip()]
    return " ".join(notes)


def _validation_config_from_payload(raw: object) -> ValidationConfig:
    payload = raw if isinstance(raw, dict) else {}
    return ValidationConfig(
        checks=_list_of_strings(payload.get("checks")),
        parallel_workers=_cfg_int(payload.get("parallel_workers"), 1, minimum=1),
        scope=_cfg_str(payload.get("scope"), "global"),
        required_domain_tlds=_cfg_str(payload.get("required_domain_tlds"), ""),
        store_countries=_cfg_str(payload.get("store_countries"), "de,ch,it"),
        timeout_s=_cfg_float(payload.get("timeout_s"), 8.0, minimum=0.1),
        company_top=_cfg_int(payload.get("company_top"), 8, minimum=1),
        social_unavailable_fail_threshold=_cfg_int(payload.get("social_unavailable_fail_threshold"), 3, minimum=1),
        web_search_order=_canonical_web_search_order(payload.get("web_search_order")),
        web_brave_top=_cfg_int(payload.get("web_brave_top"), 8, minimum=1),
        web_brave_api_env=_cfg_str(payload.get("web_brave_api_env"), "BRAVE_API_KEY"),
        web_brave_country=_cfg_str(payload.get("web_brave_country"), "DE"),
        web_brave_search_lang=_cfg_str(payload.get("web_brave_search_lang"), "en"),
        web_google_top=_cfg_int(payload.get("web_google_top"), 8, minimum=1),
        web_google_api_env=_cfg_str(payload.get("web_google_api_env"), "SERPER_API_KEY"),
        web_google_cx_env=_cfg_str(payload.get("web_google_cx_env"), "GOOGLE_CSE_CX"),
        web_google_gl=_cfg_str(payload.get("web_google_gl"), "de"),
        web_google_hl=_cfg_str(payload.get("web_google_hl"), "en"),
        web_browser_profile_dir=_cfg_str(payload.get("web_browser_profile_dir"), ""),
        web_browser_chrome_executable=_cfg_str(payload.get("web_browser_chrome_executable"), ""),
        web_retry_attempts=_cfg_int(payload.get("web_retry_attempts"), 2, minimum=0),
        web_retry_backoff_s=_cfg_float(payload.get("web_retry_backoff_s"), 1.0, minimum=0.0),
        tm_registry_top=_cfg_int(payload.get("tm_registry_top"), 12, minimum=1),
        tmview_profile_dir=_cfg_str(payload.get("tmview_profile_dir"), ""),
        tmview_chrome_executable=_cfg_str(payload.get("tmview_chrome_executable"), ""),
    )


def run_shortlist_validation(
    *,
    db_path: Path,
    candidate_names: list[str],
    config: ValidationConfig,
) -> dict[str, object]:
    normalized_names = [str(name).strip() for name in candidate_names if str(name).strip()]
    fingerprint = shortlist_fingerprint(names=normalized_names, config=config)
    with db.open_db(db_path) as conn:
        db.ensure_schema(conn)
        if detect_state_mismatch(conn, fingerprint=fingerprint):
            raise RuntimeError("validation_state_mismatch")
        run_id, created_new = prepare_shortlist_run(
            conn,
            candidate_names=normalized_names,
            config=replace(config, parallel_workers=1),
            fingerprint=fingerprint,
        )
        queue_summary = run_validation_jobs(
            conn,
            run_id=run_id,
            config=replace(config, parallel_workers=1),
            mark_run_complete=True,
        )
        validation_status_counts: Counter[str] = Counter()
        validation_check_counts: Counter[str] = Counter()
        for row in db.fetch_results_for_run(conn, run_id=run_id):
            validation_status_counts[str(row["status"])] += 1
            validation_check_counts[str(row["result_key"])] += 1
        return {
            "run_id": run_id,
            "fingerprint": fingerprint,
            "created_new": created_new,
            "job_counts": dict(queue_summary["job_counts"]),
            "validation_status_counts": dict(sorted(validation_status_counts.items())),
            "validation_check_counts": dict(sorted(validation_check_counts.items())),
        }


def _refresh_run_metrics_after_recheck(
    *,
    conn,
    run_row,
    export_top_n: int,
) -> tuple[dict[str, object], Path | None]:
    current_metrics = _load_json_dict(run_row["metrics_json"], context=f"run:{int(run_row['id'])}:metrics_json")
    result_rows = db.fetch_results_for_run(conn, run_id=int(run_row["id"]))
    validation_status_counts: Counter[str] = Counter()
    validation_check_counts: Counter[str] = Counter()
    for row in result_rows:
        if str(row["result_key"]) == "attractiveness":
            continue
        validation_status_counts[str(row["status"])] += 1
        validation_check_counts[str(row["result_key"])] += 1

    ranked_rows = db.fetch_ranked_rows(conn, run_id=int(run_row["id"]), limit=9999)
    decision_counts: Counter[str] = Counter(str(row["decision"]) for row in ranked_rows)
    top_names = [str(row["display_name"] or row["name"]) for row in ranked_rows[:5]]
    export_path_raw = str(current_metrics.get("export_path") or "").strip()
    export_path = Path(export_path_raw).resolve() if export_path_raw else None
    counts = dict(current_metrics.get("counts") or {})
    counts["validation_results"] = int(sum(validation_status_counts.values()))
    counts["ranked_candidates"] = int(db.count_ranked_rows(conn, run_id=int(run_row["id"])))
    counts["export_rows"] = min(counts["ranked_candidates"], max(1, int(export_top_n))) if export_path is not None else 0

    refreshed = {
        **current_metrics,
        "counts": counts,
        "validation_status_counts": dict(sorted(validation_status_counts.items())),
        "validation_check_counts": dict(sorted(validation_check_counts.items())),
        "decision_counts": dict(sorted(decision_counts.items())),
        "top_names": top_names,
        "export_path": str(export_path) if export_path is not None else "",
    }
    return refreshed, export_path


def rerank_run(conn, *, run_id: int):
    candidate_rows = db.list_candidates(conn, run_id=run_id)
    candidate_lookup = {str(row["display_name"] or row["name"]): int(row["id"]) for row in candidate_rows}
    run_row = db.get_run(conn, run_id=run_id)
    config_payload = _load_json_dict(run_row["config_json"], context=f"run:{run_id}:config_json") if run_row is not None else {}
    ideation_payload = config_payload.get("ideation") or {}
    naming_policy = build_naming_policy(ideation_payload.get("naming_policy"))
    for candidate_name, candidate_id in candidate_lookup.items():
        attractiveness = build_attractiveness_result(candidate_name, policy=naming_policy)
        db.upsert_result(
            conn,
            candidate_id=candidate_id,
            result_key=attractiveness.check_name,
            status=attractiveness.status.value,
            score_delta=attractiveness.score_delta,
            reason=attractiveness.reason,
            details=attractiveness.details,
        )
    grouped_rows: list[tuple[str, CandidateResult]] = []
    for row in db.fetch_results_for_run(conn, run_id=run_id):
        grouped_rows.append((str(row["display_name"] or row["name"]), _candidate_results_from_rows([row])[0]))
    grouped_results = group_results(grouped_rows)
    rankings = rank_candidate_surfaces(
        candidates=[dict(row) for row in candidate_rows],
        results_by_name=grouped_results,
        min_per_family=_cfg_int(ideation_payload.get("late_fusion_min_per_family"), 1, minimum=0),
        policy=naming_policy,
    )
    db.upsert_rankings(
        conn,
        rows=[
            (
                candidate_lookup[item.name],
                item.total_score,
                item.family_score,
                item.family_rank,
                item.rank_position,
                item.blocker_count,
                item.unavailable_count,
                item.unsupported_count,
                item.warning_count,
                item.decision,
            )
            for item in rankings
            if item.name in candidate_lookup
        ],
    )
    return rankings


def recheck_pending_web(
    *,
    db_path: Path,
    run_id: int | None = None,
    batch_id: str = "",
    limit: int = 100,
    rewrite_exports: bool = True,
    browser_profile_dir: Path | None = None,
    browser_chrome_executable: Path | None = None,
) -> dict[str, object]:
    with db.open_db(db_path) as conn:
        db.ensure_schema(conn)
        pending_rows = db.fetch_pending_web_rows(conn, run_id=run_id, batch_id=batch_id, limit=limit)
        by_run: dict[int, list[object]] = {}
        for row in pending_rows:
            by_run.setdefault(int(row["run_id"]), []).append(row)

        run_summaries: list[dict[str, object]] = []
        total_retried = 0
        for current_run_id, rows in sorted(by_run.items()):
            run_row = db.get_run(conn, run_id=current_run_id)
            if run_row is None:
                continue
            config_payload = _load_json_dict(run_row["config_json"], context=f"run:{current_run_id}:config_json")
            validation_config = _validation_config_from_payload((config_payload.get("validation") or {}))
            web_only_config = replace(
                validation_config,
                checks=["web"],
                web_search_order="serper,brave",
                web_browser_profile_dir=str(browser_profile_dir or validation_config.web_browser_profile_dir or ""),
                web_browser_chrome_executable=str(
                    browser_chrome_executable or validation_config.web_browser_chrome_executable or ""
                ),
                web_retry_attempts=0,
            )
            export_payload = config_payload.get("export") or {}
            export_top_n = _cfg_int(export_payload.get("top_n"), 25, minimum=1)

            before_rankings = {
                str(row["name"]): str(row["decision"])
                for row in db.fetch_ranked_rows(conn, run_id=current_run_id, limit=9999)
            }
            retried_names: list[str] = []
            for row in rows:
                candidate_id = int(row["candidate_id"])
                candidate_name = str(row["name"])
                web_results = validate_candidate(name=candidate_name, config=web_only_config)
                web_result = next((item for item in web_results if item.check_name == "web"), None)
                if web_result is None:
                    continue
                db.upsert_result(
                    conn,
                    candidate_id=candidate_id,
                    result_key=web_result.check_name,
                    status=web_result.status.value,
                    score_delta=web_result.score_delta,
                    reason=web_result.reason,
                    details=web_result.details,
                )
                retried_names.append(candidate_name)
            rankings = rerank_run(conn, run_id=current_run_id)
            refreshed_metrics, export_path = _refresh_run_metrics_after_recheck(
                conn=conn,
                run_row=run_row,
                export_top_n=export_top_n,
            )
            db.update_run_metrics(conn, run_id=current_run_id, metrics=refreshed_metrics)
            if rewrite_exports and export_path is not None:
                export_ranked_csv(conn=conn, run_id=current_run_id, out_path=export_path, limit=export_top_n)
            conn.commit()

            after_rankings = {item.name: item.decision for item in rankings}
            promoted_to_candidate = sum(
                1
                for name in retried_names
                if before_rankings.get(name) != "candidate" and after_rankings.get(name) == "candidate"
            )
            promoted_to_watch = sum(
                1
                for name in retried_names
                if before_rankings.get(name) not in {"watch", "candidate"} and after_rankings.get(name) == "watch"
            )
            blocked = sum(1 for name in retried_names if after_rankings.get(name) == "blocked")
            unchanged = sum(1 for name in retried_names if after_rankings.get(name) == before_rankings.get(name))
            pending_remaining = sum(
                1
                for row in db.fetch_pending_web_rows(conn, run_id=current_run_id, limit=9999)
                if str(row["name"]) in retried_names
            )
            run_summaries.append(
                {
                    "run_id": current_run_id,
                    "retried": len(retried_names),
                    "promoted_to_candidate": promoted_to_candidate,
                    "promoted_to_watch": promoted_to_watch,
                    "blocked": blocked,
                    "unchanged": unchanged,
                    "pending_remaining": pending_remaining,
                }
            )
            total_retried += len(retried_names)

        return {
            "retried": total_retried,
            "run_count": len(run_summaries),
            "runs": run_summaries,
        }


def _tmview_result_from_probe(probe_result) -> CandidateResult:
    details = {
        "source": str(probe_result.source or ""),
        "url": str(probe_result.url or ""),
        "query_ok": bool(probe_result.query_ok),
        "result_count": int(probe_result.result_count),
        "exact_hits": int(probe_result.exact_hits),
        "near_hits": int(probe_result.near_hits),
        "sample_text": str(probe_result.sample_text or ""),
        "exact_sample_text": str(probe_result.exact_sample_text or ""),
        "active_exact_hits": int(probe_result.active_exact_hits),
        "inactive_exact_hits": int(probe_result.inactive_exact_hits),
        "unknown_exact_hits": int(probe_result.unknown_exact_hits),
        "error": str(probe_result.error or ""),
    }
    if probe_result.query_ok and int(probe_result.exact_hits) > 0:
        return CandidateResult(
            check_name="tmview",
            status=ResultStatus.FAIL,
            score_delta=-35.0,
            reason="tmview_exact_collision",
            details=details,
        )
    if probe_result.query_ok and int(probe_result.near_hits) > 0:
        return CandidateResult(
            check_name="tmview",
            status=ResultStatus.FAIL,
            score_delta=-25.0,
            reason="tmview_near_collision",
            details=details,
        )
    if probe_result.query_ok:
        return CandidateResult(
            check_name="tmview",
            status=ResultStatus.PASS,
            score_delta=0.0,
            reason="tmview_clear",
            details=details,
        )
    return CandidateResult(
        check_name="tmview",
        status=ResultStatus.UNAVAILABLE,
        score_delta=0.0,
        reason="tmview_probe_unavailable",
        details=details,
    )


def recheck_tmview(
    *,
    db_path: Path,
    profile_dir: Path,
    chrome_executable: Path | None = None,
    nice_class: str = "",
    run_id: int | None = None,
    batch_id: str = "",
    limit: int = 25,
    rewrite_exports: bool = True,
    force: bool = False,
    headless: bool = True,
    timeout_ms: int = 20000,
    settle_ms: int = 2500,
) -> dict[str, object]:
    with db.open_db(db_path) as conn:
        db.ensure_schema(conn)
        target_rows = db.fetch_tmview_recheck_rows(
            conn,
            run_id=run_id,
            batch_id=batch_id,
            limit=limit,
            force=force,
        )
        by_run: dict[int, list[object]] = {}
        for row in target_rows:
            by_run.setdefault(int(row["run_id"]), []).append(row)

        run_summaries: list[dict[str, object]] = []
        total_retried = 0
        for current_run_id, rows in sorted(by_run.items()):
            run_row = db.get_run(conn, run_id=current_run_id)
            if run_row is None:
                continue
            export_payload = (_load_json_dict(run_row["config_json"], context=f"run:{current_run_id}:config_json").get("export") or {})
            export_top_n = _cfg_int(export_payload.get("top_n"), 25, minimum=1)

            before_rankings = {
                str(row["name"]): str(row["decision"])
                for row in db.fetch_ranked_rows(conn, run_id=current_run_id, limit=9999)
            }
            names = [str(row["name"]) for row in rows]
            probe_results = probe_tmview_names(
                names=names,
                profile_dir=profile_dir,
                chrome_executable=chrome_executable,
                nice_class=nice_class,
                timeout_ms=timeout_ms,
                settle_ms=settle_ms,
                headless=headless,
            )
            probe_lookup = {item.name: item for item in probe_results}
            retried_names: list[str] = []
            for row in rows:
                candidate_id = int(row["candidate_id"])
                candidate_name = str(row["name"])
                probe_result = probe_lookup.get(normalize_tmview_name(candidate_name))
                if probe_result is None:
                    continue
                tmview_result = _tmview_result_from_probe(probe_result)
                db.upsert_result(
                    conn,
                    candidate_id=candidate_id,
                    result_key=tmview_result.check_name,
                    status=tmview_result.status.value,
                    score_delta=tmview_result.score_delta,
                    reason=tmview_result.reason,
                    details=tmview_result.details,
                )
                retried_names.append(candidate_name)

            rankings = rerank_run(conn, run_id=current_run_id)
            refreshed_metrics, export_path = _refresh_run_metrics_after_recheck(
                conn=conn,
                run_row=run_row,
                export_top_n=export_top_n,
            )
            db.update_run_metrics(conn, run_id=current_run_id, metrics=refreshed_metrics)
            if rewrite_exports and export_path is not None:
                export_ranked_csv(conn=conn, run_id=current_run_id, out_path=export_path, limit=export_top_n)
            conn.commit()

            after_rankings = {item.name: item.decision for item in rankings}
            promoted_to_candidate = sum(
                1
                for name in retried_names
                if before_rankings.get(name) != "candidate" and after_rankings.get(name) == "candidate"
            )
            promoted_to_watch = sum(
                1
                for name in retried_names
                if before_rankings.get(name) not in {"watch", "candidate"} and after_rankings.get(name) == "watch"
            )
            blocked = sum(1 for name in retried_names if after_rankings.get(name) == "blocked")
            unchanged = sum(1 for name in retried_names if after_rankings.get(name) == before_rankings.get(name))
            run_summaries.append(
                {
                    "run_id": current_run_id,
                    "retried": len(retried_names),
                    "promoted_to_candidate": promoted_to_candidate,
                    "promoted_to_watch": promoted_to_watch,
                    "blocked": blocked,
                    "unchanged": unchanged,
                }
            )
            total_retried += len(retried_names)

        return {
            "retried": total_retried,
            "run_count": len(run_summaries),
            "runs": run_summaries,
        }


def _augment_brief_with_recent_failures(conn, *, brief: Brief) -> tuple[Brief, dict[str, object]]:
    pattern_report = db.recent_blocked_patterns(conn)
    suffixes = [str(item).strip().lower() for item in pattern_report.get("suffixes") or [] if str(item).strip()]
    stems = [str(item).strip().lower() for item in pattern_report.get("stems") or [] if str(item).strip()]
    if not suffixes and not stems:
        return brief, {
            "applied": False,
            "suffixes": [],
            "stems": [],
            "run_ids": pattern_report.get("run_ids") or [],
            "blocked_names": pattern_report.get("blocked_names") or [],
        }

    feedback_bits: list[str] = []
    if suffixes:
        feedback_bits.append("avoid repeating crowded suffix families: " + ", ".join(f"-{suffix}" for suffix in suffixes))
    if stems:
        feedback_bits.append("avoid reusing recently blocked stems: " + ", ".join(stems))

    augmented = replace(
        brief,
        forbidden_directions=_merge_unique_strings(brief.forbidden_directions, suffixes, stems),
        notes=_combine_notes(brief.notes, " ".join(feedback_bits)),
    )
    return augmented, {
        "applied": True,
        "suffixes": suffixes,
        "stems": stems,
        "run_ids": pattern_report.get("run_ids") or [],
        "blocked_names": pattern_report.get("blocked_names") or [],
    }


HIGH_SIGNAL_AVOIDANCE_REASONS = (
    "tmview_exact_collision",
    "tmview_near_collision",
    "web_exact_collision",
    "web_first_hit_exact",
    "web_near_collision",
    "social_handle_crowded",
)


def _high_signal_avoidance_terms(avoidance_context: dict[str, object] | None) -> dict[str, tuple[str, ...]]:
    context = avoidance_context or {}
    reason_patterns = context.get("external_reason_patterns") or {}
    avoid_names: list[str] = []
    lead_hints: list[str] = []
    lead_skeletons: list[str] = []
    tail_hints: list[str] = []
    seen_names: set[str] = set()
    seen_leads: set[str] = set()
    seen_lead_skeletons: set[str] = set()
    seen_tails: set[str] = set()

    def _lead_skeleton(raw: str) -> str:
        letters = "".join(ch for ch in str(raw).strip().lower() if ch.isalpha())
        consonants = "".join(ch for ch in letters[:6] if ch not in "aeiouy")
        return consonants[:3]

    for reason in HIGH_SIGNAL_AVOIDANCE_REASONS:
        payload = reason_patterns.get(reason) if isinstance(reason_patterns, dict) else None
        if not isinstance(payload, dict):
            continue
        for raw in payload.get("examples") or []:
            value = str(raw).strip()
            if not value:
                continue
            normalized = value.casefold()
            if normalized in seen_names:
                continue
            seen_names.add(normalized)
            avoid_names.append(value)
            if len(avoid_names) >= 12:
                break
        for raw in payload.get("lead_hints") or []:
            value = str(raw).strip().lower()
            if len(value) < 4 or value in seen_leads:
                continue
            seen_leads.add(value)
            lead_hints.append(value)
            if len(lead_hints) >= 8:
                break
            skeleton = _lead_skeleton(value)
            if len(skeleton) >= 2 and skeleton not in seen_lead_skeletons:
                seen_lead_skeletons.add(skeleton)
                lead_skeletons.append(skeleton)
        for raw in payload.get("tail_hints") or []:
            value = str(raw).strip().lower()
            if len(value) < 3 or value in seen_tails:
                continue
            seen_tails.add(value)
            tail_hints.append(value)
            if len(tail_hints) >= 8:
                break
        for raw in payload.get("examples") or []:
            skeleton = _lead_skeleton(str(raw).strip())
            if len(skeleton) >= 2 and skeleton not in seen_lead_skeletons:
                seen_lead_skeletons.add(skeleton)
                lead_skeletons.append(skeleton)
                if len(lead_skeletons) >= 8:
                    break
    if not avoid_names:
        for raw in context.get("external_avoid_names") or []:
            value = str(raw).strip()
            if not value:
                continue
            normalized = value.casefold()
            if normalized in seen_names:
                continue
            seen_names.add(normalized)
            avoid_names.append(value)
            if len(avoid_names) >= 12:
                break
    return {
        "avoid_names": tuple(avoid_names),
        "lead_hints": tuple(lead_hints),
        "lead_skeletons": tuple(lead_skeletons),
        "tail_hints": tuple(tail_hints),
    }


def _ordered_unique_tokens(values: object, *, minimum_length: int = 1) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in values if isinstance(values, (list, tuple)) else ():
        token = str(raw or "").strip().lower()
        if len(token) < minimum_length or token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return tuple(ordered)


def _surface_candidate_key(candidate: SurfacedCandidate) -> str:
    normalized = str(candidate.name_normalized or "").strip()
    if normalized:
        return normalized
    return normalize_brand_token(candidate.display_name)


def _surface_filter_inputs(avoidance_context: dict[str, object] | None) -> dict[str, tuple[str, ...]]:
    context = avoidance_context or {}
    high_signal = _high_signal_avoidance_terms(avoidance_context)
    local_patterns = context.get("local_patterns") if isinstance(context.get("local_patterns"), dict) else {}
    taste_fragments = _ordered_unique_tokens(
        [
            *(context.get("external_fragment_hints") or []),
            *(context.get("external_tail_hints") or []),
            *(context.get("external_avoid_names") or []),
            *high_signal["avoid_names"],
            *high_signal["lead_hints"],
            *high_signal["tail_hints"],
        ],
        minimum_length=3,
    )
    return {
        "taste_fragments": taste_fragments,
        "lead_fragments": _ordered_unique_tokens(
            [
                *(local_patterns.get("prefixes") or []),
                *(context.get("external_lead_hints") or []),
                *high_signal["lead_hints"],
            ],
            minimum_length=4,
        ),
        "lead_skeletons": _ordered_unique_tokens(high_signal["lead_skeletons"], minimum_length=2),
        "tail_fragments": _ordered_unique_tokens(
            [
                *(local_patterns.get("suffixes") or []),
                *(context.get("external_tail_hints") or []),
                *high_signal["tail_hints"],
            ],
            minimum_length=3,
        ),
        "crowded_terminal_families": _ordered_unique_tokens(
            context.get("external_terminal_families") or [],
            minimum_length=2,
        ),
        "crowded_terminal_skeletons": _ordered_unique_tokens(
            context.get("external_terminal_skeletons") or [],
            minimum_length=2,
        ),
    }


def _filter_surfaced_candidates(
    *,
    conn,
    brief: Brief,
    config: RunConfig,
    surfaced_candidates: list[SurfacedCandidate],
    avoidance_context: dict[str, object] | None,
    batch_id: str,
) -> tuple[list[SurfacedCandidate], dict[str, object]]:
    if not surfaced_candidates:
        return [], {
            "surface_candidate_count": 0,
            "surface_family_counts": {},
            "family_counts": {},
            "taste_filter": {"input_count": 0, "kept": 0, "dropped": {}, "examples": {}},
            "local_filter": {
                "input_count": 0,
                "kept": 0,
                "dropped": {},
                "dropped_examples": {},
            },
            "candidate_count": 0,
        }

    candidate_by_name: dict[str, SurfacedCandidate] = {}
    surfaced_names: list[str] = []
    surface_family_counts: Counter[str] = Counter()
    for candidate in surfaced_candidates:
        key = _surface_candidate_key(candidate)
        if not key or key in candidate_by_name:
            continue
        candidate_by_name[key] = candidate
        surfaced_names.append(key)
        surface_family_counts[candidate.family.value] += 1

    lexicon_bundle, _lexicon_report = build_lexicon(brief)
    filter_inputs = _surface_filter_inputs(avoidance_context)
    blocked_fragments = build_blocked_fragments(
        lexicon_bundle,
        extra_fragments=filter_inputs["taste_fragments"],
        policy=config.ideation.naming_policy,
    )
    taste_names, taste_report = filter_taste_names(
        surfaced_names,
        blocked_fragments=blocked_fragments,
        policy=config.ideation.naming_policy,
    )
    local_report: dict[str, object] = {
        "input_count": len(taste_names),
        "kept": 0,
        "dropped": {},
        "dropped_examples": {},
    }
    filtered_names = taste_names
    if filtered_names:
        filtered_names, local_report = filter_local_collisions(
            filtered_names,
            recent_corpus=db.recent_ranked_name_corpus(conn, exclude_batch_id=batch_id),
            avoid_lead_fragments=filter_inputs["lead_fragments"],
            avoid_lead_skeletons=filter_inputs["lead_skeletons"],
            avoid_tail_fragments=filter_inputs["tail_fragments"],
            crowded_terminal_families=filter_inputs["crowded_terminal_families"],
            crowded_terminal_skeletons=filter_inputs["crowded_terminal_skeletons"],
            policy=config.ideation.naming_policy,
        )

    filtered_candidates = [candidate_by_name[name] for name in filtered_names if name in candidate_by_name]
    filtered_family_counts = Counter(candidate.family.value for candidate in filtered_candidates)
    return filtered_candidates, {
        "surface_candidate_count": len(surfaced_names),
        "surface_family_counts": dict(sorted(surface_family_counts.items())),
        "family_counts": dict(sorted(filtered_family_counts.items())),
        "taste_filter": taste_report,
        "local_filter": local_report,
        "candidate_count": len(filtered_candidates),
    }


def _build_run_metrics(
    *,
    config: RunConfig,
    batch_id: str,
    batch_index: int | None,
    ideation_candidate_count: int,
    ideation_report: dict[str, object],
    validation_status_counts: Counter[str],
    validation_check_counts: Counter[str],
    rankings: list,
    durations_ms: dict[str, int],
    export_path: Path | None,
) -> dict[str, object]:
    decision_counts = Counter(item.decision for item in rankings)
    top_names = [item.display_name or item.name for item in rankings[:5]]
    return {
        "version": 1,
        "batch_id": batch_id,
        "batch_index": batch_index,
        "counts": {
            "pseudoword_seeds": int(((ideation_report.get("pseudoword") or {}).get("generated_count") or 0)),
            "seed_pool_total": int(((ideation_report.get("seed_pool") or {}).get("total") or 0)),
            "taste_filter_passed": int(((ideation_report.get("taste_filter") or {}).get("kept") or 0)),
            "local_filter_passed": int(((ideation_report.get("local_filter") or {}).get("kept") or 0)),
            "ideation_candidates": int(ideation_report.get("candidate_count") or ideation_candidate_count),
            "validation_results": int(sum(validation_status_counts.values())),
            "ranked_candidates": len(rankings),
            "export_rows": min(len(rankings), max(1, int(config.export.top_n))) if export_path is not None else 0,
        },
        "durations_ms": durations_ms,
        "validation_status_counts": dict(sorted(validation_status_counts.items())),
        "validation_check_counts": dict(sorted(validation_check_counts.items())),
        "decision_counts": dict(sorted(decision_counts.items())),
        "ideation": {
            "cost_usd": float(ideation_report.get("cost_usd") or 0.0),
            "seed_diversity": ideation_report.get("seed_diversity") or {},
            "name_diversity": ideation_report.get("name_diversity") or {},
            "surface_candidate_count": int(ideation_report.get("surface_candidate_count") or 0),
            "surface_family_counts": ideation_report.get("surface_family_counts") or {},
            "family_reports": ideation_report.get("family_reports") or {},
            "family_counts": ideation_report.get("family_counts") or {},
            "feedback": ideation_report.get("feedback") or {},
            "success_context": ideation_report.get("success_context") or {},
            "avoidance_context": ideation_report.get("avoidance_context") or {},
            "taste_filter": ideation_report.get("taste_filter") or {},
            "local_filter": ideation_report.get("local_filter") or {},
            "roles": ideation_report.get("roles") or [],
        },
        "top_names": top_names,
        "export_path": str(export_path) if export_path is not None else "",
    }


def run_loaded_config(
    config: RunConfig,
    *,
    config_path: Path | None = None,
    batch_id: str = "",
    batch_index: int | None = None,
) -> int:
    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    with db.open_db(config.db_path) as conn:
        db.ensure_schema(conn)
        effective_brief, feedback_report = _augment_brief_with_recent_failures(conn, brief=config.brief)
        effective_config = replace(config, brief=effective_brief)
        # Pipeline code owns commit boundaries. Database helpers must stay commit-free.
        run_id = db.create_run(
            conn,
            title=effective_config.title,
            brief=_serialize_value(effective_config.brief),
            config=_serialize_run_config(effective_config),
            batch_id=batch_id,
            batch_index=batch_index,
        )
        conn.commit()

        metrics: dict[str, object] = {
            "version": 1,
            "batch_id": batch_id,
            "batch_index": batch_index,
            "counts": {},
            "durations_ms": {},
            "validation_status_counts": {},
            "validation_check_counts": {},
            "decision_counts": {},
            "ideation": {},
            "top_names": [],
            "export_path": "",
        }
        total_started = time.perf_counter()
        try:
            db.set_run_state(conn, run_id=run_id, status=RunStatus.RUNNING.value, current_step="ideation")
            conn.commit()

            ideation_started = time.perf_counter()
            success_context = db.recent_positive_feedback(conn, exclude_batch_id=batch_id)
            avoidance_context = db.recent_avoidance_feedback(conn, exclude_batch_id=batch_id)
            ideation_candidate_count = 0
            surfaced_candidates, ideation_report = generate_candidate_surfaces(
                brief=effective_config.brief,
                config=replace(effective_config.ideation, family_mix_profile=DEFAULT_FAMILY_MIX_PROFILE),
                success_context=success_context,
                avoidance_context=avoidance_context,
            )
            ideation_report["feedback"] = feedback_report
            ideation_report["success_context"] = success_context
            ideation_report["avoidance_context"] = avoidance_context
            if not surfaced_candidates:
                raise RuntimeError("surface ideation produced no candidates")
            filtered_candidates, filter_report = _filter_surfaced_candidates(
                conn=conn,
                brief=effective_config.brief,
                config=effective_config,
                surfaced_candidates=surfaced_candidates,
                avoidance_context=avoidance_context,
                batch_id=batch_id,
            )
            ideation_report["surface_candidate_count"] = filter_report["surface_candidate_count"]
            ideation_report["surface_family_counts"] = filter_report["surface_family_counts"]
            ideation_report["family_counts"] = filter_report["family_counts"]
            ideation_report["taste_filter"] = filter_report["taste_filter"]
            ideation_report["local_filter"] = filter_report["local_filter"]
            ideation_report["candidate_count"] = int(filter_report["candidate_count"])
            family_reports = ideation_report.get("family_reports")
            if isinstance(family_reports, dict):
                for family_name, family_report in list(family_reports.items()):
                    if not isinstance(family_report, dict):
                        continue
                    family_reports[family_name] = {
                        **family_report,
                        "kept_after_pipeline_filters": int(filter_report["family_counts"].get(str(family_name), 0)),
                    }
            if not filtered_candidates:
                raise RuntimeError("surface filtering produced no candidates")
            db.add_candidate_surfaces(
                conn,
                run_id=run_id,
                candidates=filtered_candidates,
            )
            ideation_candidate_count = len(filtered_candidates)
            ideation_duration_ms = int((time.perf_counter() - ideation_started) * 1000)
            db.set_run_state(conn, run_id=run_id, status=RunStatus.RUNNING.value, current_step="validation")
            conn.commit()

            candidate_rows = db.list_candidates(conn, run_id=run_id)
            candidate_lookup: dict[str, int] = {}
            validation_status_counts: Counter[str] = Counter()
            validation_check_counts: Counter[str] = Counter()
            validation_started = time.perf_counter()
            for row in candidate_rows:
                name = row["display_name"] or row["name"]
                if name is None:
                    continue
                candidate_name = str(name).strip()
                if not candidate_name:
                    continue
                candidate_id = int(row["id"])
                candidate_lookup[candidate_name] = candidate_id
            db.ensure_validation_jobs(
                conn,
                run_id=run_id,
                ordered_candidate_ids=[int(row["id"]) for row in candidate_rows],
                shortlist_fingerprint=f"pipeline:{run_id}",
            )
            conn.commit()
            run_validation_jobs(
                conn,
                run_id=run_id,
                config=replace(config.validation, parallel_workers=1),
                mark_run_complete=False,
            )
            for row in db.fetch_results_for_run(conn, run_id=run_id):
                validation_status_counts[str(row["status"])] += 1
                validation_check_counts[str(row["result_key"])] += 1
            validation_duration_ms = int((time.perf_counter() - validation_started) * 1000)

            db.set_run_state(conn, run_id=run_id, status=RunStatus.RUNNING.value, current_step="ranking")
            ranking_started = time.perf_counter()
            for candidate_name, candidate_id in candidate_lookup.items():
                attractiveness = build_attractiveness_result(candidate_name, policy=effective_config.ideation.naming_policy)
                db.upsert_result(
                    conn,
                    candidate_id=candidate_id,
                    result_key=attractiveness.check_name,
                    status=attractiveness.status.value,
                    score_delta=attractiveness.score_delta,
                    reason=attractiveness.reason,
                    details=attractiveness.details,
                )
            persisted_results: list[tuple[str, CandidateResult]] = []
            for row in db.fetch_results_for_run(conn, run_id=run_id):
                persisted_results.append(
                    (
                        str(row["display_name"] or row["name"]).strip(),
                        CandidateResult(
                            check_name=str(row["result_key"]),
                            status=ResultStatus(str(row["status"])),
                            score_delta=float(row["score_delta"]),
                            reason=str(row["reason"]),
                            details=_load_json_dict(
                                row["details_json"],
                                context=f"run:{run_id}:result:{row['result_key']}",
                            ),
                        ),
                    )
                )
            grouped_results = group_results(persisted_results)
            rankings = rank_candidate_surfaces(
                candidates=[dict(row) for row in candidate_rows],
                results_by_name=grouped_results,
                min_per_family=max(0, int(effective_config.ideation.late_fusion_min_per_family)),
                policy=effective_config.ideation.naming_policy,
            )
            ranking_rows = []
            for ranking in rankings:
                candidate_id = candidate_lookup.get(ranking.name)
                if candidate_id is None:
                    continue
                ranking_rows.append(
                    (
                        candidate_id,
                        ranking.total_score,
                        ranking.family_score,
                        ranking.family_rank,
                        ranking.rank_position,
                        ranking.blocker_count,
                        ranking.unavailable_count,
                        ranking.unsupported_count,
                        ranking.warning_count,
                        ranking.decision,
                    )
                )
            db.upsert_rankings(conn, rows=ranking_rows)
            conn.commit()
            ranking_duration_ms = int((time.perf_counter() - ranking_started) * 1000)

            export_path = _format_export_path(config.export.out_csv, run_id=run_id)
            export_duration_ms = 0
            if export_path is not None:
                db.set_run_state(conn, run_id=run_id, status=RunStatus.RUNNING.value, current_step="export")
                conn.commit()
                export_started = time.perf_counter()
                export_ranked_csv(conn=conn, run_id=run_id, out_path=export_path, limit=config.export.top_n)
                export_duration_ms = int((time.perf_counter() - export_started) * 1000)

            metrics = _build_run_metrics(
                config=config,
                batch_id=batch_id,
                batch_index=batch_index,
                ideation_candidate_count=ideation_candidate_count,
                ideation_report=ideation_report,
                validation_status_counts=validation_status_counts,
                validation_check_counts=validation_check_counts,
                rankings=rankings,
                durations_ms={
                    "ideation": ideation_duration_ms,
                    "validation": validation_duration_ms,
                    "ranking": ranking_duration_ms,
                    "export": export_duration_ms,
                    "total": int((time.perf_counter() - total_started) * 1000),
                },
                export_path=export_path,
            )
            db.update_run_metrics(conn, run_id=run_id, metrics=metrics)

            db.set_run_state(conn, run_id=run_id, status=RunStatus.COMPLETED.value, current_step="complete", completed=True)
            conn.commit()
            print(f"run_id={run_id}")
            print(f"db={config.db_path}")
            if export_path is not None:
                print(f"export_csv={export_path}")
            return run_id
        except Exception as exc:
            try:
                # Validation results are committed incrementally; rollback only clears the current failed unit of work.
                conn.rollback()
                metrics["durations_ms"] = {
                    **dict(metrics.get("durations_ms") or {}),
                    "total": int((time.perf_counter() - total_started) * 1000),
                }
                db.update_run_metrics(conn, run_id=run_id, metrics=metrics)
                db.set_run_state(
                    conn,
                    run_id=run_id,
                    status=RunStatus.FAILED.value,
                    current_step="failed",
                    error_class=exc.__class__.__name__,
                    error_message=str(exc),
                    completed=True,
                )
                conn.commit()
            except Exception as state_exc:
                _warn_runtime_issue(
                    f"run:{run_id}:failed_to_persist_failure_state ({state_exc.__class__.__name__}: {state_exc})"
                )
            raise


def run_pipeline(config_path: Path) -> int:
    config = load_config(config_path)
    return run_loaded_config(config, config_path=config_path)
