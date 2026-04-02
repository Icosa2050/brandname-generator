from __future__ import annotations

import math
import os
import re
from pathlib import Path
from typing import Any

from .ideation import (
    ROLE_HINTS,
    _call_openai_compat_candidates_with_schema,
    _call_openrouter_candidates_with_schema,
    extract_candidate_names,
    load_prompt_template,
)
from .models import Brief, IdeationConfig, IdeationRoleConfig, NameFamily
from .naming_policy import DEFAULT_NAMING_POLICY, NamingPolicy
from .name_normalization import normalize_brand_token


ANTI_CORPORATE_TOKENS = DEFAULT_NAMING_POLICY.surface.anti_corporate_tokens
RUNIC_FORGE_BAD_TAILS = DEFAULT_NAMING_POLICY.surface.runic_bad_tails


def _resolved_policy(policy: NamingPolicy | None) -> NamingPolicy:
    return policy or DEFAULT_NAMING_POLICY


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_prompt_paths() -> dict[NameFamily, Path]:
    base = _repo_root() / "resources" / "brandpipe" / "prompts"
    return {
        NameFamily.LITERAL_TLD_HACK: base / "literal_tld_hack.txt",
        NameFamily.SMOOTH_BLEND: base / "smooth_blend.txt",
        NameFamily.MASCOT_MUTATION: base / "mascot_mutation.txt",
        NameFamily.RUNIC_FORGE: base / "runic_forge.txt",
        NameFamily.CONTRARIAN_DICTIONARY: base / "contrarian_dictionary.txt",
        NameFamily.BRUTALIST_UTILITY: base / "brutalist_utility.txt",
    }


def _normalize_token(raw: str) -> str:
    return normalize_brand_token(raw)


def _role_configs(config: IdeationConfig) -> tuple[IdeationRoleConfig, ...]:
    if str(config.provider).strip().lower() == "fixture":
        return (
            IdeationRoleConfig(
                model="fixture",
                role="creative_divergence",
                temperature=float(config.temperature),
                weight=1,
            ),
        )
    if config.roles:
        return tuple(config.roles)
    if str(config.model or "").strip():
        return (
            IdeationRoleConfig(
                model=str(config.model).strip(),
                role="creative_divergence",
                temperature=float(config.temperature),
                weight=1,
            ),
        )
    return ()


def _surface_candidate_schema(strict_json: bool) -> dict[str, object]:
    return {
        "name": "surface_candidates",
        "strict": bool(strict_json),
        "schema": {
            "type": "object",
            "properties": {
                "candidates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"display_name": {"type": "string"}},
                        "required": ["display_name"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["candidates"],
            "additionalProperties": False,
        },
    }


def _parse_surface_candidate_payload(raw_text: str) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for display_name in extract_candidate_names(
        raw_text,
        candidate_keys=("display_name", "name", "candidate"),
    ):
        dedupe_key = display_name.casefold()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        names.append(display_name)
    return names


def _fixture_surface_candidates(path: Path) -> tuple[list[str], dict[str, Any], str]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return [], {}, "fixture_read_error"
    names = _parse_surface_candidate_payload(raw)
    if names:
        return names, {}, ""
    return [], {}, "candidate_parse_failed"


def _call_openrouter_surface_candidates(
    *,
    api_key: str,
    model: str,
    prompt: str,
    target_count: int,
    timeout_ms: int,
    strict_json: bool,
    temperature: float,
    http_referer: str,
    x_title: str,
) -> tuple[list[str], dict[str, Any], str]:
    return _call_openrouter_candidates_with_schema(
        api_key=api_key,
        model=model,
        prompt=prompt,
        target_count=target_count,
        timeout_ms=timeout_ms,
        strict_json=strict_json,
        temperature=temperature,
        http_referer=http_referer,
        x_title=x_title,
        schema_builder=_surface_candidate_schema,
        parse_candidates=_parse_surface_candidate_payload,
    )


def _call_openai_compat_surface_candidates(
    *,
    api_key: str,
    base_url: str,
    model: str,
    prompt: str,
    timeout_ms: int,
    strict_json: bool,
    temperature: float,
) -> tuple[list[str], dict[str, Any], str]:
    return _call_openai_compat_candidates_with_schema(
        api_key=api_key,
        base_url=base_url,
        model=model,
        prompt=prompt,
        timeout_ms=timeout_ms,
        strict_json=strict_json,
        temperature=temperature,
        schema_builder=_surface_candidate_schema,
        parse_candidates=_parse_surface_candidate_payload,
    )


def _call_provider_for_family(
    *,
    provider: str,
    config: IdeationConfig,
    role_cfg: IdeationRoleConfig,
    prompt: str,
    target_count: int,
) -> tuple[list[str], dict[str, Any], str]:
    if provider == "fixture":
        if not config.fixture_input:
            raise ValueError("fixture provider requires ideation.fixture_input")
        return _fixture_surface_candidates(config.fixture_input)
    if provider == "openrouter_http":
        api_key = str(os.getenv(config.api_key_env) or "").strip()
        if not api_key:
            raise RuntimeError(f"missing env {config.api_key_env}")
        return _call_openrouter_surface_candidates(
            api_key=api_key,
            model=role_cfg.model,
            prompt=prompt,
            target_count=target_count,
            timeout_ms=max(1000, int(config.timeout_ms)),
            strict_json=bool(config.strict_json),
            temperature=float(role_cfg.temperature),
            http_referer=str(os.getenv("OPENROUTER_HTTP_REFERER") or ""),
            x_title=str(os.getenv("OPENROUTER_X_TITLE") or ""),
        )
    if provider == "openai_compat":
        api_key = str(os.getenv(config.api_key_env) or "").strip() or "ollama"
        return _call_openai_compat_surface_candidates(
            api_key=api_key,
            base_url=config.openai_base_url,
            model=role_cfg.model,
            prompt=prompt,
            timeout_ms=max(1000, int(config.timeout_ms)),
            strict_json=bool(config.strict_json),
            temperature=float(role_cfg.temperature),
        )
    raise ValueError(f"unsupported ideation provider: {provider}")


def _positive_anchor_block(success_context: dict[str, object] | None) -> str:
    names = [
        str(value).strip()
        for value in ((success_context or {}).get("top_names") or [])
        if str(value).strip()
    ]
    if not names:
        return "none"
    return "Keep the divergence energy of these prior positives without echoing them: " + ", ".join(names[:6])


def _avoidance_block(avoidance_context: dict[str, object] | None) -> str:
    names = [
        str(value).strip()
        for value in ((avoidance_context or {}).get("external_avoid_names") or [])
        if str(value).strip()
    ]
    fragments = [
        str(value).strip()
        for value in ((avoidance_context or {}).get("external_fragment_hints") or [])
        if str(value).strip()
    ]
    parts: list[str] = []
    if names:
        parts.append("Avoid these recent names or very close neighbors: " + ", ".join(names[:8]))
    if fragments:
        parts.append("Move away from crowded fragments such as: " + ", ".join(fragments[:8]))
    return "\n".join(parts) if parts else "none"


def _retry_feedback_block(rejected_examples: list[str], accepted: list[str]) -> str:
    parts: list[str] = []
    if rejected_examples:
        parts.append("Do not repeat rejected directions such as: " + ", ".join(rejected_examples[:8]))
    if accepted:
        parts.append("Do not produce close spelling variants of already accepted names: " + ", ".join(accepted[:8]))
    return "\n".join(parts) if parts else "none"


def _render_prompt(
    *,
    template: str,
    brief: Brief,
    family: NameFamily,
    target_count: int,
    role_cfg: IdeationRoleConfig,
    success_context: dict[str, object] | None,
    avoidance_context: dict[str, object] | None,
    retry_feedback: str,
    policy: NamingPolicy | None = None,
) -> str:
    active_policy = _resolved_policy(policy)
    replacements = {
        "family_name": family.value,
        "target_count": str(max(1, int(target_count))),
        "role_name": role_cfg.role,
        "role_instructions": active_policy.prompts.role_hints.get(
            role_cfg.role,
            active_policy.prompts.role_hints.get("creative_divergence", ROLE_HINTS["creative_divergence"]),
        ),
        "product_core": str(brief.product_core or ""),
        "target_users": ", ".join(brief.target_users or []) or "none",
        "trust_signals": ", ".join(brief.trust_signals or []) or "none",
        "language_market": str(brief.language_market or "") or "global",
        "notes": str(brief.notes or "") or "none",
        "positive_anchor_block": _positive_anchor_block(success_context),
        "avoidance_block": _avoidance_block(avoidance_context),
        "retry_feedback_block": retry_feedback or "none",
    }
    prompt = str(template)
    for key, value in replacements.items():
        prompt = prompt.replace(f"{{{key}}}", value)
    return prompt.strip()


def _accept_candidate(
    family: NameFamily,
    display_name: str,
    *,
    policy: NamingPolicy | None = None,
) -> tuple[bool, str]:
    active_policy = _resolved_policy(policy)
    display = str(display_name or "").strip()
    normalized = _normalize_token(display)
    lowered = normalized.lower()
    if len(normalized) < 4:
        return False, "too_short"
    if any(token in lowered for token in active_policy.surface.anti_corporate_tokens):
        return False, "corporate_cliche"
    if family == NameFamily.LITERAL_TLD_HACK:
        if " " in display:
            return False, "contains_space"
        if "." not in display and "-" not in display:
            return False, "missing_namespace_marker"
        if not re.fullmatch(r"[A-Za-z0-9]+(?:[.-][A-Za-z0-9-]+)+", display):
            return False, "invalid_surface"
        return True, ""
    if family == NameFamily.SMOOTH_BLEND:
        if any(marker in display for marker in ".- "):
            return False, "unexpected_surface_marker"
        if not re.fullmatch(r"[a-z][a-z0-9]{5,13}", display):
            return False, "invalid_surface"
        return True, ""
    if family == NameFamily.MASCOT_MUTATION:
        if any(marker in display for marker in ".- "):
            return False, "unexpected_surface_marker"
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]{4,13}", display):
            return False, "invalid_surface"
        if sum(1 for ch in lowered if ch in "aeiou") < 2:
            return False, "too_harsh"
        return True, ""
    if family == NameFamily.RUNIC_FORGE:
        if any(marker in display for marker in ".- "):
            return False, "unexpected_surface_marker"
        if any(fragment in lowered for fragment in active_policy.surface.runic_crowded_patterns):
            return False, "crowded_neighbor_pattern"
        if not re.fullmatch(r"[A-Za-zÆØÅæøå]{6,9}", display):
            return False, "invalid_surface"
        upper = display.upper()
        if not any(marker in upper for marker in ("Æ", "Ø", "Å", "Y", "Q")):
            return False, "missing_structural_marker"
        structural_markers = sum(1 for ch in upper if ch in "ÆØÅYQ")
        if structural_markers > 2:
            return False, "too_many_disruptors"
        if "QU" in upper:
            return False, "english_q_cluster"
        if sum(1 for ch in upper if ch in "AEIOU") > 3:
            return False, "too_soft"
        if any(lowered.endswith(tail) for tail in active_policy.surface.runic_bad_tails) or lowered.endswith("x"):
            return False, "fantasy_sludge_tail"
        return True, ""
    if family == NameFamily.CONTRARIAN_DICTIONARY:
        if any(marker in display for marker in ".- "):
            return False, "unexpected_surface_marker"
        if not re.fullmatch(r"[A-Za-z]{5,14}", display):
            return False, "invalid_surface"
        return True, ""
    if family == NameFamily.BRUTALIST_UTILITY:
        if "." in display:
            return False, "unexpected_dot"
        if " " not in display and not re.search(r"[A-Z0-9]{2,4}$", display):
            return False, "missing_utility_suffix"
        if len(display.split()) > 3:
            return False, "too_many_tokens"
        return True, ""
    return False, "unsupported_family"


def _resolve_prompt_path(config: IdeationConfig, family: NameFamily) -> Path:
    override = config.family_prompt_template_files.get(family.value) if config.family_prompt_template_files else None
    if override:
        return Path(override)
    return _default_prompt_paths()[family]


def generate_family_candidates(
    *,
    family: NameFamily,
    brief: Brief,
    config: IdeationConfig,
    quota: int,
    success_context: dict[str, object] | None = None,
    avoidance_context: dict[str, object] | None = None,
) -> tuple[list[str], dict[str, object]]:
    if quota <= 0:
        return [], {"family": family.value, "accepted": 0, "attempts": 0}
    role_cfgs = _role_configs(config)
    if not role_cfgs:
        raise ValueError("ideation.model or ideation.roles is required for family-native generation")
    template_path = _resolve_prompt_path(config, family)
    template = load_prompt_template(template_path)
    if not template:
        raise RuntimeError(f"family prompt template missing: {template_path}")
    total_weight = sum(max(1, int(role.weight)) for role in role_cfgs)
    accepted: list[str] = []
    accepted_seen: set[str] = set()
    rejected_examples: list[str] = []
    errors: list[str] = []
    role_reports: list[dict[str, object]] = []
    retry_limit = max(0, int(config.family_llm_retry_limit))
    attempts = 0
    while len(accepted) < quota and attempts <= retry_limit:
        attempts += 1
        for role_cfg in role_cfgs:
            role_target = max(1, round(max(1, quota) * (max(1, int(role_cfg.weight)) / max(1, total_weight))))
            request_target = max(role_target, int(math.ceil(role_target * max(1.0, float(config.overgenerate_factor)))))
            request_batches = max(1, quota - len(accepted)) if family == NameFamily.RUNIC_FORGE else 1
            accepted_now = 0
            rejected_now = 0
            usage_reports: list[dict[str, Any]] = []
            role_errors: list[str] = []
            calls_made = 0
            for _ in range(request_batches):
                if len(accepted) >= quota:
                    break
                retry_feedback = _retry_feedback_block(rejected_examples, accepted)
                active_target = 1 if family == NameFamily.RUNIC_FORGE else request_target
                prompt = _render_prompt(
                    template=template,
                    brief=brief,
                    family=family,
                    target_count=active_target,
                    role_cfg=role_cfg,
                    success_context=success_context,
                    avoidance_context=avoidance_context,
                    retry_feedback=retry_feedback,
                    policy=config.naming_policy,
                )
                raw_names, usage, err = _call_provider_for_family(
                    provider=str(config.provider).strip().lower(),
                    config=config,
                    role_cfg=role_cfg,
                    prompt=prompt,
                    target_count=active_target,
                )
                calls_made += 1
                if usage:
                    usage_reports.append(usage)
                if err:
                    errors.append(f"{role_cfg.role}:{err}")
                    role_errors.append(err)
                for raw_name in raw_names:
                    is_accepted, reason = _accept_candidate(family, raw_name, policy=config.naming_policy)
                    if not is_accepted:
                        rejected_now += 1
                        if raw_name not in rejected_examples:
                            rejected_examples.append(f"{raw_name} ({reason})")
                        continue
                    key = raw_name.casefold()
                    if key in accepted_seen:
                        continue
                    accepted_seen.add(key)
                    accepted.append(raw_name.strip())
                    accepted_now += 1
                    if len(accepted) >= quota:
                        break
            role_reports.append(
                {
                    "role": role_cfg.role,
                    "model": role_cfg.model,
                    "calls": calls_made,
                    "accepted": accepted_now,
                    "rejected": rejected_now,
                    "usage": usage_reports,
                    "error": ";".join(role_errors),
                }
            )
            if len(accepted) >= quota:
                break
        if len(accepted) >= quota:
            break
    return accepted[:quota], {
        "family": family.value,
        "provider": str(config.provider).strip().lower(),
        "accepted": len(accepted[:quota]),
        "attempts": attempts,
        "errors": errors,
        "prompt_template_file": str(template_path),
        "role_reports": role_reports,
        "rejected_examples": rejected_examples[:10],
    }
