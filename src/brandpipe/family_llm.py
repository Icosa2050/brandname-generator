from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Any

from .ideation import (
    ROLE_HINTS,
    _max_completion_tokens,
    _normalize_openai_compat_base_url,
    _normalize_openrouter_http_referer,
    _openrouter_reasoning_payload,
    _openrouter_response_modes,
    _post_json,
    _response_preview,
    _retry_delay_seconds,
    _temperature,
    extract_json_object,
    extract_response_content,
    load_prompt_template,
)
from .models import Brief, IdeationConfig, IdeationRoleConfig, NameFamily


ANTI_CORPORATE_TOKENS = (
    "solution",
    "solutions",
    "connect",
    "nexus",
    "core",
    "hub",
    "bridge",
    "sync",
    "flow",
    "cloud",
    "smart",
    "meta",
    "verse",
    "suite",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_prompt_paths() -> dict[NameFamily, Path]:
    base = _repo_root() / "resources" / "branding" / "llm"
    return {
        NameFamily.LITERAL_TLD_HACK: base / "brandpipe_family_literal_tld_hack_v1.txt",
        NameFamily.SMOOTH_BLEND: base / "brandpipe_family_smooth_blend_v2.txt",
        NameFamily.MASCOT_MUTATION: base / "brandpipe_family_mascot_mutation_v1.txt",
        NameFamily.CONTRARIAN_DICTIONARY: base / "brandpipe_family_contrarian_dictionary_v1.txt",
        NameFamily.BRUTALIST_UTILITY: base / "brandpipe_family_brutalist_utility_v1.txt",
    }


def _normalize_token(raw: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(raw or "").lower())


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
    text = str(raw_text or "").strip()
    if not text:
        return []
    try:
        payload: Any = json.loads(text)
    except json.JSONDecodeError:
        extracted = extract_json_object(text)
        if not extracted:
            payload = None
        else:
            try:
                payload = json.loads(extracted)
            except json.JSONDecodeError:
                payload = None

    source: list[Any] = []
    if isinstance(payload, dict):
        if isinstance(payload.get("candidates"), list):
            source = list(payload["candidates"])
        elif isinstance(payload.get("names"), list):
            source = list(payload["names"])
    elif isinstance(payload, list):
        source = list(payload)

    names: list[str] = []
    seen: set[str] = set()
    for item in source:
        raw_name: str | None = None
        if isinstance(item, str):
            raw_name = item
        elif isinstance(item, dict):
            for key in ("display_name", "name", "candidate"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    raw_name = value
                    break
        display_name = str(raw_name or "").strip()
        if not display_name:
            continue
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
    max_completion_tokens = _max_completion_tokens(model, target_count)
    base_body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": _temperature(temperature),
        "max_completion_tokens": max_completion_tokens,
    }
    reasoning = _openrouter_reasoning_payload(model)
    if reasoning is not None:
        base_body["reasoning"] = reasoning
    attempt_payloads: dict[str, dict[str, object]] = {
        "json_schema": {
            **base_body,
            "response_format": {"type": "json_schema", "json_schema": _surface_candidate_schema(strict_json)},
            "provider": {"require_parameters": True},
        },
        "json_object": {
            **base_body,
            "response_format": {"type": "json_object"},
        },
        "plain": dict(base_body),
    }
    attempts: list[tuple[str, dict[str, object]]] = [
        (mode, dict(attempt_payloads[mode]))
        for mode in _openrouter_response_modes(model)
    ]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    referer = _normalize_openrouter_http_referer(http_referer)
    if referer:
        headers["HTTP-Referer"] = referer
    if str(x_title or "").strip():
        headers["X-Title"] = str(x_title).strip()

    last_usage: dict[str, Any] = {}
    last_error = "unknown"
    for index, (response_mode, payload) in enumerate(attempts):
        response, error_code = _post_json(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            payload=payload,
            timeout_ms=timeout_ms,
        )
        if error_code:
            last_error = error_code
            if index + 1 < len(attempts) and error_code in {"http_400", "http_422", "http_429", "http_503", "http_504"}:
                if error_code in {"http_429", "http_503", "http_504"}:
                    import time

                    time.sleep(_retry_delay_seconds(index))
                continue
            last_usage["attempt_count"] = index + 1
            last_usage["response_mode"] = response_mode
            last_usage["max_completion_tokens"] = max_completion_tokens
            return [], last_usage, last_error
        if response is None:
            return [], last_usage, "unexpected_empty_response"
        content, usage, parse_error = extract_response_content(response)
        if usage:
            merged_usage = dict(last_usage)
            merged_usage.update(usage)
            last_usage = merged_usage
        last_usage["attempt_count"] = index + 1
        last_usage["response_mode"] = response_mode
        last_usage["max_completion_tokens"] = max_completion_tokens
        if content:
            last_usage["response_preview"] = _response_preview(content)
        if parse_error:
            return [], last_usage, parse_error
        names = _parse_surface_candidate_payload(content)
        if names:
            return names, last_usage, ""
        last_error = "candidate_parse_failed"
    return [], last_usage, last_error


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
    root = _normalize_openai_compat_base_url(base_url)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    token = str(api_key or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    base_body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": _temperature(temperature),
    }
    attempts: list[dict[str, object]] = [
        {
            **base_body,
            "response_format": {"type": "json_schema", "json_schema": _surface_candidate_schema(strict_json)},
        },
        {
            **base_body,
            "response_format": {"type": "json_object"},
        },
        dict(base_body),
    ]
    last_usage: dict[str, Any] = {}
    last_error = "unknown"
    for payload in attempts:
        response, error_code = _post_json(
            url=f"{root}/chat/completions",
            headers=headers,
            payload=payload,
            timeout_ms=timeout_ms,
        )
        if error_code:
            last_error = error_code
            if error_code in {"http_400", "http_422", "http_429", "http_503", "http_504"} and payload is not attempts[-1]:
                continue
            return [], last_usage, last_error
        if response is None:
            return [], last_usage, "unexpected_empty_response"
        content, usage, parse_error = extract_response_content(response)
        if usage:
            last_usage = usage
        if parse_error:
            return [], last_usage, parse_error
        names = _parse_surface_candidate_payload(content)
        if names:
            return names, usage, ""
        last_error = "candidate_parse_failed"
    return [], last_usage, last_error


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
) -> str:
    replacements = {
        "family_name": family.value,
        "target_count": str(max(1, int(target_count))),
        "role_name": role_cfg.role,
        "role_instructions": ROLE_HINTS.get(role_cfg.role, ROLE_HINTS["creative_divergence"]),
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


def _accept_candidate(family: NameFamily, display_name: str) -> tuple[bool, str]:
    display = str(display_name or "").strip()
    normalized = _normalize_token(display)
    lowered = normalized.lower()
    if len(normalized) < 4:
        return False, "too_short"
    if any(token in lowered for token in ANTI_CORPORATE_TOKENS):
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
        retry_feedback = _retry_feedback_block(rejected_examples, accepted)
        for role_cfg in role_cfgs:
            role_target = max(1, round(max(1, quota) * (max(1, int(role_cfg.weight)) / max(1, total_weight))))
            request_target = max(role_target, int(math.ceil(role_target * max(1.0, float(config.overgenerate_factor)))))
            prompt = _render_prompt(
                template=template,
                brief=brief,
                family=family,
                target_count=request_target,
                role_cfg=role_cfg,
                success_context=success_context,
                avoidance_context=avoidance_context,
                retry_feedback=retry_feedback,
            )
            raw_names, usage, err = _call_provider_for_family(
                provider=str(config.provider).strip().lower(),
                config=config,
                role_cfg=role_cfg,
                prompt=prompt,
                target_count=request_target,
            )
            if err:
                errors.append(f"{role_cfg.role}:{err}")
            accepted_now = 0
            rejected_now = 0
            for raw_name in raw_names:
                is_accepted, reason = _accept_candidate(family, raw_name)
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
                    "accepted": accepted_now,
                    "rejected": rejected_now,
                    "usage": usage,
                    "error": err,
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
