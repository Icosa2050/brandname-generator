from __future__ import annotations

from collections import Counter
from dataclasses import replace
import json
import re

from .family_llm import generate_family_candidates
from .lexicon import build_lexicon
from .models import DEFAULT_FAMILY_MIX_PROFILE, Brief, IdeationConfig, NameFamily, SurfacePolicy, SurfacedCandidate
from .naming_policy import DEFAULT_NAMING_POLICY, NamingPolicy
from .name_normalization import normalize_brand_token
from .ranking import score_family_surface


FAMILY_ORDER: tuple[NameFamily, ...] = tuple(NameFamily(value) for value in DEFAULT_NAMING_POLICY.surface.family_order)
STOPWORDS = frozenset(DEFAULT_NAMING_POLICY.surface.stopwords)
TLD_SUFFIXES: tuple[str, ...] = DEFAULT_NAMING_POLICY.surface.tld_suffixes
DICTIONARY_WORDS: tuple[str, ...] = DEFAULT_NAMING_POLICY.surface.dictionary_words
MASCOT_BASES: tuple[str, ...] = DEFAULT_NAMING_POLICY.surface.mascot_bases
BRUTALIST_SUFFIXES: tuple[str, ...] = DEFAULT_NAMING_POLICY.surface.brutalist_suffixes
RUNIC_FALLBACKS: tuple[str, ...] = DEFAULT_NAMING_POLICY.surface.runic_fallbacks


def _resolved_policy(policy: NamingPolicy | None) -> NamingPolicy:
    return policy or DEFAULT_NAMING_POLICY


def _family_order(policy: NamingPolicy | None) -> tuple[NameFamily, ...]:
    active_policy = _resolved_policy(policy)
    ordered: list[NameFamily] = []
    for raw_name in active_policy.surface.family_order:
        try:
            family = NameFamily(str(raw_name))
        except ValueError:
            continue
        if family not in ordered:
            ordered.append(family)
    return tuple(ordered) or FAMILY_ORDER


def normalize_surface(raw: str) -> str:
    return str(raw or "").strip().casefold()


def normalize_comparison(raw: str) -> str:
    return normalize_brand_token(raw)


def infer_surface_policy(display_name: str) -> SurfacePolicy:
    value = str(display_name or "").strip()
    if "." in value:
        return SurfacePolicy.DOTTED_LOWER
    if "-" in value:
        return SurfacePolicy.HYPHENATED_LOWER
    if " " in value:
        return SurfacePolicy.TITLE_SPACED_ACRONYM
    if value != value.lower():
        return SurfacePolicy.MIXED_CASE_ALPHA
    return SurfacePolicy.ALPHA_LOWER


def _alpha_tokens(brief: Brief, *, policy: NamingPolicy | None = None) -> list[str]:
    active_policy = _resolved_policy(policy)
    stopwords = frozenset(active_policy.surface.stopwords)
    raw = " ".join(
        [
            str(brief.product_core or ""),
            " ".join(brief.target_users or []),
            " ".join(brief.trust_signals or []),
            str(brief.notes or ""),
        ]
    )
    tokens: list[str] = []
    seen: set[str] = set()
    for token in re.findall(r"[A-Za-z]{3,18}", raw):
        lowered = token.lower()
        if lowered in stopwords:
            continue
        if lowered in seen:
            continue
        seen.add(lowered)
        tokens.append(lowered)
    return tokens


def _title_case(token: str) -> str:
    lowered = str(token or "").strip().lower()
    if not lowered:
        return ""
    return lowered[:1].upper() + lowered[1:]


def _root_pool(brief: Brief, *, policy: NamingPolicy | None = None) -> list[str]:
    active_policy = _resolved_policy(policy)
    lexicon, _report = build_lexicon(brief)
    raw = [
        *_alpha_tokens(brief, policy=active_policy),
        *lexicon.core_terms,
        *lexicon.modifiers,
        *lexicon.associative_terms,
        *lexicon.morphemes,
    ]
    roots: list[str] = []
    seen: set[str] = set()
    for token in raw:
        lowered = normalize_comparison(str(token))
        if len(lowered) < 4 or len(lowered) > 12:
            continue
        if lowered in set(active_policy.surface.stopwords) or lowered in seen:
            continue
        seen.add(lowered)
        roots.append(lowered)
    return roots


def _family_quota_map(config: IdeationConfig) -> dict[NameFamily, int]:
    ordered_families = _family_order(config.naming_policy)
    total_target = max(5, int(config.rounds) * int(config.candidates_per_round))
    requested: dict[NameFamily, int] = {}
    for family in ordered_families:
        raw = config.family_quotas.get(family.value, 0) if isinstance(config.family_quotas, dict) else 0
        if raw:
            requested[family] = max(0, int(raw))
    if requested:
        return {family: max(0, requested.get(family, 0)) for family in ordered_families}
    base = total_target // len(ordered_families)
    remainder = total_target % len(ordered_families)
    quotas: dict[NameFamily, int] = {}
    for index, family in enumerate(ordered_families):
        quotas[family] = base + (1 if index < remainder else 0)
    return quotas


def _candidate(
    display_name: str,
    family: NameFamily,
    *,
    source_kind: str,
    source_detail: dict[str, object],
    policy: NamingPolicy | None = None,
) -> SurfacedCandidate | None:
    display = str(display_name or "").strip()
    normalized = normalize_comparison(display)
    if not display or len(normalized) < 3:
        return None
    surface_policy = infer_surface_policy(display)
    return SurfacedCandidate(
        display_name=display,
        name_normalized=normalized,
        family=family,
        surface_policy=surface_policy,
        source_kind=source_kind,
        source_detail=json.dumps(source_detail, ensure_ascii=False, sort_keys=True),
        family_score=score_family_surface(
            display_name=display,
            family=family,
            surface_policy=surface_policy,
            policy=policy,
        ),
    )


def _dedupe_candidates(candidates: list[SurfacedCandidate]) -> list[SurfacedCandidate]:
    seen: set[str] = set()
    deduped: list[SurfacedCandidate] = []
    for candidate in candidates:
        key = normalize_surface(candidate.display_name)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _generate_literal_tld_hack_family(brief: Brief, quota: int, *, policy: NamingPolicy | None = None) -> list[SurfacedCandidate]:
    active_policy = _resolved_policy(policy)
    roots = _root_pool(brief, policy=active_policy)
    generated: list[SurfacedCandidate] = []
    for root in roots:
        for suffix in active_policy.surface.tld_suffixes:
            candidate = _candidate(
                f"{root}{suffix}",
                NameFamily.LITERAL_TLD_HACK,
                source_kind="family_lane_deterministic",
                source_detail={"family": NameFamily.LITERAL_TLD_HACK.value, "root": root, "suffix": suffix},
                policy=active_policy,
            )
            if candidate is not None:
                generated.append(candidate)
            if len(generated) >= max(quota * 3, quota + 4):
                return _dedupe_candidates(generated)
    return _dedupe_candidates(generated)


def _blend_halves(left: str, right: str) -> str:
    left_token = normalize_comparison(left)
    right_token = normalize_comparison(right)
    if not left_token or not right_token or left_token == right_token:
        return ""
    left_cut = max(2, min(len(left_token) - 1, (len(left_token) + 1) // 2))
    right_cut = min(len(right_token) - 2, max(1, len(right_token) // 2))
    if left_cut >= len(left_token) or right_cut <= 0 or right_cut >= len(right_token):
        return ""
    return left_token[:left_cut] + right_token[right_cut:]


def _generate_smooth_blend_family(
    *,
    brief: Brief,
    quota: int,
    policy: NamingPolicy | None = None,
) -> list[SurfacedCandidate]:
    if quota <= 0:
        return []
    roots = _root_pool(brief, policy=policy)
    generated: list[SurfacedCandidate] = []
    seen_variants: set[str] = set()
    for index, left in enumerate(roots):
        for right in roots[index + 1 :]:
            for candidate_name in (_blend_halves(left, right), _blend_halves(right, left)):
                normalized = normalize_comparison(candidate_name)
                if len(normalized) < 4 or len(normalized) > 12 or normalized in seen_variants:
                    continue
                seen_variants.add(normalized)
                candidate = _candidate(
                    candidate_name,
                    NameFamily.SMOOTH_BLEND,
                    source_kind="family_lane_deterministic",
                    source_detail={
                        "family": NameFamily.SMOOTH_BLEND.value,
                        "left": left,
                        "right": right,
                        "strategy": "deterministic_blend",
                    },
                    policy=policy,
                )
                if candidate is not None:
                    generated.append(candidate)
                if len(generated) >= max(quota * 3, quota + 4):
                    return _dedupe_candidates(generated)
    if not generated:
        for root in roots:
            candidate = _candidate(
                _title_case(root),
                NameFamily.SMOOTH_BLEND,
                source_kind="family_lane_deterministic",
                source_detail={
                    "family": NameFamily.SMOOTH_BLEND.value,
                    "root": root,
                    "strategy": "root_fallback",
                },
                policy=policy,
            )
            if candidate is not None:
                generated.append(candidate)
            if len(generated) >= max(quota, 1):
                break
    surfaced: list[SurfacedCandidate] = []
    for candidate in generated:
        surfaced.append(candidate)
        if len(surfaced) >= quota:
            break
    return _dedupe_candidates(surfaced)


def _mascot_variants(base: str) -> list[str]:
    lowered = str(base or "").strip().lower()
    if not lowered:
        return []
    variants = [
        _title_case(lowered),
        _title_case(f"o{lowered}"),
        _title_case(f"{lowered}o"),
        _title_case(f"{lowered}a"),
    ]
    if len(lowered) >= 4:
        variants.append(_title_case(lowered[:2] + lowered))
    return variants


def _generate_mascot_mutation_family(brief: Brief, quota: int, *, policy: NamingPolicy | None = None) -> list[SurfacedCandidate]:
    del brief
    active_policy = _resolved_policy(policy)
    generated: list[SurfacedCandidate] = []
    for base in active_policy.surface.mascot_bases:
        for variant in _mascot_variants(base):
            candidate = _candidate(
                variant,
                NameFamily.MASCOT_MUTATION,
                source_kind="family_lane_deterministic",
                source_detail={"family": NameFamily.MASCOT_MUTATION.value, "base": base},
                policy=active_policy,
            )
            if candidate is not None:
                generated.append(candidate)
            if len(generated) >= max(quota * 3, quota + 4):
                return _dedupe_candidates(generated)
    return _dedupe_candidates(generated)


def _generate_contrarian_dictionary_family(brief: Brief, quota: int, *, policy: NamingPolicy | None = None) -> list[SurfacedCandidate]:
    active_policy = _resolved_policy(policy)
    roots = _root_pool(brief, policy=active_policy)
    generated: list[SurfacedCandidate] = []
    pool = list(active_policy.surface.dictionary_words) + roots
    seen: set[str] = set()
    for raw in pool:
        normalized = normalize_comparison(raw)
        if len(normalized) < 5 or len(normalized) > 10:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        candidate = _candidate(
            normalized,
            NameFamily.CONTRARIAN_DICTIONARY,
            source_kind="family_lane_deterministic",
            source_detail={"family": NameFamily.CONTRARIAN_DICTIONARY.value, "source": raw},
            policy=active_policy,
        )
        if candidate is not None:
            generated.append(candidate)
        if len(generated) >= max(quota * 3, quota + 4):
            break
    return _dedupe_candidates(generated)


def _generate_runic_forge_family(quota: int, *, policy: NamingPolicy | None = None) -> list[SurfacedCandidate]:
    active_policy = _resolved_policy(policy)
    generated: list[SurfacedCandidate] = []
    for seed in active_policy.surface.runic_fallbacks:
        candidate = _candidate(
            seed,
            NameFamily.RUNIC_FORGE,
            source_kind="family_lane_deterministic",
            source_detail={"family": NameFamily.RUNIC_FORGE.value, "seed": seed},
            policy=active_policy,
        )
        if candidate is not None:
            generated.append(candidate)
        if len(generated) >= max(quota * 3, quota + 4):
            break
    return _dedupe_candidates(generated)


def _acronym_pool(brief: Brief, *, policy: NamingPolicy | None = None) -> list[str]:
    active_policy = _resolved_policy(policy)
    tokens = _alpha_tokens(brief, policy=active_policy)
    acronyms: list[str] = []
    seen: set[str] = set()
    if len(tokens) >= 2:
        joined = "".join(token[:1] for token in tokens[:3]).upper()
        if 2 <= len(joined) <= 4:
            acronyms.append(joined)
            seen.add(joined)
    for suffix in active_policy.surface.brutalist_suffixes:
        if suffix not in seen:
            acronyms.append(suffix)
            seen.add(suffix)
    return acronyms


def _generate_brutalist_utility_family(brief: Brief, quota: int, *, policy: NamingPolicy | None = None) -> list[SurfacedCandidate]:
    roots = _root_pool(brief, policy=policy)
    prefixes = [_title_case(root) for root in roots[:12] if root]
    acronyms = _acronym_pool(brief, policy=policy)
    generated: list[SurfacedCandidate] = []
    for prefix in prefixes:
        for suffix in acronyms:
            candidate = _candidate(
                f"{prefix} {suffix}",
                NameFamily.BRUTALIST_UTILITY,
                source_kind="family_lane_deterministic",
                source_detail={"family": NameFamily.BRUTALIST_UTILITY.value, "prefix": prefix, "suffix": suffix},
                policy=policy,
            )
            if candidate is not None:
                generated.append(candidate)
            if len(generated) >= max(quota * 3, quota + 4):
                return _dedupe_candidates(generated)
    return _dedupe_candidates(generated)


def _top_family_candidates(candidates: list[SurfacedCandidate], quota: int) -> list[SurfacedCandidate]:
    if quota <= 0:
        return []
    ranked = sorted(
        candidates,
        key=lambda candidate: (-candidate.family_score, candidate.display_name.casefold()),
    )
    return [
        replace(candidate, family_rank=index)
        for index, candidate in enumerate(ranked[:quota], start=1)
    ]


def _llm_family_candidates(
    *,
    family: NameFamily,
    brief: Brief,
    config: IdeationConfig,
    quota: int,
    success_context: dict[str, object] | None,
    avoidance_context: dict[str, object] | None,
) -> tuple[list[SurfacedCandidate], dict[str, object]]:
    display_names, report = generate_family_candidates(
        family=family,
        brief=brief,
        config=config,
        quota=quota,
        success_context=success_context,
        avoidance_context=avoidance_context,
    )
    surfaced: list[SurfacedCandidate] = []
    for display_name in display_names:
        candidate = _candidate(
            display_name,
            family,
            source_kind="family_lane_llm",
            source_detail={"family": family.value, "llm_report": report},
            policy=config.naming_policy,
        )
        if candidate is not None:
            surfaced.append(candidate)
    return _dedupe_candidates(surfaced), report


def generate_candidate_surfaces(
    *,
    brief: Brief,
    config: IdeationConfig,
    success_context: dict[str, object] | None = None,
    avoidance_context: dict[str, object] | None = None,
) -> tuple[list[SurfacedCandidate], dict[str, object]]:
    quotas = _family_quota_map(config)
    surfaced: list[SurfacedCandidate] = []
    family_reports: dict[str, object] = {}
    deterministic_generators = {
        NameFamily.LITERAL_TLD_HACK: lambda quota: _generate_literal_tld_hack_family(brief, quota, policy=config.naming_policy),
        NameFamily.SMOOTH_BLEND: lambda quota: _generate_smooth_blend_family(brief=brief, quota=quota, policy=config.naming_policy),
        NameFamily.MASCOT_MUTATION: lambda quota: _generate_mascot_mutation_family(brief, quota, policy=config.naming_policy),
        NameFamily.RUNIC_FORGE: lambda quota: _generate_runic_forge_family(quota, policy=config.naming_policy),
        NameFamily.CONTRARIAN_DICTIONARY: lambda quota: _generate_contrarian_dictionary_family(brief, quota, policy=config.naming_policy),
        NameFamily.BRUTALIST_UTILITY: lambda quota: _generate_brutalist_utility_family(brief, quota, policy=config.naming_policy),
    }
    family_counts: Counter[str] = Counter()
    for family in _family_order(config.naming_policy):
        quota = int(quotas.get(family, 0))
        attempts = 0
        family_candidates: list[SurfacedCandidate] = []
        family_report: dict[str, object] = {}
        fallback_count = 0
        llm_candidates, llm_report = _llm_family_candidates(
            family=family,
            brief=brief,
            config=replace(config, family_mix_profile=DEFAULT_FAMILY_MIX_PROFILE),
            quota=quota,
            success_context=success_context,
            avoidance_context=avoidance_context,
        )
        family_candidates = _top_family_candidates(llm_candidates, quota)
        attempts = int(llm_report.get("attempts") or 0)
        family_report["llm"] = llm_report
        if len(family_candidates) < quota:
            seen = {normalize_surface(candidate.display_name) for candidate in family_candidates}
            fallback_candidates = deterministic_generators[family](quota)
            for fallback_candidate in fallback_candidates:
                key = normalize_surface(fallback_candidate.display_name)
                if key in seen:
                    continue
                seen.add(key)
                family_candidates.append(fallback_candidate)
                fallback_count += 1
                if len(family_candidates) >= quota:
                    break
        surfaced.extend(family_candidates)
        family_counts[family.value] = len(family_candidates)
        family_reports[family.value] = {
            "quota": quota,
            "generated": len(family_candidates),
            "attempts": attempts,
            "fallback_count": fallback_count,
            "examples": [candidate.display_name for candidate in family_candidates[:5]],
            **family_report,
        }
    surfaced = _dedupe_candidates(surfaced)
    return surfaced, {
        "provider": config.provider,
        "family_mix_profile": DEFAULT_FAMILY_MIX_PROFILE,
        "family_quotas": {family.value: int(quotas.get(family, 0)) for family in _family_order(config.naming_policy)},
        "family_counts": dict(sorted(family_counts.items())),
        "family_reports": family_reports,
        "candidate_count": len(surfaced),
        "late_fusion_min_per_family": int(config.late_fusion_min_per_family),
    }
