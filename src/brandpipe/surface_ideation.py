from __future__ import annotations

from collections import Counter
from dataclasses import replace
import json
import re
import unicodedata

from .family_llm import generate_family_candidates
from .ideation import generate_candidates
from .lexicon import build_lexicon
from .models import Brief, IdeationConfig, NameFamily, SurfacePolicy, SurfacedCandidate


FAMILY_ORDER: tuple[NameFamily, ...] = (
    NameFamily.LITERAL_TLD_HACK,
    NameFamily.SMOOTH_BLEND,
    NameFamily.MASCOT_MUTATION,
    NameFamily.CONTRARIAN_DICTIONARY,
    NameFamily.BRUTALIST_UTILITY,
)
STOPWORDS = frozenset(
    {
        "and",
        "for",
        "the",
        "with",
        "from",
        "into",
        "your",
        "their",
        "software",
        "platform",
        "system",
        "utility",
        "manager",
        "managers",
        "cost",
    }
)
TLD_SUFFIXES: tuple[str, ...] = (".io", ".app", ".hq", ".cloud", "-hq", "-app", "-io")
DICTIONARY_WORDS: tuple[str, ...] = (
    "discord",
    "signal",
    "beacon",
    "vector",
    "anchor",
    "lattice",
    "rally",
    "harbor",
    "murmur",
    "parley",
    "temper",
    "fable",
    "orbit",
    "native",
    "current",
)
MASCOT_BASES: tuple[str, ...] = (
    "llama",
    "otter",
    "panda",
    "koala",
    "manta",
    "orca",
    "gecko",
    "yak",
    "lynx",
    "ibis",
)
BRUTALIST_SUFFIXES: tuple[str, ...] = ("TSX", "MP", "DX", "OS", "HQ", "RX")


def normalize_surface(raw: str) -> str:
    return str(raw or "").strip().casefold()


def normalize_comparison(raw: str) -> str:
    folded = unicodedata.normalize("NFKD", str(raw or ""))
    plain = "".join(ch for ch in folded if not unicodedata.combining(ch)).lower()
    return re.sub(r"[^a-z0-9]", "", plain)


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


def _alpha_tokens(brief: Brief) -> list[str]:
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
        if lowered in STOPWORDS:
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


def _root_pool(brief: Brief) -> list[str]:
    lexicon, _report = build_lexicon(brief)
    raw = [
        *_alpha_tokens(brief),
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
        if lowered in STOPWORDS or lowered in seen:
            continue
        seen.add(lowered)
        roots.append(lowered)
    return roots


def _family_quota_map(config: IdeationConfig) -> dict[NameFamily, int]:
    total_target = max(5, int(config.rounds) * int(config.candidates_per_round))
    requested: dict[NameFamily, int] = {}
    for family in FAMILY_ORDER:
        raw = config.family_quotas.get(family.value, 0) if isinstance(config.family_quotas, dict) else 0
        if raw:
            requested[family] = max(0, int(raw))
    if requested:
        return {family: max(0, requested.get(family, 0)) for family in FAMILY_ORDER}
    base = total_target // len(FAMILY_ORDER)
    remainder = total_target % len(FAMILY_ORDER)
    quotas: dict[NameFamily, int] = {}
    for index, family in enumerate(FAMILY_ORDER):
        quotas[family] = base + (1 if index < remainder else 0)
    return quotas


def _candidate(
    display_name: str,
    family: NameFamily,
    *,
    source_kind: str,
    source_detail: dict[str, object],
) -> SurfacedCandidate | None:
    display = str(display_name or "").strip()
    normalized = normalize_comparison(display)
    if not display or len(normalized) < 3:
        return None
    return SurfacedCandidate(
        display_name=display,
        name_normalized=normalized,
        family=family,
        surface_policy=infer_surface_policy(display),
        source_kind=source_kind,
        source_detail=json.dumps(source_detail, ensure_ascii=False, sort_keys=True),
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


def _generate_literal_tld_hack_family(brief: Brief, quota: int) -> list[SurfacedCandidate]:
    roots = _root_pool(brief)
    generated: list[SurfacedCandidate] = []
    for root in roots:
        for suffix in TLD_SUFFIXES:
            candidate = _candidate(
                f"{root}{suffix}",
                NameFamily.LITERAL_TLD_HACK,
                source_kind="family_lane_deterministic",
                source_detail={"family": NameFamily.LITERAL_TLD_HACK.value, "root": root, "suffix": suffix},
            )
            if candidate is not None:
                generated.append(candidate)
            if len(generated) >= max(quota * 3, quota + 4):
                return _dedupe_candidates(generated)
    return _dedupe_candidates(generated)


def _generate_smooth_blend_family(
    *,
    brief: Brief,
    config: IdeationConfig,
    quota: int,
    success_context: dict[str, object] | None,
    avoidance_context: dict[str, object] | None,
) -> list[SurfacedCandidate]:
    if quota <= 0:
        return []
    legacy_config = replace(
        config,
        family_mix_profile="legacy_alpha",
        family_quotas={},
        late_fusion_min_per_family=1,
        rounds=1,
        candidates_per_round=max(quota, int(config.candidates_per_round)),
    )
    names, report = generate_candidates(
        brief=brief,
        config=legacy_config,
        success_context=success_context,
        avoidance_context=avoidance_context,
    )
    surfaced: list[SurfacedCandidate] = []
    for name in names:
        candidate = _candidate(
            name,
            NameFamily.SMOOTH_BLEND,
            source_kind="family_lane_llm_legacy",
            source_detail={"family": NameFamily.SMOOTH_BLEND.value, "legacy_report": report.get("provider", "")},
        )
        if candidate is not None:
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


def _generate_mascot_mutation_family(brief: Brief, quota: int) -> list[SurfacedCandidate]:
    del brief
    generated: list[SurfacedCandidate] = []
    for base in MASCOT_BASES:
        for variant in _mascot_variants(base):
            candidate = _candidate(
                variant,
                NameFamily.MASCOT_MUTATION,
                source_kind="family_lane_deterministic",
                source_detail={"family": NameFamily.MASCOT_MUTATION.value, "base": base},
            )
            if candidate is not None:
                generated.append(candidate)
            if len(generated) >= max(quota * 3, quota + 4):
                return _dedupe_candidates(generated)
    return _dedupe_candidates(generated)


def _generate_contrarian_dictionary_family(brief: Brief, quota: int) -> list[SurfacedCandidate]:
    roots = _root_pool(brief)
    generated: list[SurfacedCandidate] = []
    pool = list(DICTIONARY_WORDS) + roots
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
        )
        if candidate is not None:
            generated.append(candidate)
        if len(generated) >= max(quota * 3, quota + 4):
            break
    return _dedupe_candidates(generated)


def _acronym_pool(brief: Brief) -> list[str]:
    tokens = _alpha_tokens(brief)
    acronyms: list[str] = []
    seen: set[str] = set()
    if len(tokens) >= 2:
        joined = "".join(token[:1] for token in tokens[:3]).upper()
        if 2 <= len(joined) <= 4:
            acronyms.append(joined)
            seen.add(joined)
    for suffix in BRUTALIST_SUFFIXES:
        if suffix not in seen:
            acronyms.append(suffix)
            seen.add(suffix)
    return acronyms


def _generate_brutalist_utility_family(brief: Brief, quota: int) -> list[SurfacedCandidate]:
    roots = _root_pool(brief)
    prefixes = [_title_case(root) for root in roots[:12] if root]
    acronyms = _acronym_pool(brief)
    generated: list[SurfacedCandidate] = []
    for prefix in prefixes:
        for suffix in acronyms:
            candidate = _candidate(
                f"{prefix} {suffix}",
                NameFamily.BRUTALIST_UTILITY,
                source_kind="family_lane_deterministic",
                source_detail={"family": NameFamily.BRUTALIST_UTILITY.value, "prefix": prefix, "suffix": suffix},
            )
            if candidate is not None:
                generated.append(candidate)
            if len(generated) >= max(quota * 3, quota + 4):
                return _dedupe_candidates(generated)
    return _dedupe_candidates(generated)


def _top_family_candidates(candidates: list[SurfacedCandidate], quota: int) -> list[SurfacedCandidate]:
    if quota <= 0:
        return []
    return candidates[:quota]


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
        NameFamily.LITERAL_TLD_HACK: lambda quota: _generate_literal_tld_hack_family(brief, quota),
        NameFamily.SMOOTH_BLEND: lambda quota: _generate_smooth_blend_family(
            brief=brief,
            config=config,
            quota=quota,
            success_context=success_context,
            avoidance_context=avoidance_context,
        ),
        NameFamily.MASCOT_MUTATION: lambda quota: _generate_mascot_mutation_family(brief, quota),
        NameFamily.CONTRARIAN_DICTIONARY: lambda quota: _generate_contrarian_dictionary_family(brief, quota),
        NameFamily.BRUTALIST_UTILITY: lambda quota: _generate_brutalist_utility_family(brief, quota),
    }
    family_counts: Counter[str] = Counter()
    use_family_llm = str(config.family_mix_profile or "").strip().lower() == "surface_diverse_v2"
    for family in FAMILY_ORDER:
        quota = int(quotas.get(family, 0))
        attempts = 0
        family_candidates: list[SurfacedCandidate] = []
        family_report: dict[str, object] = {}
        fallback_count = 0
        if use_family_llm:
            llm_candidates, llm_report = _llm_family_candidates(
                family=family,
                brief=brief,
                config=config,
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
        else:
            while len(family_candidates) < quota and attempts < 3:
                generated = deterministic_generators[family](quota)
                family_candidates = _top_family_candidates(_dedupe_candidates(generated), quota)
                attempts += 1
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
        "family_mix_profile": config.family_mix_profile,
        "family_quotas": {family.value: int(quotas.get(family, 0)) for family in FAMILY_ORDER},
        "family_counts": dict(sorted(family_counts.items())),
        "family_reports": family_reports,
        "candidate_count": len(surfaced),
        "late_fusion_min_per_family": int(config.late_fusion_min_per_family),
    }
