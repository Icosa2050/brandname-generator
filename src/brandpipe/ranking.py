from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
import re

from .name_normalization import fold_brand_text, normalize_brand_token
from .models import CandidateResult, NameFamily, RankedCandidate, ResultStatus, SurfacePolicy
from .naming_policy import DEFAULT_NAMING_POLICY, NamingPolicy


FUSION_FAMILY_ORDER: tuple[NameFamily, ...] = tuple(NameFamily(value) for value in DEFAULT_NAMING_POLICY.surface.fusion_family_order)
MASCOT_HINTS = DEFAULT_NAMING_POLICY.surface.mascot_hints
CONTRARIAN_HINTS = DEFAULT_NAMING_POLICY.surface.contrarian_hints
TLD_HINTS = DEFAULT_NAMING_POLICY.surface.tld_hints
RUNIC_FORGE_GOOD_ENDINGS = DEFAULT_NAMING_POLICY.surface.runic_forge_good_endings


def _resolved_policy(policy: NamingPolicy | None) -> NamingPolicy:
    return policy or DEFAULT_NAMING_POLICY


def _fusion_family_order(policy: NamingPolicy | None) -> tuple[NameFamily, ...]:
    active_policy = _resolved_policy(policy)
    ordered: list[NameFamily] = []
    for raw_name in active_policy.surface.fusion_family_order:
        try:
            family = NameFamily(str(raw_name))
        except ValueError:
            continue
        if family not in ordered:
            ordered.append(family)
    return tuple(ordered) or FUSION_FAMILY_ORDER


def _alnum_normalized(raw: str) -> str:
    return normalize_brand_token(raw)


def _surface_tokens(display_name: str) -> list[str]:
    folded = fold_brand_text(display_name)
    return [token for token in re.split(r"[^A-Za-z0-9]+", str(folded or "").strip()) if token]


def _base_rank_fields(results: list[CandidateResult]) -> tuple[float, int, int, int, int, str]:
    total_score = 100.0
    blocker_count = 0
    unavailable_count = 0
    unsupported_count = 0
    warning_count = 0
    for result in results:
        total_score += float(result.score_delta)
        if result.status == ResultStatus.FAIL:
            blocker_count += 1
        elif result.status == ResultStatus.UNAVAILABLE:
            unavailable_count += 1
        elif result.status == ResultStatus.UNSUPPORTED:
            unsupported_count += 1
        elif result.status == ResultStatus.WARN:
            warning_count += 1
    if blocker_count > 0:
        decision = "blocked"
    elif unavailable_count > 0:
        decision = "degraded"
    elif unsupported_count > 0:
        decision = "partial"
    elif warning_count > 0:
        decision = "watch"
    else:
        decision = "candidate"
    return (
        round(total_score, 2),
        blocker_count,
        unavailable_count,
        unsupported_count,
        warning_count,
        decision,
    )


def _vowel_ratio(normalized: str) -> float:
    if not normalized:
        return 0.0
    vowels = sum(1 for ch in normalized if ch in "aeiou")
    return vowels / max(1, len(normalized))


def _max_consonant_run(normalized: str) -> int:
    longest = 0
    current = 0
    for ch in normalized:
        if ch in "aeiou":
            current = 0
            continue
        current += 1
        longest = max(longest, current)
    return longest


def _runic_forge_score(display_name: str, normalized: str, *, policy: NamingPolicy | None = None) -> float:
    active_policy = _resolved_policy(policy)
    upper = str(display_name or "").upper()
    vowel_ratio = _vowel_ratio(normalized)
    marker_count = sum(1 for ch in upper if ch in "ÆØÅYQ")
    glyph_marker_count = sum(1 for ch in upper if ch in "ÆØÅ")
    consonant_run = _max_consonant_run(normalized)
    score = 0.0
    if " " not in display_name and "-" not in display_name and "." not in display_name:
        score += 5.0
    if 6 <= len(normalized) <= 8:
        score += 6.0
    elif len(normalized) == 9:
        score += 2.0
    if glyph_marker_count == 1:
        score += 6.0
    elif glyph_marker_count > 1:
        score += 2.0
    if "Q" in upper and "QU" not in upper:
        score += 2.0
    if marker_count == 1:
        score += 4.0
    elif marker_count == 2:
        score += 1.0
    elif marker_count > 2:
        score -= 8.0
    if 0.28 <= vowel_ratio <= 0.52:
        score += 5.0
    elif vowel_ratio < 0.2:
        score -= 6.0
    elif vowel_ratio < 0.28 or vowel_ratio > 0.58:
        score -= 3.0
    if re.search(r"(VÆR|KÆD|TRÆN|FYRN|VYR|SYL|KYL|SOLK|ZYL|QYL)", upper):
        score += 2.0
    if any(upper.endswith(ending) for ending in active_policy.surface.runic_forge_good_endings):
        score += 5.0
    else:
        score -= 1.0
    if upper.endswith("X") or re.search(r"(RAX|GN)$", upper):
        score -= 8.0
    if consonant_run >= 4:
        score -= 5.0
    elif consonant_run == 3:
        score -= 1.5
    if sum(1 for ch in normalized if ch in "aeiou") > 4:
        score -= 5.0
    return round(score, 2)


def score_family_surface(
    *,
    display_name: str,
    family: NameFamily,
    surface_policy: SurfacePolicy,
    policy: NamingPolicy | None = None,
) -> float:
    active_policy = _resolved_policy(policy)
    normalized = _alnum_normalized(display_name)
    tokens = _surface_tokens(display_name)
    score = 0.0
    vowel_ratio = _vowel_ratio(normalized)
    if family == NameFamily.LITERAL_TLD_HACK:
        if any(display_name.lower().endswith(tld) for tld in active_policy.surface.tld_hints):
            score += 14.0
        if "." in display_name:
            score += 8.0
        if "-" in display_name:
            score += 4.0
        if len(tokens) == 2 and len(tokens[0]) >= 5:
            score += 3.0
        if 5 <= len(normalized) <= 11:
            score += 4.0
        if display_name.count(".") > 1:
            score -= 10.0
    elif family == NameFamily.SMOOTH_BLEND:
        if surface_policy == SurfacePolicy.ALPHA_LOWER:
            score += 8.0
        if 7 <= len(normalized) <= 10:
            score += 6.0
        if 0.35 <= vowel_ratio <= 0.62:
            score += 4.0
        if any(doubled in normalized for doubled in ("aa", "ee", "oo", "ll", "mm")):
            score += 2.0
        if len(set(normalized[-3:])) >= 2:
            score += 1.5
    elif family == NameFamily.MASCOT_MUTATION:
        if surface_policy in {SurfacePolicy.ALPHA_LOWER, SurfacePolicy.MIXED_CASE_ALPHA}:
            score += 6.0
        if 5 <= len(normalized) <= 9:
            score += 5.0
        if any(hint in normalized for hint in active_policy.surface.mascot_hints):
            score += 5.0
        if any(doubled in normalized for doubled in ("ll", "rr", "tt", "mm", "oo")):
            score += 2.0
        if normalized[:1] in {"o", "a", "m", "k"}:
            score += 1.0
    elif family == NameFamily.RUNIC_FORGE:
        return _runic_forge_score(display_name, normalized, policy=active_policy)
    elif family == NameFamily.CONTRARIAN_DICTIONARY:
        if surface_policy in {SurfacePolicy.ALPHA_LOWER, SurfacePolicy.MIXED_CASE_ALPHA}:
            score += 5.0
        if len(tokens) == 1:
            score += 4.0
        if 6 <= len(normalized) <= 10:
            score += 4.0
        if any(hint in normalized for hint in active_policy.surface.contrarian_hints):
            score += 3.0
        if "-" not in display_name and "." not in display_name and " " not in display_name:
            score += 2.0
    elif family == NameFamily.BRUTALIST_UTILITY:
        if surface_policy == SurfacePolicy.TITLE_SPACED_ACRONYM:
            score += 10.0
        elif surface_policy == SurfacePolicy.MIXED_CASE_ALPHA:
            score += 4.0
        if " " in display_name:
            score += 6.0
        if re.search(r"\b[A-Z0-9]{2,4}$", display_name):
            score += 8.0
        if re.search(r"[A-Z][a-z]+[A-Z][a-z]+", display_name):
            score += 3.0
        if 6 <= len(normalized) <= 12:
            score += 2.0
    return round(score, 2)


def _sort_key(item: RankedCandidate) -> tuple[object, ...]:
    display_name = item.display_name or item.name
    return (
        item.blocker_count,
        item.unavailable_count,
        item.unsupported_count,
        item.warning_count,
        -item.total_score,
        -item.family_score,
        display_name.casefold(),
    )


def _family_sort(groups: dict[NameFamily, list[RankedCandidate]]) -> None:
    for family in groups:
        groups[family].sort(key=_sort_key)
        groups[family] = [replace(item, family_rank=index) for index, item in enumerate(groups[family], start=1)]


def _late_fusion(
    groups: dict[NameFamily, list[RankedCandidate]],
    *,
    min_per_family: int,
    policy: NamingPolicy | None = None,
) -> list[RankedCandidate]:
    fused: list[RankedCandidate] = []
    seen: set[str] = set()
    guaranteed = max(0, int(min_per_family))
    for family in _fusion_family_order(policy):
        family_items = groups.get(family, [])
        promotable = [item for item in family_items if item.blocker_count == 0] or family_items
        for item in promotable[:guaranteed]:
            key = (item.display_name or item.name).casefold()
            if key in seen:
                continue
            seen.add(key)
            fused.append(item)
    remaining: list[RankedCandidate] = []
    for family in _fusion_family_order(policy):
        for item in groups.get(family, []):
            key = (item.display_name or item.name).casefold()
            if key in seen:
                continue
            remaining.append(item)
    remaining.sort(key=_sort_key)
    fused.extend(remaining)
    return [replace(item, rank_position=index) for index, item in enumerate(fused, start=1)]


def rank_candidates(results_by_name: dict[str, list[CandidateResult]]) -> list[RankedCandidate]:
    ranked: list[RankedCandidate] = []
    for name, results in results_by_name.items():
        (
            total_score,
            blocker_count,
            unavailable_count,
            unsupported_count,
            warning_count,
            decision,
        ) = _base_rank_fields(results)
        ranked.append(
            RankedCandidate(
                name=name,
                display_name=name,
                name_normalized=_alnum_normalized(name),
                total_score=total_score,
                blocker_count=blocker_count,
                unavailable_count=unavailable_count,
                unsupported_count=unsupported_count,
                warning_count=warning_count,
                decision=decision,
                family=NameFamily.SMOOTH_BLEND,
                surface_policy=SurfacePolicy.ALPHA_LOWER,
            )
        )
    ranked.sort(key=_sort_key)
    return [
        replace(item, rank_position=index, family_rank=index)
        for index, item in enumerate(ranked, start=1)
    ]


def rank_candidate_surfaces(
    *,
    candidates: list[dict[str, object]],
    results_by_name: dict[str, list[CandidateResult]],
    min_per_family: int,
    policy: NamingPolicy | None = None,
) -> list[RankedCandidate]:
    ranked: list[RankedCandidate] = []
    by_family: dict[NameFamily, list[RankedCandidate]] = defaultdict(list)
    for candidate in candidates:
        display_name = str(candidate.get("display_name") or candidate.get("name") or "").strip()
        if not display_name:
            continue
        family = NameFamily(str(candidate.get("family") or NameFamily.SMOOTH_BLEND.value))
        surface_policy = SurfacePolicy(str(candidate.get("surface_policy") or SurfacePolicy.ALPHA_LOWER.value))
        candidate_results = results_by_name.get(display_name, [])
        (
            validation_score,
            blocker_count,
            unavailable_count,
            unsupported_count,
            warning_count,
            decision,
        ) = _base_rank_fields(candidate_results)
        family_score = score_family_surface(
            display_name=display_name,
            family=family,
            surface_policy=surface_policy,
            policy=policy,
        )
        total_score = round(validation_score + family_score, 2)
        item = RankedCandidate(
            name=display_name,
            display_name=display_name,
            name_normalized=str(candidate.get("name_normalized") or _alnum_normalized(display_name)),
            total_score=total_score,
            blocker_count=blocker_count,
            unavailable_count=unavailable_count,
            unsupported_count=unsupported_count,
            warning_count=warning_count,
            decision=decision,
            family=family,
            surface_policy=surface_policy,
            family_score=family_score,
        )
        ranked.append(item)
        by_family[family].append(item)
    _family_sort(by_family)
    return _late_fusion(by_family, min_per_family=min_per_family, policy=policy)


def group_results(rows: list[tuple[str, CandidateResult]]) -> dict[str, list[CandidateResult]]:
    grouped: dict[str, list[CandidateResult]] = defaultdict(list)
    for name, result in rows:
        grouped[name].append(result)
    return grouped
