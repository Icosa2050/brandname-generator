from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
import re

from .models import CandidateResult, NameFamily, RankedCandidate, ResultStatus, SurfacePolicy


FUSION_FAMILY_ORDER: tuple[NameFamily, ...] = (
    NameFamily.LITERAL_TLD_HACK,
    NameFamily.SMOOTH_BLEND,
    NameFamily.MASCOT_MUTATION,
    NameFamily.CONTRARIAN_DICTIONARY,
    NameFamily.BRUTALIST_UTILITY,
)
MASCOT_HINTS = ("llama", "otter", "orca", "panda", "koala", "gecko", "lynx", "manta", "koi", "yak")
CONTRARIAN_HINTS = ("signal", "vector", "anchor", "rally", "forge", "discord", "pulse", "beacon", "arc")
TLD_HINTS = (".io", ".app", ".hq", ".cloud", ".ai")


def _alnum_normalized(raw: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(raw or "").lower())


def _surface_tokens(display_name: str) -> list[str]:
    return [token for token in re.split(r"[^A-Za-z0-9]+", str(display_name or "").strip()) if token]


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


def _family_local_score(*, display_name: str, family: NameFamily, surface_policy: SurfacePolicy) -> float:
    normalized = _alnum_normalized(display_name)
    tokens = _surface_tokens(display_name)
    score = 0.0
    vowel_ratio = _vowel_ratio(normalized)
    if family == NameFamily.LITERAL_TLD_HACK:
        if any(display_name.lower().endswith(tld) for tld in TLD_HINTS):
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
        if any(hint in normalized for hint in MASCOT_HINTS):
            score += 5.0
        if any(doubled in normalized for doubled in ("ll", "rr", "tt", "mm", "oo")):
            score += 2.0
        if normalized[:1] in {"o", "a", "m", "k"}:
            score += 1.0
    elif family == NameFamily.CONTRARIAN_DICTIONARY:
        if surface_policy in {SurfacePolicy.ALPHA_LOWER, SurfacePolicy.MIXED_CASE_ALPHA}:
            score += 5.0
        if len(tokens) == 1:
            score += 4.0
        if 6 <= len(normalized) <= 10:
            score += 4.0
        if any(hint in normalized for hint in CONTRARIAN_HINTS):
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


def _late_fusion(groups: dict[NameFamily, list[RankedCandidate]], *, min_per_family: int) -> list[RankedCandidate]:
    fused: list[RankedCandidate] = []
    seen: set[str] = set()
    guaranteed = max(0, int(min_per_family))
    for family in FUSION_FAMILY_ORDER:
        family_items = groups.get(family, [])
        promotable = [item for item in family_items if item.blocker_count == 0] or family_items
        for item in promotable[:guaranteed]:
            key = (item.display_name or item.name).casefold()
            if key in seen:
                continue
            seen.add(key)
            fused.append(item)
    remaining: list[RankedCandidate] = []
    for family in FUSION_FAMILY_ORDER:
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
        family_score = _family_local_score(
            display_name=display_name,
            family=family,
            surface_policy=surface_policy,
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
    return _late_fusion(by_family, min_per_family=min_per_family)


def group_results(rows: list[tuple[str, CandidateResult]]) -> dict[str, list[CandidateResult]]:
    grouped: dict[str, list[CandidateResult]] = defaultdict(list)
    for name, result in rows:
        grouped[name].append(result)
    return grouped
