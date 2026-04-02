from __future__ import annotations

import re

from .models import LexiconBundle, SeedCandidate
from .naming_policy import DEFAULT_NAMING_POLICY, NamingPolicy
from .taste import build_blocked_fragments


VOWEL_RE = re.compile(r"[aeiouy]")
VOWEL_SWAPS: dict[str, tuple[str, ...]] = {
    "a": ("e", "o"),
    "e": ("a", "i"),
    "i": ("e", "a"),
    "o": ("a", "e"),
    "u": ("a", "e"),
    "y": ("i", "e"),
}


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z]", "", str(value or "").strip().lower())


def _resolved_policy(policy: NamingPolicy | None) -> NamingPolicy:
    return policy or DEFAULT_NAMING_POLICY


def _is_pronounceable(name: str, *, policy: NamingPolicy | None = None) -> bool:
    active_policy = _resolved_policy(policy)
    shape = active_policy.shape
    normalized = _normalize(name)
    if not (int(shape.min_length) <= len(normalized) <= int(shape.max_length)):
        return False
    if bool(shape.reject_repeated_char_run) and re.search(
        rf"(.)\1{{{max(1, int(shape.repeated_char_run_length)) - 1},}}",
        normalized,
    ):
        return False
    if re.search(rf"[^aeiouy]{{{max(2, int(shape.max_consonant_run)) + 1},}}", normalized):
        return False
    if not VOWEL_RE.search(normalized):
        return False
    return True


def _candidate_stems(bundle: LexiconBundle, *, policy: NamingPolicy | None = None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    active_policy = _resolved_policy(policy)
    blocked_fragments = set(build_blocked_fragments(bundle, policy=active_policy))
    source_pool = [*bundle.modifiers[:10], *bundle.associative_terms[:14], *bundle.morphemes[:20]]
    for raw in source_pool:
        normalized = _normalize(raw)
        if len(normalized) < 4:
            continue
        if normalized in blocked_fragments:
            continue
        if normalized in set(active_policy.surface.roleish_terms):
            continue
        variants = {normalized}
        for ending in active_policy.surface.transmute_common_endings:
            if normalized.endswith(ending) and len(normalized) - len(ending) >= 4:
                variants.add(normalized[: -len(ending)])
        for variant in sorted(variants, key=len, reverse=True):
            candidate = _normalize(variant)
            if len(candidate) < 4 or len(candidate) > 6 or candidate in seen:
                continue
            if candidate in blocked_fragments:
                continue
            seen.add(candidate)
            ordered.append(candidate)
    return ordered[:32]


def _score_name(name: str, ingredients: tuple[str, ...]) -> float:
    normalized = _normalize(name)
    score = 0.55
    if 6 <= len(normalized) <= 10:
        score += 0.15
    if normalized[-1:] in {"a", "e", "n", "l", "r", "s", "o", "u", "m"}:
        score += 0.08
    if normalized.endswith(("oo", "ou", "io", "um", "or")):
        score += 0.05
    if len(re.findall(r"[aeiouy]+", normalized)) in {2, 3}:
        score += 0.12
    if len({part[:1] for part in ingredients if part}) > 1:
        score += 0.03
    return round(score, 4)


def _make_seed(name: str, ingredients: tuple[str, ...], *, policy: NamingPolicy | None = None) -> SeedCandidate | None:
    normalized = _normalize(name)
    if not _is_pronounceable(normalized, policy=policy):
        return None
    return SeedCandidate(
        name=normalized,
        archetype="transmute",
        ingredients=tuple(_normalize(part) for part in ingredients if _normalize(part)),
        source_score=_score_name(normalized, ingredients),
    )


def _retarget_endings(stem: str, *, policy: NamingPolicy | None = None) -> list[str]:
    variants: list[str] = []
    trimmed = re.sub(r"[aeiouy]+$", "", stem)
    base = trimmed if len(trimmed) >= 4 else stem
    for ending in _resolved_policy(policy).surface.transmute_retarget_endings:
        candidate = base + ending
        if candidate != stem:
            variants.append(candidate)
    return variants


def _shift_vowel(stem: str) -> list[str]:
    chars = list(stem)
    variants: list[str] = []
    for index in range(1, len(chars) - 1):
        char = chars[index]
        if char not in VOWEL_SWAPS:
            continue
        for replacement in VOWEL_SWAPS[char]:
            mutated = chars[:]
            mutated[index] = replacement
            variants.append("".join(mutated))
        break
    return variants


def _base_forms(stem: str) -> list[str]:
    normalized = _normalize(stem)
    bases: list[str] = []
    trimmed = re.sub(r"(ability|ibility|ity|ment|tion|ness|ship|ward|ance|ence)$", "", normalized)
    if len(trimmed) >= 4:
        bases.append(trimmed)
    de_voweled = re.sub(r"[aeiouy]+$", "", trimmed or normalized)
    if len(de_voweled) >= 4:
        bases.append(de_voweled)
    if len(normalized) <= 6:
        bases.append(normalized)
    seen: set[str] = set()
    return [base for base in bases if len(base) >= 4 and not (base in seen or seen.add(base))]


def generate_transmute_candidates(
    bundle: LexiconBundle,
    *,
    limit: int,
    policy: NamingPolicy | None = None,
) -> list[SeedCandidate]:
    stems = _candidate_stems(bundle, policy=policy)
    seen: set[str] = set()
    grouped: list[list[SeedCandidate]] = []
    for stem in stems:
        per_stem: list[SeedCandidate] = []
        base_forms = _base_forms(stem)
        variants: list[str] = []
        for base in base_forms:
            variants.extend(_retarget_endings(base, policy=policy))
            for shifted in _shift_vowel(base):
                variants.extend(_retarget_endings(shifted, policy=policy))
        for variant in variants:
            seed = _make_seed(variant, (stem,), policy=policy)
            if seed is None or seed.name in seen:
                continue
            if any(term in seed.name for term in bundle.avoid_terms if len(term) >= 4):
                continue
            seen.add(seed.name)
            per_stem.append(seed)
        if per_stem:
            grouped.append(per_stem)
    candidates: list[SeedCandidate] = []
    index = 0
    while len(candidates) < limit:
        advanced = False
        for per_stem in grouped:
            if index >= len(per_stem):
                continue
            candidates.append(per_stem[index])
            advanced = True
            if len(candidates) >= limit:
                break
        if not advanced:
            break
        index += 1
    return candidates
