from __future__ import annotations

from collections import Counter
from itertools import product
import math
import re

from .blend import best_blend
from .diversity import filter_seed_candidates as filter_diverse_seed_candidates
from .models import LexiconBundle, SeedCandidate
from .naming_policy import DEFAULT_NAMING_POLICY, NamingPolicy
from .taste import build_blocked_fragments, filter_seed_candidates as filter_taste_seed_candidates
from .transmute import generate_transmute_candidates


VOWEL_RE = re.compile(r"[aeiouy]")


def _terminal_family(name: str) -> str:
    normalized = _normalize(name)
    return normalized[-2:] if len(normalized) >= 2 else normalized


def _resolved_policy(policy: NamingPolicy | None) -> NamingPolicy:
    return policy or DEFAULT_NAMING_POLICY


def _is_pronounceable(name: str, *, policy: NamingPolicy | None = None) -> bool:
    active_policy = _resolved_policy(policy)
    shape = active_policy.shape
    lowered = str(name or "").strip().lower()
    if not (int(shape.min_length) <= len(lowered) <= int(shape.max_length)):
        return False
    if bool(shape.disallow_terminal_o) and lowered.endswith("o"):
        return False
    if re.search(rf"[^aeiouy]{{{max(2, int(shape.max_consonant_run)) + 1},}}", lowered):
        return False
    if bool(shape.reject_repeated_char_run) and re.search(
        rf"(.)\1{{{max(1, int(shape.repeated_char_run_length)) - 1},}}",
        lowered,
    ):
        return False
    return True


def _syllable_count(name: str) -> int:
    parts = re.findall(r"[aeiouy]+", str(name or "").lower())
    return max(1, len(parts))


def _normalize(name: str) -> str:
    return re.sub(r"[^a-z]", "", str(name or "").strip().lower())


def _score_name(name: str, ingredients: tuple[str, ...]) -> float:
    score = 0.5
    syllables = _syllable_count(name)
    if 2 <= syllables <= 4:
        score += 0.2
    if len(name) <= 10:
        score += 0.1
    if VOWEL_RE.search(name[:2]) is None:
        score += 0.05
    initials = {part[:1] for part in ingredients if part}
    if len(initials) == 1 and len(ingredients) > 1:
        score += 0.05
    return round(score, 4)


def _make_seed(
    name: str,
    archetype: str,
    ingredients: tuple[str, ...],
    *,
    policy: NamingPolicy | None = None,
) -> SeedCandidate | None:
    active_policy = _resolved_policy(policy)
    normalized = _normalize(name)
    if not _is_pronounceable(normalized, policy=active_policy):
        return None
    if any(char in normalized for char in set(active_policy.shape.harsh_letters)):
        return None
    return SeedCandidate(
        name=normalized,
        archetype=archetype,
        ingredients=tuple(_normalize(part) for part in ingredients if _normalize(part)),
        source_score=_score_name(normalized, ingredients),
    )


def _seed_source_units(bundle: LexiconBundle, *, include_modifiers: bool, policy: NamingPolicy | None = None) -> list[str]:
    active_policy = _resolved_policy(policy)
    blocked = set(build_blocked_fragments(bundle, policy=active_policy))
    raw_pool: list[str] = []
    if include_modifiers:
        raw_pool.extend(bundle.modifiers[:8])
    raw_pool.extend(bundle.associative_terms[:12])
    raw_pool.extend(bundle.morphemes[:20])

    units: list[str] = []
    seen: set[str] = set()
    for raw in raw_pool:
        normalized = _normalize(raw)
        if len(normalized) < 4 or normalized in blocked:
            continue
        variants = {normalized}
        for ending in active_policy.surface.source_unit_endings:
            if normalized.endswith(ending) and len(normalized) - len(ending) >= 4:
                variants.add(normalized[: -len(ending)])
        for candidate in sorted(variants, key=len):
            unit = _normalize(candidate)
            if len(unit) < 4 or len(unit) > 6:
                continue
            if any(char in unit for char in set(active_policy.shape.harsh_letters)):
                continue
            if unit.endswith(("k", "d", "x")):
                continue
            if unit in blocked or unit in seen:
                continue
            seen.add(unit)
            units.append(unit)
    return units


def generate_compound_candidates(
    bundle: LexiconBundle,
    *,
    limit: int,
    policy: NamingPolicy | None = None,
) -> list[SeedCandidate]:
    left_pool = _seed_source_units(bundle, include_modifiers=True, policy=policy)
    right_pool = _seed_source_units(bundle, include_modifiers=False, policy=policy)
    candidates: list[SeedCandidate] = []
    seen: set[str] = set()
    for left, right in product(left_pool, right_pool):
        if left == right:
            continue
        seed = _make_seed(left + right, "compound", (left, right), policy=policy)
        if seed is None or seed.name in seen:
            continue
        if len(left) >= 5 and len(right) >= 5 and seed.name.startswith(left) and seed.name.endswith(right):
            continue
        if any(term in seed.name for term in bundle.avoid_terms if len(term) >= 4):
            continue
        seen.add(seed.name)
        candidates.append(seed)
        if len(candidates) >= limit:
            break
    return candidates


def generate_blend_candidates(
    bundle: LexiconBundle,
    *,
    limit: int,
    policy: NamingPolicy | None = None,
) -> list[SeedCandidate]:
    source_pool = _seed_source_units(bundle, include_modifiers=True, policy=policy)
    candidates: list[SeedCandidate] = []
    seen: set[str] = set()
    for left, right in product(source_pool, source_pool):
        if left == right:
            continue
        blended = best_blend(left, right)
        if blended is None:
            continue
        seed = _make_seed(blended, "blend", (left, right), policy=policy)
        if seed is None or seed.name in seen:
            continue
        if any(term in seed.name for term in bundle.avoid_terms if len(term) >= 4):
            continue
        seen.add(seed.name)
        candidates.append(seed)
        if len(candidates) >= limit:
            break
    return candidates


def generate_coined_candidates(
    bundle: LexiconBundle,
    *,
    pseudowords: list[str],
    limit: int,
    policy: NamingPolicy | None = None,
) -> list[SeedCandidate]:
    candidates: list[SeedCandidate] = []
    seen: set[str] = set()
    for raw in pseudowords:
        seed = _make_seed(raw, "coined", (raw,), policy=policy)
        if seed is None or seed.name in seen:
            continue
        if any(term in seed.name for term in bundle.avoid_terms if len(term) >= 4):
            continue
        seen.add(seed.name)
        candidates.append(seed)
        if len(candidates) >= limit:
            break
    return candidates


def _hardstop_variants(raw: str, *, policy: NamingPolicy | None = None) -> list[str]:
    normalized = _normalize(raw)
    if len(normalized) < 4:
        return []
    variants: list[str] = []
    bases: list[str] = []
    if normalized[-1:] not in "aeiouy":
        bases.append(normalized)
    trimmed = re.sub(r"[aeiouy]+$", "", normalized)
    if len(trimmed) >= 4 and VOWEL_RE.search(trimmed):
        bases.append(trimmed)
    for size in (5, 6):
        if len(normalized) >= size:
            clipped = re.sub(r"[aeiouy]+$", "", normalized[:size])
            if len(clipped) >= 4 and VOWEL_RE.search(clipped):
                bases.append(clipped)

    seen: set[str] = set()
    ordered_bases = [base for base in bases if not (base in seen or seen.add(base))]
    hardstop_endings = _resolved_policy(policy).surface.hardstop_endings
    for index, base in enumerate(ordered_bases):
        if base[-1:] not in "aeiouy":
            variants.append(base)
        ending = hardstop_endings[(sum(ord(char) for char in base) + index) % len(hardstop_endings)]
        if base[-1:] in "aeiouy":
            mutated = (base[:-1] + ending) if len(base) >= 5 else (base + ending)
        elif len(base) <= 7:
            mutated = base + ending
        else:
            mutated = base
        if mutated != base:
            variants.append(mutated)
    return variants


def generate_hardstop_candidates(
    bundle: LexiconBundle,
    *,
    pseudowords: list[str],
    limit: int,
    policy: NamingPolicy | None = None,
) -> list[SeedCandidate]:
    source_pool = list(pseudowords[:12]) + list(bundle.morphemes[:16]) + list(bundle.associative_terms[:10])
    candidates: list[SeedCandidate] = []
    seen: set[str] = set()
    for raw in source_pool:
        for variant in _hardstop_variants(raw, policy=policy):
            seed = _make_seed(variant, "hardstop", (raw,), policy=policy)
            if seed is None or seed.name in seen:
                continue
            if any(term in seed.name for term in bundle.avoid_terms if len(term) >= 4):
                continue
            seen.add(seed.name)
            candidates.append(seed)
            if len(candidates) >= limit:
                return candidates
    return candidates


def generate_seed_pool(
    bundle: LexiconBundle,
    *,
    pseudowords: list[str],
    total_limit: int = 96,
    blocked_fragments_extra: tuple[str, ...] = (),
    avoid_terms_extra: tuple[str, ...] = (),
    crowded_terminal_families: tuple[str, ...] = (),
    policy: NamingPolicy | None = None,
) -> tuple[list[SeedCandidate], dict[str, object]]:
    raw_limit = max(total_limit, math.ceil(total_limit * 1.5))
    budgets = {
        "transmute": max(12, int(raw_limit * 0.34)),
        "compound": max(8, int(raw_limit * 0.20)),
        "blend": max(8, int(raw_limit * 0.22)),
        "coined": max(6, int(raw_limit * 0.16)),
        "hardstop": max(
            4,
            raw_limit
            - int(raw_limit * 0.34)
            - int(raw_limit * 0.20)
            - int(raw_limit * 0.22)
            - int(raw_limit * 0.16),
        ),
    }
    pool = (
        generate_transmute_candidates(bundle, limit=budgets["transmute"], policy=policy)
        + generate_compound_candidates(bundle, limit=budgets["compound"], policy=policy)
        + generate_blend_candidates(bundle, limit=budgets["blend"], policy=policy)
        + generate_hardstop_candidates(bundle, pseudowords=pseudowords, limit=budgets["hardstop"], policy=policy)
        + generate_coined_candidates(bundle, pseudowords=pseudowords, limit=budgets["coined"], policy=policy)
    )
    deduped_all: list[SeedCandidate] = []
    seen: set[str] = set()
    crowded_families = {str(value).strip().lower() for value in crowded_terminal_families if str(value).strip()}
    for candidate in sorted(
        pool,
        key=lambda item: (
            _terminal_family(item.name) in crowded_families,
            -(item.source_score - item.taste_penalty),
            item.name,
        ),
    ):
        if candidate.name in seen:
            continue
        seen.add(candidate.name)
        deduped_all.append(candidate)
    blocked_fragments = build_blocked_fragments(bundle, extra_fragments=blocked_fragments_extra, policy=policy)
    deduped_all, taste_report = filter_taste_seed_candidates(
        deduped_all,
        blocked_fragments=blocked_fragments,
        policy=policy,
    )
    deduped = sorted(
        deduped_all,
        key=lambda item: (-(item.source_score - item.taste_penalty), item.name),
    )[:total_limit]
    avoid_terms = tuple(
        sorted(
            {
                *(str(value).strip() for value in bundle.avoid_terms if str(value).strip()),
                *(str(value).strip() for value in avoid_terms_extra if str(value).strip()),
            }
        )
    )
    deduped, seed_diversity_report = filter_diverse_seed_candidates(
        deduped,
        avoid_terms=avoid_terms,
        saturation_limit=1,
        policy=policy,
    )
    counts = Counter(candidate.archetype for candidate in deduped)
    total = max(1, len(deduped))
    entropy = 0.0
    for count in counts.values():
        share = count / total
        entropy -= share * math.log2(share)
    normalized_entropy = 0.0
    if len(counts) > 1:
        normalized_entropy = entropy / math.log2(len(counts))
    source_scores = [candidate.source_score for candidate in deduped]
    report = {
        "total": len(deduped),
        "archetypes": dict(sorted(counts.items())),
        "source_score_avg": round(sum(source_scores) / total, 4) if source_scores else 0.0,
        "source_score_min": round(min(source_scores), 4) if source_scores else 0.0,
        "source_score_max": round(max(source_scores), 4) if source_scores else 0.0,
        "taste_filter": taste_report,
        "blocked_fragments": list(blocked_fragments),
        "avoid_terms_extra": list(avoid_terms_extra),
        "crowded_terminal_families": sorted(crowded_families),
        "seed_diversity": seed_diversity_report,
        "diversity_score": round(normalized_entropy, 4),
        "top_examples": [
            {
                "name": candidate.name,
                "archetype": candidate.archetype,
                "ingredients": list(candidate.ingredients),
                "source_score": candidate.source_score,
                "taste_penalty": candidate.taste_penalty,
                "taste_reasons": list(candidate.taste_reasons),
            }
            for candidate in deduped[:12]
        ],
    }
    return deduped, report


def select_round_seed_candidates(
    *,
    seed_pool: list[SeedCandidate],
    round_index: int,
    max_count: int,
) -> list[SeedCandidate]:
    if not seed_pool or max_count <= 0:
        return []
    take = min(len(seed_pool), max(1, max_count))
    grouped: dict[str, list[SeedCandidate]] = {}
    for candidate in seed_pool:
        grouped.setdefault(str(getattr(candidate, "archetype", "seed") or "seed"), []).append(candidate)
    archetypes = list(grouped.keys())
    if not archetypes:
        return seed_pool[:take]
    offset = max(0, int(round_index)) % len(archetypes)
    rotated_archetypes = archetypes[offset:] + archetypes[:offset]
    positions = {name: 0 for name in rotated_archetypes}
    selected: list[SeedCandidate] = []
    while len(selected) < take:
        advanced = False
        for archetype in rotated_archetypes:
            pool = grouped[archetype]
            index = positions[archetype]
            if index >= len(pool):
                continue
            selected.append(pool[index])
            positions[archetype] = index + 1
            advanced = True
            if len(selected) >= take:
                break
        if not advanced:
            break
    return selected
