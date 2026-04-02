from __future__ import annotations

from collections import Counter
from dataclasses import replace
import re
from typing import Callable, TypeVar

from .models import LexiconBundle, SeedCandidate, TasteDecision, TasteRuleHit
from .naming_policy import DEFAULT_NAMING_POLICY, NameShapePolicy, NamingPolicy, TastePolicy


T = TypeVar("T")

VOWELS = frozenset("aeiouy")
CONSONANT_RUN_RE = re.compile(r"[^aeiouy]+")
SYLLABLE_LIKE_RE = re.compile(r"[^aeiouy]*[aeiouy]+[^aeiouy]*")


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z]", "", str(name or "").strip().lower())


def _resolved_policy(policy: NamingPolicy | None) -> NamingPolicy:
    return policy or DEFAULT_NAMING_POLICY


def _is_valid_shape(name: str, shape: NameShapePolicy) -> bool:
    normalized = normalize_name(name)
    return int(shape.min_length) <= len(normalized) <= int(shape.max_length)


def build_blocked_fragments(
    bundle: LexiconBundle | None = None,
    *,
    extra_fragments: tuple[str, ...] = (),
    policy: NamingPolicy | None = None,
) -> tuple[str, ...]:
    active_policy = _resolved_policy(policy)
    fragments = set(active_policy.taste.direct_domain_fragment_roots)
    for fragment in extra_fragments:
        normalized = normalize_name(fragment)
        if len(normalized) >= 4:
            fragments.add(normalized)
    if bundle is not None:
        for term in (*bundle.avoid_terms, *bundle.core_terms, *bundle.modifiers, *bundle.associative_terms):
            normalized = normalize_name(term)
            if len(normalized) < 4:
                continue
            if normalized in active_policy.taste.direct_domain_fragment_roots:
                fragments.add(normalized)
                continue
            if any(root in normalized for root in active_policy.taste.direct_domain_fragment_roots if len(root) >= 5):
                fragments.add(normalized)
    return tuple(sorted(fragments))


def _vowel_ratio(name: str) -> float:
    normalized = normalize_name(name)
    if not normalized:
        return 0.0
    vowel_count = sum(1 for char in normalized if char in VOWELS)
    return vowel_count / len(normalized)


def _open_syllable_ratio_proxy(name: str) -> float:
    normalized = normalize_name(name)
    chunks = SYLLABLE_LIKE_RE.findall(normalized)
    if not chunks:
        return 0.0
    openish = 0
    for chunk in chunks:
        tail = re.sub(r"^.*?[aeiouy]+", "", chunk)
        tail_letters = re.sub(r"[^a-z]", "", tail)
        if len(tail_letters) <= 1:
            openish += 1
    return openish / len(chunks)


def _contains_bad_cluster(name: str, shape: NameShapePolicy) -> tuple[bool, str]:
    normalized = normalize_name(name)
    for run in CONSONANT_RUN_RE.findall(normalized):
        if len(run) > int(shape.max_consonant_run):
            return True, run
        if (
            len(run) == 3
            and int(shape.max_consonant_run) >= 3
            and run in set(shape.banned_triple_clusters)
            and run not in set(shape.safe_triple_clusters)
        ):
            return True, run
    return False, ""


def _matching_suffix(name: str, taste: TastePolicy) -> str:
    normalized = normalize_name(name)
    for suffix in sorted(taste.banned_suffix_families, key=len, reverse=True):
        if normalized.endswith(suffix) and len(normalized) - len(suffix) >= 3:
            return suffix
    return ""


def _matching_morpheme(name: str, taste: TastePolicy) -> str:
    normalized = normalize_name(name)
    for morpheme in sorted(taste.banned_morphemes, key=len, reverse=True):
        if morpheme in normalized:
            return morpheme
    return ""


def _fragment_seam_hit(name: str, blocked_fragments: tuple[str, ...]) -> str:
    normalized = normalize_name(name)
    for fragment in blocked_fragments:
        candidate = normalize_name(fragment)
        if len(candidate) < 4:
            continue
        index = normalized.find(candidate)
        if index < 0:
            continue
        if normalized == candidate:
            continue
        prefix_len = index
        suffix_len = len(normalized) - (index + len(candidate))
        if prefix_len >= 2 or suffix_len >= 2:
            return candidate
    return ""


def _leading_fragment_hit(name: str, blocked_fragments: tuple[str, ...]) -> str:
    normalized = normalize_name(name)
    variants: set[str] = set()
    for fragment in blocked_fragments:
        candidate = normalize_name(fragment)
        if len(candidate) < 4:
            continue
        if len(candidate) >= 6:
            variants.add(candidate[:5])
        variants.add(candidate[:4])
    for variant in sorted(variants, key=len, reverse=True):
        if len(variant) < 4:
            continue
        if normalized.startswith(variant) and len(normalized) - len(variant) >= 2:
            return variant
    return ""


def _generic_safe_opening(name: str, taste: TastePolicy) -> str:
    normalized = normalize_name(name)
    for opening in taste.generic_safe_openings:
        if normalized.startswith(opening) and len(normalized) - len(opening) >= 2:
            return opening
    return ""


def _exact_generic_word_hit(name: str, taste: TastePolicy) -> str:
    normalized = normalize_name(name)
    if normalized in set(taste.exact_generic_words):
        return normalized
    return ""


def evaluate_name(
    name: str,
    *,
    blocked_fragments: tuple[str, ...] = (),
    policy: NamingPolicy | None = None,
) -> TasteDecision:
    active_policy = _resolved_policy(policy)
    shape = active_policy.shape
    taste = active_policy.taste
    normalized = normalize_name(name)
    effective_blocked_fragments = tuple(
        sorted(
            {
                *taste.direct_domain_fragment_roots,
                *(normalize_name(fragment) for fragment in blocked_fragments if normalize_name(fragment)),
            }
        )
    )
    hits: list[TasteRuleHit] = []
    penalty = 0.0

    if not _is_valid_shape(normalized, shape):
        return TasteDecision(
            accepted=False,
            penalty=1.0,
            reasons=("invalid_shape",),
            hits=(TasteRuleHit(code="invalid_shape", details={"name": normalized}),),
        )

    suffix = _matching_suffix(normalized, taste)
    if suffix:
        hits.append(TasteRuleHit(code="banned_suffix_family", details={"suffix": suffix}))

    morpheme = _matching_morpheme(normalized, taste)
    if morpheme:
        hits.append(TasteRuleHit(code="banned_morpheme", details={"morpheme": morpheme}))

    if bool(shape.reject_repeated_char_run) and re.search(
        rf"(.)\1{{{max(1, int(shape.repeated_char_run_length)) - 1},}}",
        normalized,
    ):
        hits.append(TasteRuleHit(code="repeated_char_run", details={"name": normalized}))

    bad_cluster, cluster = _contains_bad_cluster(normalized, shape)
    if bad_cluster:
        hits.append(TasteRuleHit(code="cluster_overload", details={"cluster": cluster}))

    fragment = _fragment_seam_hit(normalized, effective_blocked_fragments)
    if fragment:
        hits.append(TasteRuleHit(code="direct_domain_fragment", details={"fragment": fragment}))

    leading_fragment = _leading_fragment_hit(normalized, effective_blocked_fragments)
    if leading_fragment:
        hits.append(TasteRuleHit(code="clipped_literal_fragment", details={"fragment": leading_fragment}))

    generic_opening = _generic_safe_opening(normalized, taste)
    if generic_opening:
        hits.append(TasteRuleHit(code="generic_safe_opening", details={"opening": generic_opening}))

    exact_word = _exact_generic_word_hit(normalized, taste)
    if exact_word:
        hits.append(TasteRuleHit(code="exact_generic_word", details={"word": exact_word}))

    vowel_ratio = _vowel_ratio(normalized)
    if vowel_ratio < float(taste.min_vowel_ratio):
        penalty += float(taste.low_vowel_penalty)
        hits.append(TasteRuleHit(code="low_vowel_ratio", details={"ratio": round(vowel_ratio, 3)}))

    open_ratio = _open_syllable_ratio_proxy(normalized)
    if open_ratio < float(taste.min_open_syllable_ratio):
        penalty += float(taste.low_open_syllable_penalty)
        hits.append(TasteRuleHit(code="low_open_syllable_ratio", details={"ratio": round(open_ratio, 3)}))

    hard_reasons = set(taste.reject_codes)
    reject = any(hit.code in hard_reasons for hit in hits)
    if penalty >= float(taste.reject_penalty_threshold):
        reject = True

    reasons = tuple(hit.code for hit in hits)
    return TasteDecision(
        accepted=not reject,
        penalty=round(penalty, 4),
        reasons=reasons,
        hits=tuple(hits),
    )


def _filter_items(
    items: list[T],
    *,
    name_getter: Callable[[T], str],
    blocked_fragments: tuple[str, ...],
    annotate_seed: bool = False,
    policy: NamingPolicy | None = None,
) -> tuple[list[T], dict[str, object]]:
    kept: list[T] = []
    drops = Counter()
    examples: list[dict[str, object]] = []
    input_count = len(items)

    for item in items:
        raw_name = name_getter(item)
        normalized = normalize_name(raw_name)
        decision = evaluate_name(normalized, blocked_fragments=blocked_fragments, policy=policy)
        if not decision.accepted:
            for reason in decision.reasons:
                drops[reason] += 1
            if len(examples) < 8:
                examples.append(
                    {
                        "name": normalized,
                        "reasons": list(decision.reasons),
                    }
                )
            continue
        if annotate_seed and isinstance(item, SeedCandidate):
            kept.append(
                replace(
                    item,
                    taste_penalty=float(decision.penalty),
                    taste_reasons=tuple(decision.reasons),
                )
            )
            continue
        kept.append(item)

    return kept, {
        "input_count": input_count,
        "kept": len(kept),
        "compression_ratio": round((len(kept) / input_count), 4) if input_count else 0.0,
        "dropped": dict(sorted(drops.items())),
        "examples": examples,
    }


def filter_seed_candidates(
    candidates: list[SeedCandidate],
    *,
    blocked_fragments: tuple[str, ...],
    policy: NamingPolicy | None = None,
) -> tuple[list[SeedCandidate], dict[str, object]]:
    return _filter_items(
        candidates,
        name_getter=lambda item: item.name,
        blocked_fragments=blocked_fragments,
        annotate_seed=True,
        policy=policy,
    )


def filter_names(
    names: list[str],
    *,
    blocked_fragments: tuple[str, ...],
    policy: NamingPolicy | None = None,
) -> tuple[list[str], dict[str, object]]:
    kept, report = _filter_items(
        names,
        name_getter=lambda item: str(item),
        blocked_fragments=blocked_fragments,
        annotate_seed=False,
        policy=policy,
    )
    return [normalize_name(str(item)) for item in kept], report
