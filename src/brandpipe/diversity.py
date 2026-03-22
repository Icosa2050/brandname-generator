from __future__ import annotations

from collections import Counter
import re
from typing import Callable, TypeVar

from .models import SeedCandidate


T = TypeVar("T")

BRAND_SUFFIXES = (
    "ability",
    "ation",
    "ingly",
    "ingly",
    "ify",
    "ness",
    "tion",
    "able",
    "core",
    "flow",
    "hub",
    "labs",
    "lab",
    "line",
    "logic",
    "loop",
    "nova",
    "pilot",
    "scope",
    "stack",
    "sync",
    "ware",
    "wise",
    "ly",
    "io",
    "iq",
    "sy",
    "er",
    "ai",
    "x",
)


def normalize_brand_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name or "").strip().lower())


def root_key(name: str) -> str:
    lowered = normalize_brand_name(name)
    for suffix in BRAND_SUFFIXES:
        if lowered.endswith(suffix) and len(lowered) - len(suffix) >= 4:
            lowered = lowered[: -len(suffix)]
            break
    if lowered.endswith("i") and len(lowered) >= 4:
        lowered = lowered[:-1]
    return lowered[:6]


def phonetic_key(name: str) -> str:
    lowered = normalize_brand_name(name)
    consonants = re.sub(r"[aeiouyhw]", "", lowered)
    collapsed = re.sub(r"(.)\1+", r"\1", consonants)
    return collapsed[:6] or lowered[:4]


def trigram_set(name: str) -> set[str]:
    lowered = normalize_brand_name(name)
    if not lowered:
        return set()
    padded = f"  {lowered} "
    return {padded[index : index + 3] for index in range(max(0, len(padded) - 2))}


def trigram_dice(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return (2.0 * len(left & right)) / (len(left) + len(right))


def _levenshtein(a: str, b: str, limit: int = 2) -> int:
    if a == b:
        return 0
    if abs(len(a) - len(b)) > limit:
        return limit + 1
    prev = list(range(len(b) + 1))
    for i, char_a in enumerate(a, start=1):
        current = [i]
        row_min = current[0]
        for j, char_b in enumerate(b, start=1):
            cost = 0 if char_a == char_b else 1
            current.append(min(prev[j] + 1, current[j - 1] + 1, prev[j - 1] + cost))
            row_min = min(row_min, current[-1])
        if row_min > limit:
            return limit + 1
        prev = current
    return prev[-1]


def terminal_skeleton(name: str) -> str:
    normalized = normalize_brand_name(name)
    if not normalized:
        return ""
    window = normalized[-3:] if len(normalized) >= 3 else normalized
    skeleton = re.sub(r"[aeiouy]", "", window)
    if len(skeleton) < 2 and len(normalized) >= 4:
        skeleton = re.sub(r"[aeiouy]", "", normalized[-4:])
    return skeleton[:3]


def leading_skeleton(name: str) -> str:
    normalized = normalize_brand_name(name)
    if not normalized:
        return ""
    window = normalized[:6]
    skeleton = re.sub(r"[aeiouy]", "", window)
    return skeleton[:3]


def _too_close_to_avoid(name: str, avoid_terms: tuple[str, ...]) -> bool:
    normalized = normalize_brand_name(name)
    stem = root_key(normalized)
    for avoid in avoid_terms:
        candidate = normalize_brand_name(avoid)
        if not candidate:
            continue
        if normalized == candidate or stem == root_key(candidate):
            return True
        if len(candidate) >= 4 and (candidate in normalized or normalized in candidate):
            return True
    return False


def _filter_items(
    items: list[T],
    *,
    name_getter: Callable[[T], str],
    avoid_terms: tuple[str, ...],
    saturation_limit: int,
    lead_fragment_limit: int = 0,
    lead_fragment_length: int = 4,
    lead_skeleton_limit: int = 0,
) -> tuple[list[T], dict[str, object]]:
    kept: list[T] = []
    seen_exact: set[str] = set()
    seen_roots: Counter[str] = Counter()
    seen_lead_fragments: Counter[str] = Counter()
    seen_lead_skeletons: Counter[str] = Counter()
    seen_phonetics: set[str] = set()
    kept_names: list[str] = []
    drops = Counter()
    input_count = len(items)
    for item in items:
        name = normalize_brand_name(name_getter(item))
        if not name:
            drops["empty"] += 1
            continue
        if name in seen_exact:
            drops["exact_duplicate"] += 1
            continue
        if _too_close_to_avoid(name, avoid_terms):
            drops["avoid_term"] += 1
            continue
        stem = root_key(name)
        if seen_roots[stem] >= max(1, saturation_limit):
            drops["suffix_family"] += 1
            continue
        lead_fragment = name[: max(2, int(lead_fragment_length))] if len(name) >= max(2, int(lead_fragment_length)) else ""
        if lead_fragment_limit > 0 and lead_fragment and seen_lead_fragments[lead_fragment] >= int(lead_fragment_limit):
            drops["lead_fragment_quota"] += 1
            continue
        lead_shape = leading_skeleton(name)
        if lead_skeleton_limit > 0 and lead_shape and seen_lead_skeletons[lead_shape] >= int(lead_skeleton_limit):
            drops["lead_skeleton_quota"] += 1
            continue
        ph = phonetic_key(name)
        if ph in seen_phonetics:
            drops["phonetic_duplicate"] += 1
            continue
        too_close = False
        for prior in kept_names:
            if abs(len(prior) - len(name)) > 1:
                continue
            if prior[:2] != name[:2]:
                continue
            if _levenshtein(prior, name, limit=2) <= (1 if max(len(prior), len(name)) <= 6 else 2):
                too_close = True
                break
        if too_close:
            drops["edit_distance"] += 1
            continue
        kept.append(item)
        kept_names.append(name)
        seen_exact.add(name)
        seen_phonetics.add(ph)
        seen_roots[stem] += 1
        if lead_fragment:
            seen_lead_fragments[lead_fragment] += 1
        if lead_shape:
            seen_lead_skeletons[lead_shape] += 1
    return kept, {
        "input_count": input_count,
        "kept": len(kept),
        "compression_ratio": round((len(kept) / input_count), 4) if input_count else 0.0,
        "unique_root_count": len(seen_roots),
        "unique_phonetic_count": len(seen_phonetics),
        "unique_lead_fragment_count": len(seen_lead_fragments),
        "unique_lead_skeleton_count": len(seen_lead_skeletons),
        "lead_fragment_limit": int(max(0, lead_fragment_limit)),
        "lead_fragment_length": int(max(2, lead_fragment_length)),
        "lead_skeleton_limit": int(max(0, lead_skeleton_limit)),
        "lead_fragment_distribution": dict(sorted(seen_lead_fragments.items())),
        "lead_skeleton_distribution": dict(sorted(seen_lead_skeletons.items())),
        "dropped": dict(sorted(drops.items())),
    }


def filter_seed_candidates(
    candidates: list[SeedCandidate],
    *,
    avoid_terms: tuple[str, ...],
    saturation_limit: int = 1,
) -> tuple[list[SeedCandidate], dict[str, object]]:
    return _filter_items(
        candidates,
        name_getter=lambda item: item.name,
        avoid_terms=avoid_terms,
        saturation_limit=saturation_limit,
    )


def filter_names(
    names: list[str],
    *,
    avoid_terms: tuple[str, ...],
    saturation_limit: int = 1,
    lead_fragment_limit: int = 0,
    lead_fragment_length: int = 4,
    lead_skeleton_limit: int = 0,
) -> tuple[list[str], dict[str, object]]:
    return _filter_items(
        names,
        name_getter=lambda item: item,
        avoid_terms=avoid_terms,
        saturation_limit=saturation_limit,
        lead_fragment_limit=lead_fragment_limit,
        lead_fragment_length=lead_fragment_length,
        lead_skeleton_limit=lead_skeleton_limit,
    )


def salvage_names(
    names: list[str],
    *,
    avoid_terms: tuple[str, ...],
    limit: int = 3,
) -> tuple[list[str], dict[str, object]]:
    kept: list[str] = []
    seen_exact: set[str] = set()
    drops = Counter()
    for raw_name in names:
        name = normalize_brand_name(raw_name)
        if not name:
            drops["empty"] += 1
            continue
        if name in seen_exact:
            drops["exact_duplicate"] += 1
            continue
        if _too_close_to_avoid(name, avoid_terms):
            drops["avoid_term"] += 1
            continue
        seen_exact.add(name)
        kept.append(name)
        if len(kept) >= max(1, limit):
            break
    return kept, {
        "input_count": len(names),
        "kept": len(kept),
        "compression_ratio": round((len(kept) / len(names)), 4) if names else 0.0,
        "dropped": dict(sorted(drops.items())),
        "mode": "salvage_exact_only",
    }


def filter_local_collisions(
    names: list[str],
    *,
    recent_corpus: list[dict[str, object]] | None = None,
    terminal_bigram_quota: int = 2,
    trigram_threshold: float = 0.62,
    avoid_lead_fragments: tuple[str, ...] = (),
    avoid_lead_skeletons: tuple[str, ...] = (),
    avoid_tail_fragments: tuple[str, ...] = (),
    crowded_terminal_families: tuple[str, ...] = (),
    crowded_terminal_skeletons: tuple[str, ...] = (),
) -> tuple[list[str], dict[str, object]]:
    corpus_entries: list[dict[str, object]] = []
    for item in recent_corpus or []:
        normalized = normalize_brand_name(item.get("name"))
        if not normalized:
            continue
        corpus_entries.append(
            {
                "name": normalized,
                "decision": str(item.get("decision") or "").strip(),
                "phonetic": phonetic_key(normalized),
                "trigrams": trigram_set(normalized),
            }
        )

    kept: list[str] = []
    drops = Counter()
    terminal_counts: Counter[str] = Counter()
    kept_exact: set[str] = set()
    kept_phonetic: set[str] = set()
    kept_trigrams: dict[str, set[str]] = {}
    dropped_examples: dict[str, list[str]] = {}
    trigram_scores: list[float] = []
    salvage_pool: list[tuple[float, int, str, str]] = []
    lead_fragments = {
        str(value).strip().lower()
        for value in avoid_lead_fragments
        if len(str(value).strip()) >= 4
    }
    lead_skeletons = {
        str(value).strip().lower()
        for value in avoid_lead_skeletons
        if len(str(value).strip()) >= 2
    }
    tail_fragments = {
        str(value).strip().lower()
        for value in avoid_tail_fragments
        if len(str(value).strip()) >= 3
    }
    crowded_families = {str(value).strip().lower() for value in crowded_terminal_families if str(value).strip()}
    crowded_skeletons = {str(value).strip().lower() for value in crowded_terminal_skeletons if str(value).strip()}

    def remember_drop(reason: str, name: str) -> None:
        bucket = dropped_examples.setdefault(reason, [])
        if len(bucket) < 5:
            bucket.append(name)

    for raw_name in names:
        name = normalize_brand_name(raw_name)
        if not name:
            drops["empty"] += 1
            remember_drop("empty", str(raw_name))
            continue
        if name in kept_exact:
            drops["exact_duplicate"] += 1
            remember_drop("exact_duplicate", name)
            continue
        if any(entry["name"] == name for entry in corpus_entries):
            drops["exact_corpus_collision"] += 1
            remember_drop("exact_corpus_collision", name)
            continue
        if any(name.startswith(fragment) and len(name) - len(fragment) >= 2 for fragment in lead_fragments):
            drops["lead_fragment_collision"] += 1
            fragment = next(
                fragment
                for fragment in sorted(lead_fragments, key=len, reverse=True)
                if name.startswith(fragment) and len(name) - len(fragment) >= 2
            )
            remember_drop("lead_fragment_collision", f"{name}:{fragment}")
            salvage_pool.append((0.85, 0, name, "lead_fragment_collision"))
            continue
        lead_shape = leading_skeleton(name)
        if lead_shape and lead_shape in lead_skeletons:
            drops["lead_skeleton_collision"] += 1
            remember_drop("lead_skeleton_collision", f"{name}:{lead_shape}")
            salvage_pool.append((0.86, 0, name, "lead_skeleton_collision"))
            continue
        if any(name.endswith(fragment) and len(name) - len(fragment) >= 2 for fragment in tail_fragments):
            drops["tail_fragment_collision"] += 1
            fragment = next(
                fragment
                for fragment in sorted(tail_fragments, key=len, reverse=True)
                if name.endswith(fragment) and len(name) - len(fragment) >= 2
            )
            remember_drop("tail_fragment_collision", f"{name}:{fragment}")
            salvage_pool.append((0.87, 0, name, "tail_fragment_collision"))
            continue

        terminal = name[-2:] if len(name) >= 2 else name
        if terminal_counts[terminal] >= max(1, int(terminal_bigram_quota)):
            drops["terminal_quota"] += 1
            remember_drop("terminal_quota", name)
            salvage_pool.append((1.0, terminal_counts[terminal], name, "terminal_quota"))
            continue
        if terminal in crowded_families:
            drops["crowded_terminal_family"] += 1
            remember_drop("crowded_terminal_family", f"{name}:{terminal}")
            salvage_pool.append((0.95, 0, name, "crowded_terminal_family"))
            continue
        ending_shape = terminal_skeleton(name)
        if ending_shape and ending_shape in crowded_skeletons:
            drops["crowded_terminal_skeleton"] += 1
            remember_drop("crowded_terminal_skeleton", f"{name}:{ending_shape}")
            salvage_pool.append((0.9, 0, name, "crowded_terminal_skeleton"))
            continue

        ph = phonetic_key(name)
        if ph in kept_phonetic or any(entry["phonetic"] == ph for entry in corpus_entries):
            drops["phonetic_corpus_collision"] += 1
            remember_drop("phonetic_corpus_collision", name)
            continue

        name_trigrams = trigram_set(name)
        max_score = 0.0
        if len(name) >= 5:
            for prior in corpus_entries:
                if abs(len(prior["name"]) - len(name)) > 2:
                    continue
                score = trigram_dice(name_trigrams, prior["trigrams"])
                if score > max_score:
                    max_score = score
                if score >= float(trigram_threshold):
                    drops["trigram_corpus_collision"] += 1
                    remember_drop("trigram_corpus_collision", f"{name}:{prior['name']}")
                    salvage_pool.append((max_score, 0, name, "trigram_corpus_collision"))
                    break
            else:
                for prior_name, prior_trigrams in kept_trigrams.items():
                    if abs(len(prior_name) - len(name)) > 2:
                        continue
                    score = trigram_dice(name_trigrams, prior_trigrams)
                    if score > max_score:
                        max_score = score
                    if score >= float(trigram_threshold):
                        drops["trigram_batch_collision"] += 1
                        remember_drop("trigram_batch_collision", f"{name}:{prior_name}")
                        salvage_pool.append((max_score, 0, name, "trigram_batch_collision"))
                        break
                else:
                    trigram_scores.append(max_score)
                    kept.append(name)
                    kept_exact.add(name)
                    kept_phonetic.add(ph)
                    kept_trigrams[name] = name_trigrams
                    terminal_counts[terminal] += 1
                    continue
            trigram_scores.append(max_score)
            continue

        trigram_scores.append(max_score)
        kept.append(name)
        kept_exact.add(name)
        kept_phonetic.add(ph)
        kept_trigrams[name] = name_trigrams
        terminal_counts[terminal] += 1

    relaxed_report: dict[str, object] = {}
    if not kept and salvage_pool:
        relaxed_names: list[str] = []
        for _score, _terminal_penalty, name, reason in sorted(salvage_pool, key=lambda item: (item[0], item[1], item[2])):
            if name in relaxed_names:
                continue
            relaxed_names.append(name)
            if len(relaxed_names) >= 1:
                break
        if relaxed_names:
            kept = relaxed_names
            relaxed_report = {
                "applied": True,
                "mode": "retain_lowest_local_collision",
                "kept": list(relaxed_names),
                "pool_size": len(salvage_pool),
            }

    return kept, {
        "input_count": len(names),
        "kept": len(kept),
        "compression_ratio": round((len(kept) / len(names)), 4) if names else 0.0,
        "dropped": dict(sorted(drops.items())),
        "dropped_examples": {key: values for key, values in sorted(dropped_examples.items())},
        "terminal_distribution": dict(sorted(terminal_counts.items())),
        "avg_max_trigram_score": round(sum(trigram_scores) / len(trigram_scores), 4) if trigram_scores else 0.0,
        "corpus_size": len(corpus_entries),
        "terminal_bigram_quota": max(1, int(terminal_bigram_quota)),
        "trigram_threshold": float(trigram_threshold),
        "avoid_lead_fragments": sorted(lead_fragments),
        "avoid_lead_skeletons": sorted(lead_skeletons),
        "avoid_tail_fragments": sorted(tail_fragments),
        "crowded_terminal_families": sorted(crowded_families),
        "crowded_terminal_skeletons": sorted(crowded_skeletons),
        "relaxed": bool(relaxed_report),
        "salvage": relaxed_report,
    }
