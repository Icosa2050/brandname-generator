from __future__ import annotations

import re

from .models import Brief, LexiconBundle


TOKEN_RE = re.compile(r"[a-z]{3,20}")
STOPWORDS = {
    "about",
    "across",
    "after",
    "around",
    "cost",
    "costs",
    "during",
    "for",
    "from",
    "into",
    "latin",
    "legal",
    "letters",
    "lowercase",
    "only",
    "software",
    "that",
    "the",
    "their",
    "these",
    "this",
    "utility",
    "with",
}
SATURATED_TERMS = {
    "accord",
    "app",
    "base",
    "bridge",
    "cloud",
    "clarity",
    "flow",
    "immo",
    "legal",
    "ledger",
    "meter",
    "pay",
    "prop",
    "rent",
    "secure",
    "settle",
    "stack",
    "trust",
}
LITERAL_SIGNAL_TERMS = {
    "accuracy",
    "calm",
    "candid",
    "charter",
    "civic",
    "clarity",
    "common",
    "covenant",
    "defensibility",
    "dependability",
    "fairness",
    "legal",
    "precision",
    "reliability",
    "stability",
    "trust",
}
LATERAL_MAP: dict[str, tuple[str, ...]] = {
    "accuracy": ("prism", "caliper", "vector", "signal", "datum"),
    "balance": ("counter", "keystone", "tandem", "steady", "meridian"),
    "clarity": ("lens", "prism", "signal", "lucent", "serein"),
    "defensibility": ("harbor", "lattice", "meridian", "lucent", "serein"),
    "energy": ("grid", "current", "spark", "radian", "pulse"),
    "fairness": ("parity", "mosaic", "meridian", "serein", "lumen"),
    "landlord": ("harbor", "keystone", "steward", "meridian", "haven"),
    "property": ("harbor", "keystone", "lattice", "meridian", "lucent"),
    "reliability": ("anchor", "harbor", "steady", "keystone", "meridian"),
    "settlement": ("meridian", "mosaic", "parity", "lumen", "serein"),
    "tenant": ("haven", "harbor", "meridian", "serein", "lumen"),
    "utility": ("grid", "current", "circuit", "signal", "radian"),
    "validation": ("signal", "charter", "anchor", "prism", "lucent"),
}
BIGRAM_MAP: dict[tuple[str, str], tuple[str, ...]] = {
    ("property", "manager"): ("keystone", "harbor", "steward", "lattice", "meridian"),
    ("private", "landlord"): ("lucent", "steward", "harbor", "keystone", "meridian"),
    ("utility", "cost"): ("radian", "signal", "counter", "vector", "tandem"),
}
LANGUAGE_BIAS_HINTS = {
    "de": "germanic",
    "ch": "germanic",
    "at": "germanic",
    "it": "latin",
    "es": "latin",
    "fr": "latin",
    "en": "neutral",
    "uk": "neutral",
    "us": "neutral",
}


def _ordered_unique(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in values:
        value = str(raw).strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _tokenize(text: str) -> list[str]:
    return [
        token
        for token in TOKEN_RE.findall(str(text or "").lower())
        if token not in STOPWORDS
    ]


def _expand_associative_terms(core_terms: tuple[str, ...], modifiers: tuple[str, ...]) -> tuple[str, ...]:
    collected: list[str] = []
    for token in (*core_terms, *modifiers):
        collected.extend(LATERAL_MAP.get(token, ()))
    pairs = list(zip(core_terms, core_terms[1:])) + list(zip(modifiers, modifiers[1:]))
    for pair in pairs:
        collected.extend(BIGRAM_MAP.get(pair, ()))
    return _ordered_unique(collected)


def _language_bias(language_market: str) -> str:
    for token in _tokenize(language_market):
        if token in LANGUAGE_BIAS_HINTS:
            return LANGUAGE_BIAS_HINTS[token]
    return "neutral"


def _extract_morphemes(words: tuple[str, ...], avoid_terms: tuple[str, ...]) -> tuple[str, ...]:
    avoid_set = set(avoid_terms)
    parts: list[str] = []
    for word in words:
        if len(word) < 4:
            continue
        variants = {word}
        if len(word) >= 6:
            variants.add(word[:3])
            variants.add(word[:4])
            variants.add(word[-3:])
            variants.add(word[-4:])
        for size in (3, 4):
            for idx in range(0, max(0, len(word) - size + 1)):
                chunk = word[idx : idx + size]
                if re.search(r"[aeiouy]", chunk):
                    variants.add(chunk)
        for variant in variants:
            if len(variant) < 3:
                continue
            if any(variant in avoid or avoid in variant for avoid in avoid_set if len(avoid) >= 4):
                continue
            parts.append(variant)
    ordered = sorted(set(parts), key=lambda item: (-len(item), item))
    return tuple(ordered[:32])


def build_lexicon(brief: Brief) -> tuple[LexiconBundle, dict[str, object]]:
    core_terms = _ordered_unique(_tokenize(brief.product_core))
    raw_modifiers = _ordered_unique(
        [token for part in [*brief.target_users, *brief.trust_signals] for token in _tokenize(part)]
    )
    modifiers = _ordered_unique([token for token in raw_modifiers if token not in LITERAL_SIGNAL_TERMS])
    avoid_terms = _ordered_unique(_tokenize(" ".join(brief.forbidden_directions)) + sorted(SATURATED_TERMS))
    associative_terms = _expand_associative_terms(core_terms, raw_modifiers)
    morphemes = _extract_morphemes(core_terms + modifiers + associative_terms, avoid_terms)
    bundle = LexiconBundle(
        core_terms=core_terms,
        modifiers=modifiers,
        avoid_terms=avoid_terms,
        associative_terms=associative_terms,
        morphemes=morphemes,
        language_bias=_language_bias(brief.language_market),
    )
    report = {
        "core_count": len(bundle.core_terms),
        "modifier_count": len(bundle.modifiers),
        "avoid_count": len(bundle.avoid_terms),
        "associative_count": len(bundle.associative_terms),
        "morpheme_count": len(bundle.morphemes),
        "language_bias": bundle.language_bias,
    }
    return bundle, report
