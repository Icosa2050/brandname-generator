from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
import json
import http.client
import math
import os
import re
import socket
import time
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from .diversity import filter_names, filter_seed_candidates, salvage_names
from .generator_pool import generate_seed_pool, select_round_seed_candidates
from .lexicon import build_lexicon
from .models import Brief, IdeationConfig, IdeationRoleConfig, LexiconBundle, SeedCandidate
from .pseudowords import generate_pseudoword_pool
from .scoring import score_name_attractiveness
from .taste import evaluate_name


ROUND_SCHEMES: tuple[dict[str, str], ...] = (
    {
        "phonetic": "smooth",
        "morphology": "blend",
        "semantic": "trust",
        "label": "rounded-open",
        "preferred_endings": "a, o, u, oo, el",
        "structure": "2-3 syllables, open endings, rounded vowels, liquid consonants",
    },
    {
        "phonetic": "crisp",
        "morphology": "coined",
        "semantic": "precision",
        "label": "stop-spark",
        "preferred_endings": "e, o, um, et, ix",
        "structure": "firmer stops, contrasty rhythm, less default enterprise polish",
    },
    {
        "phonetic": "bright",
        "morphology": "hybrid",
        "semantic": "clarity",
        "label": "bright-lilt",
        "preferred_endings": "i, o, ar, il, en",
        "structure": "clean stems, light vowels, sharper finish, wider ending range",
    },
    {
        "phonetic": "grounded",
        "morphology": "coined",
        "semantic": "stability",
        "label": "odd-familiar",
        "preferred_endings": "o, a, er, um, en",
        "structure": "almost-familiar forms, asymmetry welcome, avoid obvious dictionary drift",
    },
    {
        "phonetic": "balanced",
        "morphology": "blend",
        "semantic": "fairness",
        "label": "cross-current",
        "preferred_endings": "al, or, ou, ar, el",
        "structure": "mixed cadences, less latinate sameness, push opening diversity",
    },
    {
        "phonetic": "resonant",
        "morphology": "hybrid",
        "semantic": "lift",
        "label": "wildcard-open",
        "preferred_endings": "a, o, u, is, on",
        "structure": "surprising phonetic turns, rounded or clipped exits, pronounceability retained",
    },
)
ROLE_HINTS: dict[str, str] = {
    "creative_divergence": "Push away from the existing-company center of gravity; favor non-obvious sound-shapes and structural variety while staying pronounceable.",
    "recombinator": "Use the seed pool and morphemes as launch points, then splice and mutate them into less ordinary phonetic territory.",
    "contrarian": "Refuse the most literal naming path and search for angled, unexpected directions instead of safe B2B polish.",
    "phonetic_explorer": "Search for fresh openings, rhythm shifts, and less crowded sound-shapes without defaulting to near-real dictionary comfort.",
    "morpheme_hybridizer": "Fuse lexicon atoms into names that feel ownable and less namespace-crowded than direct near-real-word transmutations.",
    "ending_diversifier": "Actively explore endings and cadences that widen the batch instead of returning another safe enterprise family remix.",
}
ROLE_SCHEME_OFFSETS: dict[str, int] = {
    "creative_divergence": 5,
    "recombinator": 3,
    "contrarian": 4,
    "phonetic_explorer": 1,
    "morpheme_hybridizer": 2,
    "ending_diversifier": 0,
}
OPENROUTER_MODEL_FALLBACKS: dict[str, str] = {
    "mistralai/mistral-small-creative": "moonshotai/kimi-k2.5",
}
OPENROUTER_REASONING_DISABLED_PREFIXES: tuple[str, ...] = (
    "moonshotai/",
    "deepseek/",
)
OPENROUTER_COMPLETION_CAP_PREFIXES: tuple[tuple[str, int], ...] = (
    ("google/gemini", 1024),
)
OPENROUTER_RESPONSE_MODE_PREFIXES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("google/gemini", ("json_object", "json_schema", "plain")),
)
VALID_NAME_RE = re.compile(r"^[a-z]{6,14}$")
PSEUDOWORD_NON_FATAL_WARNINGS = frozenset({"insufficient_pseudoword_yield"})
ENDING_FAMILY_RULES: tuple[tuple[str, str], ...] = (
    ("aria", "aria"),
    ("eria", "eria"),
    ("ia", "ia"),
    ("ea", "ea"),
    ("en", "en"),
    ("er", "er"),
    ("el", "el"),
    ("et", "et"),
    ("is", "is"),
    ("il", "il"),
    ("in", "in"),
    ("ix", "ix"),
    ("ex", "ex"),
    ("um", "um"),
    ("an", "an"),
    ("ar", "ar"),
    ("a", "a"),
    ("e", "e"),
    ("i", "i"),
    ("n", "n"),
    ("r", "r"),
    ("l", "l"),
    ("s", "s"),
    ("x", "x"),
)


def normalize_alpha_name(raw: str) -> str:
    return re.sub(r"[^a-z]", "", str(raw or "").strip().lower())


def is_valid_candidate_name(name: str) -> bool:
    return bool(VALID_NAME_RE.fullmatch(str(name or "").strip()))


def load_prompt_template(path: str | Path | None) -> str:
    file_path = Path(str(path or "").strip()).expanduser()
    if not str(path or "").strip():
        return ""
    if not file_path.exists():
        raise ValueError(f"prompt_template_not_found:{file_path}")
    try:
        text = file_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ValueError(f"prompt_template_read_error:{file_path}:{exc}") from exc
    if not text:
        raise ValueError(f"prompt_template_empty:{file_path}")
    return text


def render_context_lines(context_packet: dict[str, object]) -> list[str]:
    lines: list[str] = []
    if context_packet.get("product_core"):
        lines.append(f"product_core: {context_packet['product_core']}")
    if context_packet.get("target_users"):
        lines.append(f"target_users: {', '.join(context_packet['target_users'])}")
    if context_packet.get("trust_signals"):
        lines.append(f"trust_signals: {', '.join(context_packet['trust_signals'])}")
    if context_packet.get("forbidden_directions"):
        lines.append(f"forbidden_directions: {', '.join(context_packet['forbidden_directions'])}")
    if context_packet.get("language_market"):
        lines.append(f"language_market: {context_packet['language_market']}")
    if context_packet.get("notes"):
        lines.append(f"notes: {context_packet['notes']}")
    return lines


def format_avoidance_block(avoidance_context: dict[str, object] | None) -> str:
    context = avoidance_context or {}
    local_examples = context.get("local_examples") or []
    local_patterns = context.get("local_patterns") or {}
    external_failures = context.get("external_failures") or {}
    external_patterns = context.get("external_patterns") or {}
    external_terminal_families = context.get("external_terminal_families") or []
    external_lead_hints = context.get("external_lead_hints") or []
    external_tail_hints = context.get("external_tail_hints") or []
    external_fragment_hints = context.get("external_fragment_hints") or []
    external_reason_patterns = context.get("external_reason_patterns") or {}
    if (
        not local_examples
        and not local_patterns
        and not external_failures
        and not external_patterns
        and not external_terminal_families
        and not external_lead_hints
        and not external_tail_hints
        and not external_fragment_hints
        and not external_reason_patterns
    ):
        return ""

    lines = [
        "Crowded neighborhoods from recent collisions:",
        "Treat these as soft steering away from overused phonetic neighborhoods, not absolute bans. Move farther than a tiny prefix or suffix tweak.",
    ]
    if isinstance(local_examples, list) and local_examples:
        lines.append("Recent local collision neighborhoods:")
        for item in local_examples[:6]:
            if not isinstance(item, dict):
                continue
            reason = str(item.get("reason") or "").strip().replace("_", " ")
            example = str(item.get("example") or "").strip()
            if not example:
                continue
            lines.append(f"- {reason}: {example}")
    if isinstance(local_patterns, dict):
        prefixes = [str(value).strip() for value in (local_patterns.get("prefixes") or []) if str(value).strip()]
        suffixes = [str(value).strip() for value in (local_patterns.get("suffixes") or []) if str(value).strip()]
        if prefixes or suffixes:
            lines.append("Local collision patterns worth moving away from:")
            if prefixes:
                lines.append("- move away from locally crowded opening clusters: " + ", ".join(f"{prefix}-" for prefix in prefixes[:4]))
            if suffixes:
                lines.append("- move away from locally crowded ending families: " + ", ".join(f"-{suffix}" for suffix in suffixes[:4]))
    if isinstance(external_failures, dict) and external_failures:
        lines.append("Recent external failure neighborhoods:")
        for reason, examples in list(external_failures.items())[:4]:
            if not isinstance(examples, list):
                continue
            pretty_reason = str(reason or "").strip().replace("_", " ")
            shown = ", ".join(str(example).strip() for example in examples[:3] if str(example).strip())
            if not shown:
                continue
            lines.append(f"- {pretty_reason}: {shown}")
    if isinstance(external_reason_patterns, dict) and external_reason_patterns:
        lines.append("Reason-specific steering:")
        for reason, payload in list(external_reason_patterns.items())[:3]:
            if not isinstance(payload, dict):
                continue
            pretty_reason = str(reason or "").strip().replace("_", " ")
            examples = [str(value).strip() for value in (payload.get("examples") or []) if str(value).strip()]
            lead_hints = [str(value).strip() for value in (payload.get("lead_hints") or []) if str(value).strip()]
            tail_hints = [str(value).strip() for value in (payload.get("tail_hints") or []) if str(value).strip()]
            terminal_families = [str(value).strip() for value in (payload.get("terminal_families") or []) if str(value).strip()]
            bits: list[str] = []
            if examples:
                bits.append("names " + ", ".join(examples[:2]))
            if lead_hints:
                bits.append("lead " + ", ".join(f"{hint}-" for hint in lead_hints[:2]))
            if tail_hints:
                bits.append("tail " + ", ".join(f"-{hint}" for hint in tail_hints[:2]))
            if terminal_families:
                bits.append("families " + ", ".join(f"-{family}" for family in terminal_families[:2]))
            if bits:
                lines.append(f"- {pretty_reason}: " + "; ".join(bits))
    if isinstance(external_patterns, dict):
        prefixes = [str(value).strip() for value in (external_patterns.get("prefixes") or []) if str(value).strip()]
        suffixes = [str(value).strip() for value in (external_patterns.get("suffixes") or []) if str(value).strip()]
        if prefixes or suffixes:
            lines.append("External collision patterns worth moving away from:")
            if prefixes:
                lines.append("- move away from crowded opening clusters: " + ", ".join(f"{prefix}-" for prefix in prefixes[:4]))
            if suffixes:
                lines.append("- move away from crowded terminal families: " + ", ".join(f"-{suffix}" for suffix in suffixes[:4]))
    if isinstance(external_terminal_families, list):
        terminal_families = [str(value).strip() for value in external_terminal_families if str(value).strip()]
        if terminal_families:
            lines.append("- move away from crowded terminal families: " + ", ".join(f"-{family}" for family in terminal_families[:4]))
    if isinstance(external_lead_hints, list):
        lead_hints = [str(value).strip() for value in external_lead_hints if str(value).strip()]
        if lead_hints:
            lines.append("Recently crowded lead fragments:")
            lines.append("- " + ", ".join(f"{fragment}-" for fragment in lead_hints[:6]))
    if isinstance(external_tail_hints, list):
        tail_hints = [str(value).strip() for value in external_tail_hints if str(value).strip()]
        if tail_hints:
            lines.append("Recently crowded tail fragments:")
            lines.append("- " + ", ".join(f"-{fragment}" for fragment in tail_hints[:6]))
    if isinstance(external_fragment_hints, list):
        fragment_hints = [str(value).strip() for value in external_fragment_hints if str(value).strip()]
        if fragment_hints and not (external_lead_hints or external_tail_hints):
            lines.append("Recently crowded lead fragments:")
            lines.append("- " + ", ".join(f"{fragment}-" for fragment in fragment_hints[:6]))
    lines.append("Do not solve this with rhymes, echoes, or tiny mutations. Move materially away from these neighborhoods instead of changing one syllable or one ending.")
    return "\n".join(lines)


def format_positive_anchor_block(success_context: dict[str, object] | None) -> str:
    context = success_context or {}
    names = [str(value).strip() for value in (context.get("names") or []) if str(value).strip()]
    endings = [str(value).strip() for value in (context.get("endings") or []) if str(value).strip()]
    if not names and not endings:
        return ""

    lines = [
        "Positive anchors from recent keepers:",
        "A few recent names survived better. Borrow their distinctiveness and distance from crowded namespace patterns, not their exact letters.",
    ]
    if names:
        lines.append("- recent keepers: " + ", ".join(names[:4]))
    if endings:
        lines.append("- rarer terminal shapes worth echoing in spirit: " + ", ".join(f"-{ending}" for ending in endings[:4]))
    lines.append("Treat these as proof that less generic phonetic territory can work; do not copy, rhyme with, or lightly mutate them.")
    return "\n".join(lines)


def _literal_fragment_hints(blocked_fragments: tuple[str, ...], *, limit: int = 6) -> list[str]:
    hints: list[str] = []
    seen: set[str] = set()
    for fragment in blocked_fragments:
        normalized = normalize_alpha_name(fragment)
        if len(normalized) < 5:
            continue
        hint = normalized[:5] if len(normalized) >= 7 else normalized[:4]
        if len(hint) < 4 or hint in seen:
            continue
        seen.add(hint)
        hints.append(hint)
        if len(hints) >= max(1, int(limit)):
            break
    return hints


def _avoidance_fragment_hints(avoidance_context: dict[str, object] | None, *, limit: int = 6) -> tuple[str, ...]:
    context = avoidance_context or {}
    hints: list[str] = []
    seen: set[str] = set()
    for key in ("external_fragment_hints", "external_tail_hints"):
        for raw in context.get(key) or []:
            normalized = normalize_alpha_name(raw)
            if len(normalized) < 4 or normalized in seen:
                continue
            seen.add(normalized)
            hints.append(normalized)
            if len(hints) >= max(1, int(limit)):
                return tuple(hints)
    return tuple(hints)


def _avoidance_terminal_families(avoidance_context: dict[str, object] | None, *, limit: int = 4) -> tuple[str, ...]:
    context = avoidance_context or {}
    families: list[str] = []
    seen: set[str] = set()
    for raw in context.get("external_terminal_families") or []:
        normalized = normalize_alpha_name(raw)
        if len(normalized) < 2 or normalized in seen:
            continue
        seen.add(normalized)
        families.append(normalized)
        if len(families) >= max(1, int(limit)):
            break
    return tuple(families)


def select_direct_seed_names(
    round_seed_candidates: list[SeedCandidate],
    *,
    limit: int = 2,
    crowded_terminal_families: tuple[str, ...] = (),
) -> list[str]:
    archetype_rank = {
        "transmute": 0,
        "blend": 1,
        "compound": 2,
        "coined": 3,
        "hardstop": 4,
    }
    preferred = [
        candidate
        for candidate in round_seed_candidates
        if str(getattr(candidate, "archetype", "") or "").strip().lower() in {"transmute", "blend", "compound"}
    ]
    ordered = sorted(
        preferred,
        key=lambda item: (
            archetype_rank.get(str(getattr(item, "archetype", "") or "").strip().lower(), 99),
            -float(getattr(item, "source_score", 0.0) or 0.0),
            float(getattr(item, "taste_penalty", 0.0) or 0.0),
            str(getattr(item, "name", "")),
        ),
    )
    names: list[str] = []
    seen: set[str] = set()
    crowded_families = {str(value).strip().lower() for value in crowded_terminal_families if str(value).strip()}
    for candidate in ordered:
        normalized = normalize_alpha_name(candidate.name)
        if not normalized or normalized in seen:
            continue
        if crowded_families and _ending_family(normalized) in crowded_families:
            continue
        attractiveness = score_name_attractiveness(normalized)
        if attractiveness.status != "pass":
            continue
        if float(attractiveness.score_delta or 0.0) < 12.0:
            continue
        seen.add(normalized)
        names.append(normalized)
        if len(names) >= max(1, int(limit)):
            break
    return names


def _is_strong_positive_anchor(name: str) -> bool:
    normalized = normalize_alpha_name(name)
    if not normalized:
        return False
    if not evaluate_name(normalized).accepted:
        return False
    attractiveness = score_name_attractiveness(normalized)
    if attractiveness.status != "pass":
        return False
    if float(attractiveness.score_delta or 0.0) < 18.0:
        return False
    reasons = set(attractiveness.reasons)
    if "length_sweet_spot" not in reasons:
        return False
    if "vowel_balance" not in reasons:
        return False
    if "pleasant_ending" not in reasons or "liquid_support" not in reasons:
        return False
    if "closed_syllables_heavy" in reasons:
        return False
    if "dense_consonant_run" in reasons or "sharp_v" in reasons:
        return False
    if "lexical_seam" in reasons:
        return False
    if "open_syllables" not in reasons and "open_syllables_soft" not in reasons:
        return False
    return True


def sanitize_positive_anchor_context(
    success_context: dict[str, object] | None,
    *,
    seed_pool: list[SeedCandidate] | None = None,
    max_names: int = 4,
    min_names: int = 2,
) -> dict[str, object]:
    context = dict(success_context or {})
    clean_names: list[str] = []
    seen_names: set[str] = set()

    for raw_name in context.get("names") or []:
        normalized = normalize_alpha_name(raw_name)
        if not normalized or normalized in seen_names:
            continue
        if not _is_strong_positive_anchor(normalized):
            continue
        seen_names.add(normalized)
        clean_names.append(normalized)
        if len(clean_names) >= max(1, int(max_names)):
            break

    # Positive anchors should only come from already validated wins, never from
    # the current seed pool. If we do not have a small cluster of strong prior
    # keepers, it is better to teach the prompt nothing than to let mediocre
    # survivors or fresh seeds drag the style back toward safe sludge.
    _ = seed_pool
    if len(clean_names) < max(1, int(min_names)):
        clean_names = []

    endings: list[str] = []
    seen_endings: set[str] = set()
    for name in clean_names:
        if len(name) < 3:
            continue
        ending = name[-3:]
        if ending in seen_endings:
            continue
        seen_endings.add(ending)
        endings.append(ending)

    return {
        "run_ids": list(context.get("run_ids") or []),
        "names": clean_names[: max(1, int(max_names))],
        "endings": endings[: max(1, min(4, int(max_names)))],
    }


def filter_exemplar_echoes(
    names: list[str],
    *,
    exemplars: list[str],
    threshold: float = 0.63,
) -> tuple[list[str], dict[str, object]]:
    normalized_exemplars = [
        normalize_alpha_name(value)
        for value in exemplars
        if normalize_alpha_name(value)
    ]
    if not normalized_exemplars:
        return list(names), {
            "input_count": len(names),
            "kept": len(names),
            "compression_ratio": 1.0 if names else 0.0,
            "dropped": {},
            "examples": [],
            "threshold": round(float(threshold), 3),
        }

    kept: list[str] = []
    dropped = 0
    examples: list[dict[str, object]] = []
    for raw_name in names:
        name = normalize_alpha_name(raw_name)
        if not name:
            continue
        closest_name = ""
        closest_score = 0.0
        for exemplar in normalized_exemplars:
            score = SequenceMatcher(None, name, exemplar).ratio()
            if score > closest_score:
                closest_score = score
                closest_name = exemplar
        if closest_score >= float(threshold):
            dropped += 1
            if len(examples) < 8:
                examples.append(
                    {
                        "name": name,
                        "closest_exemplar": closest_name,
                        "score": round(closest_score, 4),
                    }
                )
            continue
        kept.append(name)

    return kept, {
        "input_count": len(names),
        "kept": len(kept),
        "compression_ratio": round((len(kept) / len(names)), 4) if names else 0.0,
        "dropped": {"exemplar_echo": dropped} if dropped else {},
        "examples": examples,
        "threshold": round(float(threshold), 3),
    }


def _context_packet(brief: Brief) -> dict[str, object]:
    return {
        "product_core": brief.product_core,
        "target_users": brief.target_users,
        "trust_signals": brief.trust_signals,
        "forbidden_directions": brief.forbidden_directions,
        "language_market": brief.language_market,
        "notes": brief.notes,
    }


def _role_context_packet(context_packet: dict[str, object], role_name: str) -> dict[str, object]:
    if role_name == "recombinator":
        packet = dict(context_packet)
        packet["product_core"] = ""
        packet["target_users"] = []
        packet["notes"] = " ".join(
            bit
            for bit in [
                str(context_packet.get("notes") or "").strip(),
                "Recombine from lexicon cues and seed shapes. Avoid obvious English word joins from the raw brief.",
            ]
            if bit
        ).strip()
        return packet
    if role_name == "morpheme_hybridizer":
        packet = dict(context_packet)
        packet["product_core"] = ""
        packet["target_users"] = []
        packet["notes"] = " ".join(
            bit
            for bit in [
                str(context_packet.get("notes") or "").strip(),
                "Fuse shorter lexicon atoms into broader, less-crowded brand shapes. Avoid obvious source-word joins.",
            ]
            if bit
        ).strip()
        return packet
    if role_name == "contrarian":
        packet = dict(context_packet)
        packet["target_users"] = []
        return packet
    if role_name == "phonetic_explorer":
        packet = dict(context_packet)
        packet["target_users"] = []
        packet["notes"] = " ".join(
            bit
            for bit in [
                str(context_packet.get("notes") or "").strip(),
                "Push farther on opening and rhythm variation than the default lane, but keep names smooth enough to say aloud.",
            ]
            if bit
        ).strip()
        return packet
    if role_name == "ending_diversifier":
        packet = dict(context_packet)
        packet["notes"] = " ".join(
            bit
            for bit in [
                str(context_packet.get("notes") or "").strip(),
                "Favor endings and terminal cadences that differ from the rest of the batch.",
            ]
            if bit
        ).strip()
        return packet
    return context_packet


def _round_seed_target(config: IdeationConfig) -> int:
    desired = max(1, int(config.candidates_per_round) // 2)
    lower = max(1, int(config.round_seed_min))
    upper = max(lower, int(config.round_seed_max))
    return min(upper, max(lower, desired))


def _prompt_lexicon_terms(bundle: LexiconBundle, config: IdeationConfig) -> dict[str, list[str]]:
    return {
        "core_terms": list(bundle.core_terms[: max(1, int(config.lexicon_core_limit))]),
        "modifiers": list(bundle.modifiers[: max(1, int(config.lexicon_modifier_limit))]),
        "associative_terms": list(bundle.associative_terms[: max(1, int(config.lexicon_associative_limit))]),
        "morphemes": list(bundle.morphemes[: max(1, int(config.lexicon_morpheme_limit))]),
    }


def build_prompt(
    *,
    scope: str,
    round_index: int,
    target_count: int,
    context_packet: dict[str, object],
    lexicon_terms: dict[str, list[str]] | None = None,
    seed_names: list[str] | None = None,
    success_context: dict[str, object] | None = None,
    avoidance_context: dict[str, object] | None = None,
    literal_fragments: list[str] | None = None,
    role_name: str = "creative_divergence",
    role_instructions: str = "",
    prompt_template: str = "",
) -> tuple[str, tuple[str, str, str]]:
    scheme_index = (int(round_index) + ROLE_SCHEME_OFFSETS.get(role_name, 0)) % len(ROUND_SCHEMES)
    scheme = ROUND_SCHEMES[scheme_index]
    phonetic = scheme["phonetic"]
    morphology = scheme["morphology"]
    semantic = scheme["semantic"]
    mode = (phonetic, morphology, semantic)
    role_context = _role_context_packet(context_packet, role_name)
    context_lines = render_context_lines(role_context)
    context_block = ""
    if context_lines:
        context_block = "Context packet:\n" + "\n".join(f"- {line}" for line in context_lines) + "\n"
    lexicon_block = ""
    if lexicon_terms:
        lexicon_lines: list[str] = []
        for label, values in lexicon_terms.items():
            if values:
                lexicon_lines.append(f"- {label}: {', '.join(values)}")
        if lexicon_lines:
            lexicon_block = "Lexicon cues:\n" + "\n".join(lexicon_lines) + "\n"
    seed_block = ""
    if seed_names:
        seed_block = (
            "Phonotactic seed shapes:\n"
            + "\n".join(f"- {name}" for name in seed_names)
            + "\nUse these only as structural inspiration or mutation targets. Do not copy them verbatim.\n"
        )
    avoidance_block = ""
    positive_anchor_block = ""
    formatted_positive_anchors = format_positive_anchor_block(success_context)
    if formatted_positive_anchors:
        positive_anchor_block = f"{formatted_positive_anchors}\n"
    formatted_avoidance = format_avoidance_block(avoidance_context)
    if formatted_avoidance:
        avoidance_block = f"{formatted_avoidance}\n"
    literal_fragment_block = ""
    if literal_fragments:
        literal_fragment_block = (
            "Avoid clipped business fragments:\n"
            + "- do not build names around pieces like "
            + ", ".join(f"{fragment}-" for fragment in literal_fragments[:6])
            + "\n"
        )

    template_vars = {
        "scope": str(scope),
        "round_index": str(int(round_index) + 1),
        "target_count": str(max(1, int(target_count))),
        "phonetic": str(phonetic),
        "morphology": str(morphology),
        "semantic": str(semantic),
        "scheme_label": scheme["label"],
        "preferred_endings": scheme["preferred_endings"],
        "structure": scheme["structure"],
        "context_block": str(context_block or "none\n"),
        "lexicon_block": str(lexicon_block or "none\n"),
        "seed_block": str(seed_block or "none\n"),
        "positive_anchor_block": str(positive_anchor_block or "none\n"),
        "avoidance_block": str(avoidance_block or "none\n"),
        "literal_fragment_block": str(literal_fragment_block or "none\n"),
        "role_name": str(role_name),
        "role_instructions": str(role_instructions),
    }

    if str(prompt_template or "").strip():
        prompt = str(prompt_template)
        for key, value in template_vars.items():
            prompt = prompt.replace(f"{{{key}}}", value)
        if seed_block and "{seed_block}" not in str(prompt_template):
            prompt = f"{prompt.rstrip()}\n\n{seed_block.strip()}"
        if positive_anchor_block and "{positive_anchor_block}" not in str(prompt_template):
            prompt = f"{prompt.rstrip()}\n\n{positive_anchor_block.strip()}"
        if avoidance_block and "{avoidance_block}" not in str(prompt_template):
            prompt = f"{prompt.rstrip()}\n\n{avoidance_block.strip()}"
        if literal_fragment_block and "{literal_fragment_block}" not in str(prompt_template):
            prompt = f"{prompt.rstrip()}\n\n{literal_fragment_block.strip()}"
        return prompt.strip(), mode

    prompt = (
        "This is the divergence phase for brand naming, not the evaluation phase.\n"
        "Generate app brand names for utility-cost settlement software.\n"
        "Invent first. Do not optimize for professionalism, enterprise caution, domain safety, or trademark comfort in this step. Those checks happen later.\n"
        f"Scope: {scope}\n"
        f"Round: {round_index + 1}\n"
        f"Creative lens: {role_name}\n"
        f"Target candidates: {max(1, int(target_count))}\n"
        f"Phonetic mode: {phonetic}\n"
        f"Morphology mode: {morphology}\n"
        f"Semantic mode: {semantic}\n"
        f"Scheme label: {scheme['label']}\n"
        f"Preferred endings: {scheme['preferred_endings']}\n"
        f"Structure cues: {scheme['structure']}\n"
        f"{context_block}"
        f"{lexicon_block}"
        f"{seed_block}"
        f"{positive_anchor_block}"
        f"{avoidance_block}"
        f"{literal_fragment_block}"
        f"Role guidance: {role_instructions or ROLE_HINTS.get(role_name, ROLE_HINTS['creative_divergence'])}\n"
        "Rules:\n"
        "- lowercase latin letters only, 6-14 chars\n"
        "- no spaces, punctuation, digits\n"
        "- invent first, validate later; prioritize phonetic novelty over category fit\n"
        "- allow rounded endings, open syllables, uncommon letters, and unexpected sound-shapes when they remain pronounceable\n"
        "- maximize variation across openings, middles, endings, cadence, and stress; do not collapse into one ending family\n"
        "- treat crowded-neighborhood hints as soft steering, not absolute bans; move materially away from them instead of making tiny edits\n"
        "- do not clip literal business/source words into name fragments like priv-, parc-, ledg-, rent-, util-, settl-\n"
        "- avoid default corporate templates, near-dictionary safety moves, and obvious category compounds\n"
        "- if seed shapes are provided, mutate beyond them or invert their rhythm; do not echo them unchanged\n"
        "- align with context packet priorities when provided, but do not let seriousness flatten the phonetics\n"
        "- no availability claims (domain/store/trademark/social)\n"
        '- return JSON only with schema: {"candidates":[{"name":"string"}]}\n'
        "- no markdown, no prose, no additional keys\n"
    )
    return prompt, mode


def extract_json_object(raw: str) -> str | None:
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    decoder = json.JSONDecoder()
    for start, char in enumerate(cleaned):
        if char != "{":
            continue
        try:
            payload, end = decoder.raw_decode(cleaned[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return cleaned[start : start + end]
    return None


def parse_candidate_payload(raw_text: str) -> list[str]:
    text = raw_text.strip()
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
    for item in source:
        raw_name: str | None = None
        if isinstance(item, str):
            raw_name = item
        elif isinstance(item, dict):
            candidate_name = item.get("name") or item.get("candidate")
            if isinstance(candidate_name, str):
                raw_name = candidate_name
        if raw_name is None:
            continue
        name = normalize_alpha_name(raw_name)
        if is_valid_candidate_name(name):
            names.append(name)
    return sorted(set(names))


def extract_response_content(response: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    usage = response.get("usage") if isinstance(response.get("usage"), dict) else {}
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        return "", usage, "missing_choices"
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else ""
    if isinstance(content, list):
        text_parts: list[str] = []
        for part in content:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                text_parts.append(part["text"])
        content = "\n".join(text_parts)
    if not isinstance(content, str):
        content = str(content or "")
    return content, usage, ""


def load_fixture_candidates_with_usage(path: str | Path | None) -> tuple[list[str], dict[str, Any], str]:
    file_path = Path(str(path or "").strip()).expanduser()
    if not str(path or "").strip() or not file_path.exists():
        return [], {}, "fixture_missing"
    try:
        raw = file_path.read_text(encoding="utf-8")
    except OSError:
        return [], {}, "fixture_read_error"

    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError:
        payload = None

    usage: dict[str, Any] = {}
    if isinstance(payload, dict):
        if isinstance(payload.get("usage"), dict):
            usage = payload["usage"]
        content, extracted_usage, extracted_err = extract_response_content(payload)
        if extracted_err == "":
            names = parse_candidate_payload(content)
            if names:
                return names, extracted_usage, ""
            return [], extracted_usage, "candidate_parse_failed"

    names = parse_candidate_payload(raw)
    if names:
        return names, usage, ""

    fallback: list[str] = []
    for line in raw.splitlines():
        name = normalize_alpha_name(line.strip().strip("-*").strip())
        if is_valid_candidate_name(name):
            fallback.append(name)
    deduped = sorted(set(fallback))
    if deduped:
        return deduped, usage, ""
    return [], usage, "fixture_no_candidates"


def _normalize_openai_compat_base_url(raw: str) -> str:
    value = str(raw or "").strip().rstrip("/")
    if not value:
        return "https://api.openai.com/v1"
    parsed = parse.urlparse(value)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return value
    if "://" in value or any(char.isspace() for char in value):
        return "https://api.openai.com/v1"
    host = value.split("/", 1)[0].split(":", 1)[0].strip().lower()
    scheme = "http" if host in {"localhost", "127.0.0.1", "0.0.0.0"} else "https"
    return f"{scheme}://{value}"


def _normalize_openrouter_http_referer(raw: str) -> str:
    value = str(raw or "").strip().strip('"').strip("'")
    if not value:
        return ""
    parsed = parse.urlparse(value)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return value
    if "://" in value or any(char.isspace() for char in value):
        return ""
    candidate = value.lstrip("/")
    if not candidate:
        return ""
    if "/" in candidate:
        host, path = candidate.split("/", 1)
        host = host.strip()
        if not host:
            return ""
        return f"https://{host}/{path}"
    return f"https://{candidate}"


def _temperature(raw: float) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = 0.8
    return max(0.0, min(2.0, value))


def _retry_delay_seconds(attempt_index: int) -> float:
    return min(2.0, 0.25 * (2**max(0, attempt_index)))


def _max_completion_tokens(model: str, target_count: int) -> int:
    lowered = str(model or "").strip().lower()
    for prefix, floor in OPENROUTER_COMPLETION_CAP_PREFIXES:
        if lowered.startswith(prefix):
            return max(int(floor), int(target_count) * 128)
    return max(256, int(target_count) * 64)


def _openrouter_reasoning_payload(model: str) -> dict[str, object] | None:
    lowered = str(model or "").strip().lower()
    if not lowered:
        return None
    if lowered.startswith(OPENROUTER_REASONING_DISABLED_PREFIXES):
        return {"effort": "none", "exclude": True}
    return None


def _openrouter_response_modes(model: str) -> tuple[str, ...]:
    lowered = str(model or "").strip().lower()
    for prefix, modes in OPENROUTER_RESPONSE_MODE_PREFIXES:
        if lowered.startswith(prefix):
            return modes
    return ("json_schema", "json_object", "plain")


def _response_preview(raw: object, limit: int = 200) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    compact = re.sub(r"\s+", " ", text)
    return compact[:limit]


def _ideation_roles(config: IdeationConfig) -> tuple[IdeationRoleConfig, ...]:
    if config.roles:
        return config.roles
    if config.model:
        return (
            IdeationRoleConfig(
                model=config.model,
                role="creative_divergence",
                temperature=float(config.temperature),
                weight=1,
            ),
        )
    return ()


def _candidate_schema(strict_json: bool) -> dict[str, object]:
    return {
        "name": "name_candidates",
        "strict": bool(strict_json),
        "schema": {
            "type": "object",
            "properties": {
                "candidates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                        "required": ["name"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["candidates"],
            "additionalProperties": False,
        },
    }


def _post_json(
    *,
    url: str,
    headers: dict[str, str],
    payload: dict[str, object],
    timeout_ms: int,
) -> tuple[dict[str, Any] | None, str]:
    if int(timeout_ms) <= 0:
        return None, "invalid_timeout"
    req = request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=max(1.0, timeout_ms / 1000.0)) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        return None, f"http_{exc.code}"
    except error.URLError as exc:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return None, "timeout"
        return None, "network_error"
    except socket.timeout:
        return None, "timeout"
    except (http.client.HTTPException, socket.error):
        return None, "network_error"
    except TimeoutError:
        return None, "timeout"

    try:
        parsed: Any = json.loads(raw)
    except json.JSONDecodeError:
        return None, "response_json_decode_error"
    if not isinstance(parsed, dict):
        return None, "response_invalid_root"
    return parsed, ""


def call_openrouter_candidates(
    *,
    api_key: str,
    model: str,
    prompt: str,
    target_count: int,
    timeout_ms: int,
    strict_json: bool,
    temperature: float = 0.8,
    http_referer: str = "",
    x_title: str = "",
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
            "response_format": {"type": "json_schema", "json_schema": _candidate_schema(strict_json)},
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
            last_error = parse_error
            return [], last_usage, last_error
        names = parse_candidate_payload(content)
        if names:
            return names, last_usage, ""
        last_error = "candidate_parse_failed"
        if index + 1 >= len(attempts):
            return [], last_usage, last_error
    return [], last_usage, last_error


def call_openai_compat_candidates(
    *,
    api_key: str,
    base_url: str,
    model: str,
    prompt: str,
    timeout_ms: int,
    strict_json: bool,
    temperature: float = 0.8,
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
            "response_format": {"type": "json_schema", "json_schema": _candidate_schema(strict_json)},
        },
        {
            **base_body,
            "response_format": {"type": "json_object"},
        },
        dict(base_body),
    ]

    last_usage: dict[str, Any] = {}
    last_error = "unknown"
    for index, payload in enumerate(attempts):
        response, error_code = _post_json(
            url=f"{root}/chat/completions",
            headers=headers,
            payload=payload,
            timeout_ms=timeout_ms,
        )
        if error_code:
            last_error = error_code
            if index + 1 < len(attempts) and error_code in {"http_400", "http_422", "http_429", "http_503", "http_504"}:
                if error_code in {"http_429", "http_503", "http_504"}:
                    time.sleep(_retry_delay_seconds(index))
                continue
            return [], last_usage, last_error
        if response is None:
            return [], last_usage, "unexpected_empty_response"
        content, usage, parse_error = extract_response_content(response)
        if usage:
            last_usage = usage
        if parse_error:
            last_error = parse_error
            return [], last_usage, last_error
        names = parse_candidate_payload(content)
        if names:
            return names, usage, ""
        last_error = "candidate_parse_failed"
        if index + 1 >= len(attempts):
            return [], last_usage, last_error
    return [], last_usage, last_error


def _call_provider_for_role(
    *,
    provider: str,
    config: IdeationConfig,
    role_cfg: IdeationRoleConfig,
    prompt: str,
    target_count: int,
) -> tuple[list[str], dict[str, Any], str]:
    if provider == "openrouter_http":
        api_key = str(os.getenv(config.api_key_env) or "").strip()
        if not api_key:
            raise RuntimeError(f"missing env {config.api_key_env}")
        names, usage, err = call_openrouter_candidates(
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
        if err == "http_404":
            fallback_model = OPENROUTER_MODEL_FALLBACKS.get(role_cfg.model, "")
            if fallback_model:
                names, usage, err = call_openrouter_candidates(
                    api_key=api_key,
                    model=fallback_model,
                    prompt=prompt,
                    target_count=target_count,
                    timeout_ms=max(1000, int(config.timeout_ms)),
                    strict_json=bool(config.strict_json),
                    temperature=float(role_cfg.temperature),
                    http_referer=str(os.getenv("OPENROUTER_HTTP_REFERER") or ""),
                    x_title=str(os.getenv("OPENROUTER_X_TITLE") or ""),
                )
                usage = dict(usage or {})
                usage["fallback_from"] = role_cfg.model
                usage["resolved_model"] = fallback_model
        return names, usage, err

    api_key = str(os.getenv(config.api_key_env) or "").strip() or "ollama"
    return call_openai_compat_candidates(
        api_key=api_key,
        base_url=config.openai_base_url,
        model=role_cfg.model,
        prompt=prompt,
        timeout_ms=max(1000, int(config.timeout_ms)),
        strict_json=bool(config.strict_json),
        temperature=float(role_cfg.temperature),
    )


def estimate_usage_cost_usd(
    *,
    usage: dict[str, Any],
    in_price_per_1k: float,
    out_price_per_1k: float,
) -> float:
    try:
        direct_cost = float(usage.get("cost") or 0.0)
    except (TypeError, ValueError):
        direct_cost = 0.0
    if direct_cost > 0.0:
        return round(direct_cost, 8)

    prompt_tokens = float(usage.get("prompt_tokens") or usage.get("input_tokens") or 0.0)
    completion_tokens = float(usage.get("completion_tokens") or usage.get("output_tokens") or 0.0)
    in_cost = (prompt_tokens / 1000.0) * max(0.0, in_price_per_1k)
    out_cost = (completion_tokens / 1000.0) * max(0.0, out_price_per_1k)
    return round(in_cost + out_cost, 8)


def _ending_family(name: str) -> str:
    lowered = str(name or "").strip().lower()
    for suffix, family in ENDING_FAMILY_RULES:
        if lowered.endswith(suffix):
            return family
    return lowered[-2:] if len(lowered) >= 2 else lowered


def _extend_diverse_names(
    *,
    current_names: list[str],
    seen: set[str],
    family_counts: dict[str, int],
    round_names: list[str],
    per_family_cap: int,
) -> tuple[int, int]:
    filtered_end_o = 0
    filtered_family = 0
    for name in round_names:
        if name.endswith("o"):
            filtered_end_o += 1
            continue
        family = _ending_family(name)
        if family_counts.get(family, 0) >= max(1, per_family_cap):
            filtered_family += 1
            continue
        if name in seen:
            continue
        seen.add(name)
        family_counts[family] = family_counts.get(family, 0) + 1
        current_names.append(name)
    return filtered_end_o, filtered_family


def generate_candidates(
    *,
    brief: Brief,
    config: IdeationConfig,
    success_context: dict[str, object] | None = None,
    avoidance_context: dict[str, object] | None = None,
) -> tuple[list[str], dict[str, object]]:
    provider = str(config.provider).strip().lower()
    prompt_template = load_prompt_template(config.prompt_template_file)
    context_packet = _context_packet(brief)

    if provider == "fixture":
        if not config.fixture_input:
            raise ValueError("fixture provider requires ideation.fixture_input")
        names, usage, err = load_fixture_candidates_with_usage(config.fixture_input)
        if err:
            raise RuntimeError(f"fixture ideation failed: {err}")
        return names, {"provider": "fixture", "usage": usage, "rounds": 1}

    if provider not in {"openrouter_http", "openai_compat"}:
        raise ValueError(f"unsupported ideation provider: {provider}")
    if config.pseudoword is None:
        raise ValueError("ideation.pseudoword is required for non-fixture providers")
    role_cfgs = _ideation_roles(config)
    if not role_cfgs:
        raise ValueError("ideation.model or ideation.roles is required for non-fixture providers")

    names: list[str] = []
    seen: set[str] = set()
    family_counts: dict[str, int] = {}
    total_cost = 0.0
    errors: list[str] = []
    filtered_end_o = 0
    filtered_family = 0
    lexicon_bundle, lexicon_report = build_lexicon(brief)
    pseudoword_seed_pool, pseudoword_report = generate_pseudoword_pool(
        brief=brief,
        config=config.pseudoword,
        lexicon=lexicon_bundle,
    )
    pseudoword_warning = str(pseudoword_report.get("warning") or "").strip()
    if (pseudoword_warning and pseudoword_warning not in PSEUDOWORD_NON_FATAL_WARNINGS) or not pseudoword_seed_pool:
        error_bits = [pseudoword_warning or "no_pseudowords_generated"]
        error_message = str(pseudoword_report.get("error_message") or "").strip()
        if error_message:
            error_bits.append(error_message)
        raise RuntimeError(f"pseudoword seed stage failed: {'; '.join(error_bits)}")
    avoidance_fragment_hints = _avoidance_fragment_hints(avoidance_context)
    avoidance_terminal_families = _avoidance_terminal_families(avoidance_context)
    avoidance_names = tuple(
        str(value).strip()
        for value in ((avoidance_context or {}).get("external_avoid_names") or [])
        if str(value).strip()
    )
    seed_pool, seed_pool_report = generate_seed_pool(
        lexicon_bundle,
        pseudowords=pseudoword_seed_pool,
        total_limit=max(24, int(config.candidates_per_round) * max(1, int(config.seed_pool_multiplier))),
        blocked_fragments_extra=avoidance_fragment_hints,
        avoid_terms_extra=avoidance_names,
        crowded_terminal_families=avoidance_terminal_families,
    )
    blocked_fragments = tuple(str(value) for value in (seed_pool_report.get("blocked_fragments") or []))
    literal_fragment_hints = _literal_fragment_hints(blocked_fragments)
    seed_pool, seed_filter_report = filter_seed_candidates(
        seed_pool,
        avoid_terms=tuple(sorted({*lexicon_bundle.avoid_terms, *avoidance_names})),
        saturation_limit=max(1, int(config.seed_saturation_limit)),
    )
    if not seed_pool:
        raise RuntimeError("generator seed pool failed: no_seed_candidates")
    positive_anchor_context = sanitize_positive_anchor_context(
        success_context,
        seed_pool=seed_pool,
    )
    round_seed_sizes: list[int] = []
    role_reports: list[dict[str, object]] = []
    exemplar_echo_reports: list[dict[str, object]] = []
    direct_seed_reports: list[dict[str, object]] = []
    prompt_lexicon_terms = _prompt_lexicon_terms(lexicon_bundle, config)
    for round_index in range(max(1, int(config.rounds))):
        round_seed_candidates = select_round_seed_candidates(
            seed_pool=seed_pool,
            round_index=round_index,
            max_count=_round_seed_target(config),
        )
        round_seed_names = [candidate.name for candidate in round_seed_candidates]
        direct_seed_names = select_direct_seed_names(
            round_seed_candidates,
            limit=min(2, max(1, len(round_seed_candidates))),
            crowded_terminal_families=avoidance_terminal_families,
        )
        round_seed_sizes.append(len(round_seed_candidates))
        total_weight = sum(max(1, role.weight) for role in role_cfgs)

        def _run_role(role_cfg: IdeationRoleConfig) -> tuple[IdeationRoleConfig, list[str], dict[str, Any], str]:
            role_target = max(1, round(max(1, int(config.candidates_per_round)) * (role_cfg.weight / max(1, total_weight))))
            request_target = max(
                role_target,
                int(math.ceil(role_target * max(1.0, float(config.overgenerate_factor)))),
            )
            prompt, mode = build_prompt(
                scope="global",
                round_index=round_index,
                target_count=request_target,
                context_packet=context_packet,
                lexicon_terms=prompt_lexicon_terms,
                seed_names=round_seed_names,
                success_context=positive_anchor_context,
                avoidance_context=avoidance_context,
                literal_fragments=literal_fragment_hints,
                role_name=role_cfg.role,
                role_instructions=ROLE_HINTS.get(role_cfg.role, ROLE_HINTS["creative_divergence"]),
                prompt_template=prompt_template,
            )
            started_at = time.perf_counter()
            role_names, usage, err = _call_provider_for_role(
                provider=provider,
                config=config,
                role_cfg=role_cfg,
                prompt=prompt,
                target_count=request_target,
            )
            usage_payload = dict(usage or {})
            usage_payload["latency_ms"] = round((time.perf_counter() - started_at) * 1000.0, 2)
            usage_payload["requested_target"] = request_target
            usage_payload["desired_target"] = role_target
            usage_payload["scheme_mode"] = list(mode)
            return role_cfg, role_names, usage_payload, err

        round_outputs: list[str] = []
        with ThreadPoolExecutor(max_workers=min(4, len(role_cfgs))) as executor:
            futures = [executor.submit(_run_role, role_cfg) for role_cfg in role_cfgs]
            for future in as_completed(futures):
                role_cfg, role_names, usage, err = future.result()
                total_cost += estimate_usage_cost_usd(
                    usage=usage,
                    in_price_per_1k=float(config.input_price_per_1k),
                    out_price_per_1k=float(config.output_price_per_1k),
                )
                role_reports.append(
                    {
                        "round": round_index + 1,
                        "model": str(usage.get("resolved_model") or role_cfg.model),
                        "requested_model": role_cfg.model,
                        "role": role_cfg.role,
                        "weight": role_cfg.weight,
                        "desired_target": int(usage.get("desired_target") or 0),
                        "requested_target": int(usage.get("requested_target") or 0),
                        "status": "error" if err else "ok",
                        "candidate_count": len(role_names),
                        "latency_ms": usage.get("latency_ms", 0.0),
                        "attempt_count": int(usage.get("attempt_count") or 0),
                        "response_mode": str(usage.get("response_mode") or ""),
                        "max_completion_tokens": int(usage.get("max_completion_tokens") or 0),
                        "prompt_tokens": int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0),
                        "completion_tokens": int(usage.get("completion_tokens") or usage.get("output_tokens") or 0),
                        "reasoning_tokens": int(
                            ((usage.get("completion_tokens_details") or {}).get("reasoning_tokens"))
                            or 0
                        ),
                        "response_preview": str(usage.get("response_preview") or ""),
                        "scheme_mode": usage.get("scheme_mode") or [],
                        "error": err,
                    }
                )
                if err:
                    errors.append(f"{role_cfg.role}:{role_cfg.model}:{err}")
                    continue
                round_outputs.extend(role_names)

        round_outputs, exemplar_echo_report = filter_exemplar_echoes(
            round_outputs,
            exemplars=list(positive_anchor_context.get("names") or []),
        )
        direct_seed_reports.append(
            {
                "round": round_index + 1,
                "names": list(direct_seed_names),
            }
        )
        if direct_seed_names:
            round_outputs = list(direct_seed_names) + list(round_outputs)
        exemplar_echo_reports.append(
            {
                "round": round_index + 1,
                **exemplar_echo_report,
            }
        )
        round_filtered_end_o, round_filtered_family = _extend_diverse_names(
            current_names=names,
            seen=seen,
            family_counts=family_counts,
            round_names=round_outputs,
            per_family_cap=max(1, int(config.per_family_cap)),
        )
        filtered_end_o += round_filtered_end_o
        filtered_family += round_filtered_family

    if not names:
        error_blob = ",".join(errors) if errors else "no_candidates"
        raise RuntimeError(f"ideation failed: {error_blob}")
    filtered_names, diversity_report = filter_names(
        names,
        avoid_terms=lexicon_bundle.avoid_terms,
        saturation_limit=max(1, int(config.local_filter_saturation_limit)),
        lead_fragment_limit=max(0, int(config.local_filter_lead_fragment_limit)),
        lead_fragment_length=max(2, int(config.local_filter_lead_fragment_length)),
        lead_skeleton_limit=max(0, int(config.local_filter_lead_skeleton_limit)),
    )
    if not filtered_names and names:
        filtered_names, salvage_report = salvage_names(
            names,
            avoid_terms=lexicon_bundle.avoid_terms,
            limit=min(3, max(1, len(names))),
        )
        diversity_report = {
            **diversity_report,
            "relaxed": True,
            "salvage": salvage_report,
        }
    if not filtered_names:
        raise RuntimeError("ideation diversity filter failed: no_candidates")

    return filtered_names, {
        "provider": provider,
        "model": config.model or ",".join(role.model for role in role_cfgs),
        "rounds": int(config.rounds),
        "candidate_count": len(filtered_names),
        "cost_usd": round(total_cost, 6),
        "errors": errors,
        "filtered_end_o": filtered_end_o,
        "filtered_family": filtered_family,
        "ending_families": dict(sorted(family_counts.items())),
        "lexicon": {
            **lexicon_report,
            "core_terms": list(lexicon_bundle.core_terms[: max(1, int(config.lexicon_core_limit))]),
            "modifiers": list(lexicon_bundle.modifiers[: max(1, int(config.lexicon_modifier_limit))]),
            "associative_terms": list(lexicon_bundle.associative_terms[: max(1, int(config.lexicon_associative_limit))]),
            "morphemes": list(lexicon_bundle.morphemes[: max(1, int(config.lexicon_morpheme_limit))]),
        },
        "pseudoword": {
            **pseudoword_report,
            "used_per_round": round_seed_sizes,
        },
        "broadside": {
            "round_seed_min": int(config.round_seed_min),
            "round_seed_max": int(config.round_seed_max),
            "seed_pool_multiplier": int(config.seed_pool_multiplier),
            "seed_saturation_limit": int(config.seed_saturation_limit),
            "per_family_cap": int(config.per_family_cap),
            "lexicon_core_limit": int(config.lexicon_core_limit),
            "lexicon_modifier_limit": int(config.lexicon_modifier_limit),
            "lexicon_associative_limit": int(config.lexicon_associative_limit),
            "lexicon_morpheme_limit": int(config.lexicon_morpheme_limit),
            "local_filter_saturation_limit": int(config.local_filter_saturation_limit),
            "local_filter_lead_fragment_limit": int(config.local_filter_lead_fragment_limit),
            "local_filter_lead_fragment_length": int(config.local_filter_lead_fragment_length),
            "local_filter_lead_skeleton_limit": int(config.local_filter_lead_skeleton_limit),
        },
        "seed_pool": seed_pool_report,
        "seed_diversity": seed_filter_report,
        "name_diversity": diversity_report,
        "exemplar_echo_filter": exemplar_echo_reports,
        "direct_seed_candidates": direct_seed_reports,
        "overgenerate_factor": float(config.overgenerate_factor),
        "success_context": positive_anchor_context,
        "avoidance_context": avoidance_context or {},
        "roles": role_reports,
    }
