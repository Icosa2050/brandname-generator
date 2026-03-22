from __future__ import annotations

import inspect
import math
import os
import re

from .models import Brief, LexiconBundle, PseudowordConfig


VALID_PSEUDOWORD_RE = re.compile(r"^[a-z]{6,14}$")
SEED_TOKEN_RE = re.compile(r"[a-z]{4,16}")
MAX_PSEUDOWORD_SEED_COUNT = 48
STOPWORDS = {
    "about",
    "across",
    "always",
    "around",
    "costs",
    "could",
    "every",
    "legal",
    "lowercase",
    "managers",
    "market",
    "only",
    "settlement",
    "should",
    "signals",
    "software",
    "their",
    "these",
    "trust",
    "utility",
    "with",
}
PSEUDOWORD_MIN_GENERATED = 6
PSEUDOWORD_MIN_SUCCESSFUL_SEEDS = 2


def _normalize_alpha(value: str) -> str:
    return re.sub(r"[^a-z]", "", str(value or "").strip().lower())


def _is_valid_pseudoword(value: str) -> bool:
    return bool(VALID_PSEUDOWORD_RE.fullmatch(value)) and not value.endswith("o")


def _seed_forms(token: str) -> list[str]:
    cleaned = _normalize_alpha(token)
    if len(cleaned) < 4 or cleaned in STOPWORDS:
        return []
    ordered: list[str] = []

    def add(value: str) -> None:
        candidate = _normalize_alpha(value)
        if len(candidate) < 4 or candidate in STOPWORDS or candidate in ordered:
            return
        ordered.append(candidate)

    add(cleaned)
    if cleaned.endswith("ies") and len(cleaned) > 5:
        add(f"{cleaned[:-3]}y")
    if cleaned.endswith("s") and not cleaned.endswith("ss") and len(cleaned) > 5:
        add(cleaned[:-1])
    if cleaned.endswith("ability") and len(cleaned) > 7:
        add(f"{cleaned[:-7]}able")
    if cleaned.endswith("ibility") and len(cleaned) > 7:
        add(f"{cleaned[:-7]}ible")
    return ordered


def _extract_plain_candidate(match: object) -> str:
    if isinstance(match, str):
        return match
    if isinstance(match, dict):
        for key in ("plain", "pseudoword", "word"):
            raw = match.get(key)
            if isinstance(raw, str):
                return raw
    return str(match or "")


def derive_seed_words(brief: Brief) -> list[str]:
    texts = [
        brief.product_core,
        *brief.target_users,
        *brief.trust_signals,
        brief.notes,
    ]
    ordered: list[str] = []
    seen: set[str] = set()
    for text in texts:
        for token in SEED_TOKEN_RE.findall(str(text or "").lower()):
            for candidate in _seed_forms(token):
                if candidate in seen:
                    continue
                seen.add(candidate)
                ordered.append(candidate)
    return ordered[:8]


def derive_seed_words_from_lexicon(bundle: LexiconBundle) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for token in (*bundle.core_terms, *bundle.modifiers, *bundle.associative_terms):
        for candidate in _seed_forms(token):
            if candidate in seen:
                continue
            seen.add(candidate)
            ordered.append(candidate)
    return ordered[:12]


def select_round_seed_names(*, seed_pool: list[str], round_index: int, max_count: int) -> list[str]:
    if not seed_pool or max_count <= 0:
        return []
    take = min(len(seed_pool), max(1, max_count))
    offset = (max(0, int(round_index)) * take) % len(seed_pool)
    rotated = seed_pool[offset:] + seed_pool[:offset]
    return rotated[:take]


def _configured_language_plugins(config: PseudowordConfig) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in (*config.language_plugins, config.language_plugin):
        plugin = str(raw or "").strip()
        if not plugin or plugin in seen:
            continue
        seen.add(plugin)
        ordered.append(plugin)
    return tuple(ordered or ("orthographic_english",))


def _plugin_target_counts(total: int, plugin_count: int) -> list[int]:
    if plugin_count <= 0:
        return []
    base = max(0, int(total)) // plugin_count
    remainder = max(0, int(total)) % plugin_count
    return [base + (1 if index < remainder else 0) for index in range(plugin_count)]


def generate_pseudoword_pool(
    *,
    brief: Brief,
    config: PseudowordConfig,
    lexicon: LexiconBundle | None = None,
) -> tuple[list[str], dict[str, object]]:
    seed_words = derive_seed_words_from_lexicon(lexicon) if lexicon is not None else derive_seed_words(brief)
    language_plugins = _configured_language_plugins(config)
    report: dict[str, object] = {
        "enabled": True,
        "engine": "wuggy",
        "language_plugin": language_plugins[0],
        "language_plugins": list(language_plugins),
        "requested_count": int(config.seed_count),
        "seed_words": seed_words,
        "generated_count": 0,
        "attempted_seed_count": len(seed_words) * len(language_plugins),
        "successful_seed_count": 0,
        "dropped_seeds": [],
        "plugin_reports": [],
        "warning": "",
    }
    if not seed_words:
        report["warning"] = "no_seed_words"
        return [], report

    try:
        from wuggy import WuggyGenerator
    except Exception as exc:
        report["warning"] = "wuggy_unavailable"
        report["error_class"] = exc.__class__.__name__
        report["error_message"] = str(exc)
        return [], report

    generator = WuggyGenerator()
    supported = getattr(generator, "supported_official_language_plugin_names", [])
    wanted = min(MAX_PSEUDOWORD_SEED_COUNT, max(1, int(config.seed_count)))
    report["effective_count"] = wanted
    report["min_generated_count"] = min(wanted, max(PSEUDOWORD_MIN_GENERATED, math.ceil(wanted * 0.5)))
    report["min_successful_seeds"] = min(len(seed_words), PSEUDOWORD_MIN_SUCCESSFUL_SEEDS)
    names: list[str] = []
    seen: set[str] = set(seed_words)
    dropped_seeds: list[dict[str, str]] = []
    successful_seed_count = 0
    active_plugins = [
        language_plugin
        for language_plugin in language_plugins
        if (not supported) or language_plugin in supported
    ]
    active_target_counts = _plugin_target_counts(wanted, len(active_plugins))
    plugin_target_lookup = {
        language_plugin: active_target_counts[index]
        for index, language_plugin in enumerate(active_plugins)
    }
    for language_plugin in language_plugins:
        plugin_report: dict[str, object] = {
            "language_plugin": language_plugin,
            "requested_count": int(plugin_target_lookup.get(language_plugin, 0)),
            "generated_count": 0,
            "successful_seed_count": 0,
            "dropped_seeds": [],
            "warning": "",
        }
        if supported and language_plugin not in supported:
            plugin_report["warning"] = "unsupported_language_plugin"
            plugin_report["supported_plugins"] = list(supported)
            report["plugin_reports"].append(plugin_report)
            continue
        plugin_target = int(plugin_target_lookup.get(language_plugin, 0))
        if plugin_target <= 0:
            plugin_report["warning"] = "not_requested"
            report["plugin_reports"].append(plugin_report)
            continue

        try:
            generator_file = inspect.getfile(generator.__class__)
            plugin_dir = os.path.join(
                os.path.dirname(os.path.dirname(generator_file)),
                "plugins",
                "language_data",
                language_plugin,
            )
            if not os.path.exists(plugin_dir) and hasattr(generator, "download_language_plugin"):
                generator.download_language_plugin(language_plugin, auto_download=True)
                plugin_report["downloaded_plugin"] = True
        except Exception as exc:
            plugin_report["warning"] = "language_plugin_download_failed"
            plugin_report["error_class"] = exc.__class__.__name__
            plugin_report["error_message"] = str(exc)
            report["plugin_reports"].append(plugin_report)
            continue

        try:
            generator.load(language_plugin)
        except Exception as exc:
            plugin_report["warning"] = "language_plugin_load_failed"
            plugin_report["error_class"] = exc.__class__.__name__
            plugin_report["error_message"] = str(exc)
            report["plugin_reports"].append(plugin_report)
            continue

        per_seed = max(1, math.ceil(plugin_target / max(1, len(seed_words))))
        for seed_word in seed_words:
            try:
                matches = generator.generate_classic(
                    [seed_word],
                    ncandidates_per_sequence=per_seed,
                    output_mode="plain",
                )
            except Exception as exc:
                error_message = str(exc)
                if "not found in lexicon" in error_message.lower():
                    dropped_seed = {"seed": seed_word, "reason": error_message}
                    dropped_seeds.append(dropped_seed)
                    cast_list = plugin_report["dropped_seeds"]
                    if isinstance(cast_list, list):
                        cast_list.append(dropped_seed)
                    continue
                plugin_report["warning"] = "generation_failed"
                plugin_report["error_class"] = exc.__class__.__name__
                plugin_report["error_message"] = error_message
                break
            seed_generated = 0
            for match in matches:
                name = _normalize_alpha(_extract_plain_candidate(match))
                if not _is_valid_pseudoword(name) or name in seen:
                    continue
                seen.add(name)
                names.append(name)
                seed_generated += 1
                plugin_report["generated_count"] = int(plugin_report["generated_count"]) + 1
                if len(names) >= wanted or int(plugin_report["generated_count"]) >= plugin_target:
                    break
            if seed_generated:
                plugin_report["successful_seed_count"] = int(plugin_report["successful_seed_count"]) + 1
                successful_seed_count += 1
            if len(names) >= wanted or int(plugin_report["generated_count"]) >= plugin_target:
                break
        report["plugin_reports"].append(plugin_report)
        if len(names) >= wanted:
            break

    report["successful_seed_count"] = successful_seed_count
    report["dropped_seeds"] = dropped_seeds
    plugin_reports = report["plugin_reports"] if isinstance(report["plugin_reports"], list) else []
    if any(
        isinstance(item, dict) and bool(item.get("downloaded_plugin"))
        for item in plugin_reports
    ):
        report["downloaded_plugin"] = True
    if not names:
        if len(plugin_reports) == 1 and isinstance(plugin_reports[0], dict) and plugin_reports[0].get("warning"):
            report["warning"] = str(plugin_reports[0]["warning"])
            if plugin_reports[0].get("supported_plugins"):
                report["supported_plugins"] = list(plugin_reports[0]["supported_plugins"])
            if plugin_reports[0].get("error_class"):
                report["error_class"] = str(plugin_reports[0]["error_class"])
            if plugin_reports[0].get("error_message"):
                report["error_message"] = str(plugin_reports[0]["error_message"])
        elif plugin_reports and all(str(item.get("warning") or "").strip() for item in plugin_reports if isinstance(item, dict)):
            report["warning"] = "all_language_plugins_failed"
        else:
            report["warning"] = "no_pseudowords_generated"
    elif len(names) < int(report["min_generated_count"]) or successful_seed_count < int(report["min_successful_seeds"]):
        report["warning"] = "insufficient_pseudoword_yield"
    report["generated_count"] = len(names)
    return names, report
