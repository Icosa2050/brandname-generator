#!/usr/bin/env python3
"""Generate and screen brand-name candidates for the app.

Pipeline:
1) Generate broad candidate pool (coined + suggestive + optional seeds)
2) Score each candidate for brand quality and challenge risk
3) Run external checks (web collisions, App Store, RDAP domains, package namespaces,
   social handles, adversarial similarity)
4) Export ranked CSV and print best candidates

This is a practical pre-screening tool, not legal advice.
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import html
import itertools
import json
import re
import sqlite3
import time
from collections import Counter
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable, Protocol
from urllib import error, parse, request

PROTECTED_MARKS = [
    'immoscout24',
    'immoscout',
    'immonet',
    'immowelt',
    'immocloud',
    'immoware24',
    'objego',
    'scalara',
    'wohnify',
    'hausify',
    'kostal',
    'costal',
    'saldeo',
    'saldio',
    'utilaro',
]

ADVERSARIAL_MARKS = [
    'haufe',
    'techem',
    'ista',
    'minol',
    'bexio',
    'klara',
    'immoscout24',
    'immoscout',
    'immonet',
    'immowelt',
    'immocloud',
    'immoware24',
    'objego',
    'scalara',
    'wohnify',
    'hausify',
]

GENERIC_TOKENS = {
    'immo',
    'haus',
    'miet',
    'wohn',
    'neben',
    'umlage',
    'kosten',
    'abrechnung',
    'saldo',
    'property',
    'rent',
    'utility',
}

GERMAN_HEAVY_TOKENS = {
    'neben',
    'umlage',
    'miet',
    'wohn',
    'haus',
    'abrechn',
    'kosten',
    'betrieb',
}

COINED_PREFIXES = [
    'util',
    'sald',
    'nov',
    'vera',
    'luma',
    'vanta',
    'nexa',
    'cora',
    'tiva',
    'solva',
    'domi',
    'resi',
    'folio',
    'mora',
    'alto',
    'sora',
    'urba',
    'vala',
    'mera',
    'fina',
]

BRAND_STEMS = [
    'utila',
    'saldi',
    'doma',
    'reli',
    'talo',
    'niva',
    'vero',
    'lumo',
    'zeno',
    'cava',
    'mora',
    'voro',
    'keli',
    'soli',
    'rivo',
]

BRAND_ENDINGS = ['ro', 'ra', 'rio', 'via', 'va', 'la', 'lo', 'na', 'no', 'ta', 'to']

COINED_SUFFIXES = [
    'ro',
    'rio',
    'ra',
    'ria',
    'via',
    'va',
    'za',
    'lo',
    'lio',
    'neo',
    'no',
    'xa',
    'xo',
    'aro',
    'ivo',
]

SUGGESTIVE_ROOTS_DACH = [
    'saldo',
    'klar',
    'immo',
    'haus',
    'miet',
    'wohn',
    'objekt',
    'neben',
    'umlage',
]

SUGGESTIVE_ROOTS_GLOBAL = [
    'saldo',
    'rento',
    'casa',
    'domus',
    'terra',
    'folio',
    'ledger',
    'nesta',
]

SHORT_SUFFIXES = ['on', 'io', 'ra', 'ro', 'ly', 'eo', 'ex', 'a', 'o']

# Curated Latin-script roots to broaden phonetic variation beyond crowded DACH patterns.
# These are inspiration roots (not legal signals), intended to produce pronounceable candidates.
GLOBAL_VARIATION_ROOTS = [
    'amani',
    'imara',
    'nuru',
    'safi',
    'wazi',
    'dira',
    'faida',
    'msingi',
    'lengo',
    'rafiki',
    'jenga',
    'nguvu',
    'umoja',
    'ayo',
    'ire',
    'zuri',
    'kazi',
    'vuna',
    'soma',
    'tamu',
    'pendo',
    'saha',
    'moyo',
    'hera',
]

GLOBAL_EXPRESSIONS = [
    'clearflow',
    'fairshare',
    'trustline',
    'trueledger',
    'cleanalloc',
    'goodsettle',
    'calmledger',
    'safebalance',
    'brightbase',
]

MORPH_PREFIX_DEFAULTS = [
    'ver',
    'clar',
    'lumi',
    'sol',
    'civ',
    'dom',
    'ter',
    'nor',
    'equi',
    'aman',
    'nuru',
    'imar',
]

MORPH_ROOT_DEFAULTS = [
    'vera',
    'claro',
    'lumina',
    'doma',
    'terra',
    'saldo',
    'ratio',
    'folio',
    'nexa',
    'safi',
    'wazi',
    'umoja',
]

MORPH_SUFFIX_DEFAULTS = [
    'ra',
    'ro',
    'rio',
    'via',
    'na',
    'la',
    'ta',
    'neo',
    'lo',
    'um',
    'is',
]

DEFAULT_GENERATOR_FAMILIES = [
    'coined',
    'stem',
    'suggestive',
    'morphology',
    'seed',
    'expression',
    'source_pool',
    'blend',
]

DEFAULT_FAMILY_QUOTAS = {
    'coined': 180,
    'stem': 140,
    'suggestive': 120,
    'morphology': 200,
    'seed': 120,
    'expression': 80,
    'source_pool': 220,
    'blend': 220,
}

FALSE_FRIEND_RULES: dict[str, tuple[int, str]] = {
    'mist': (18, 'negative_meaning_de'),
    'gift': (20, 'false_friend_de'),
    'assi': (30, 'negative_association_de'),
    'nazi': (100, 'prohibited_association'),
    'blod': (16, 'negative_association_scandi'),
    'dumm': (24, 'negative_association_de'),
    'schlecht': (24, 'negative_association_de'),
    'faux': (12, 'negative_association_fr'),
    'foul': (16, 'negative_association_en'),
    'toxic': (24, 'negative_association_en'),
    'poop': (18, 'negative_association_en'),
    'crud': (18, 'negative_association_en'),
    'fail': (14, 'failure_association_en'),
    'pain': (16, 'negative_association_en'),
    'risk': (10, 'negative_association_en'),
    'debt': (14, 'negative_association_en'),
}

GIBBERISH_BIGRAMS = {
    'qx',
    'xq',
    'qj',
    'jq',
    'vv',
    'zx',
    'xz',
    'wq',
    'qw',
}

USER_AGENT = 'kostula-name-generator/1.0'


@dataclass
class GeneratedCandidate:
    name: str
    generator_family: str
    lineage_atoms: list[str]
    source_confidence: float = 0.0


@dataclass
class Candidate:
    name: str
    generator_family: str
    lineage_atoms: str
    source_confidence: float
    quality_score: int
    challenge_risk: int
    total_score: int
    descriptive_risk: int
    similarity_risk: int
    closest_mark: str
    scope_penalty: int
    store_de_count: int = -1
    store_de_exact: bool = False
    store_ch_count: int = -1
    store_ch_exact: bool = False
    store_us_count: int = -1
    store_us_exact: bool = False
    store_exact_countries: str = ''
    store_unknown_countries: str = ''
    com_available: str = 'unknown'
    com_fallback_available: str = 'unknown'
    com_fallback_domain: str = ''
    de_available: str = 'unknown'
    ch_available: str = 'unknown'
    web_result_count: int = -1
    web_exact_hits: int = -1
    web_near_hits: int = -1
    web_sample_domains: str = ''
    web_source: str = ''
    pypi_exists: str = 'unknown'
    npm_exists: str = 'unknown'
    social_github_available: str = 'unknown'
    social_linkedin_available: str = 'unknown'
    social_x_available: str = 'unknown'
    social_instagram_available: str = 'unknown'
    social_unavailable_count: int = 0
    social_unknown_count: int = 0
    adversarial_risk: int = 0
    adversarial_top_hits: str = ''
    psych_spelling_risk: int = 0
    psych_trust_proxy: int = 0
    trademark_dpma_url: str = ''
    trademark_swissreg_url: str = ''
    trademark_tmview_url: str = ''
    external_penalty: int = 0
    gibberish_penalty: int = 0
    gibberish_flags: str = ''
    false_friend_risk: int = 0
    false_friend_hits: str = ''
    shortlist_selected: bool = False
    shortlist_rank: int = 0
    shortlist_bucket: str = ''
    shortlist_reason: str = ''
    hard_fail: bool = False
    fail_reason: str = ''


@dataclass(frozen=True)
class PipelineFeatureFlags:
    pipeline_version: str
    v3_enabled: bool
    use_engine_interfaces: bool
    use_tiered_validation: bool


@dataclass(frozen=True)
class GenerationRequest:
    scope: str
    seeds: tuple[str, ...]
    min_len: int
    max_len: int
    variation_profile: str
    generator_families: tuple[str, ...]
    family_quotas: dict[str, int]
    source_atoms: list[dict]
    max_per_prefix2: int
    max_per_suffix2: int
    max_per_shape: int
    max_per_family: int


@dataclass(frozen=True)
class FilterRequest:
    generated: list[GeneratedCandidate]
    max_per_prefix2: int
    max_per_suffix2: int
    max_per_shape: int
    max_per_family: int


@dataclass(frozen=True)
class ScoringRequest:
    scope: str
    generated_items: list[GeneratedCandidate]
    similarity_fail_threshold: int
    false_friend_fail_threshold: int
    gibberish_fail_threshold: int
    false_friend_rules: dict[str, tuple[int, str]]


@dataclass(frozen=True)
class ExternalValidationRequest:
    candidates: list[Candidate]
    scope: str
    throttle_ms: int
    gate: str
    store_countries: list[str]
    store_check: bool
    web_check: bool
    web_top: int
    domain_check: bool
    require_base_com: bool
    fail_on_unknown: bool
    package_check: bool
    social_check: bool
    adversarial_fail_threshold: int
    show_progress: bool
    degraded_network_mode: bool


class CandidateGeneratorEngine(Protocol):
    engine_id: str

    def generate(self, request: GenerationRequest) -> list[GeneratedCandidate]:
        """Generate candidates from the request."""


class CandidateFilter(Protocol):
    filter_id: str

    def apply(self, request: FilterRequest) -> list[GeneratedCandidate]:
        """Apply diversity constraints to generated candidates."""


class CandidateScorerEngine(Protocol):
    scorer_id: str

    def score(self, request: ScoringRequest) -> list[Candidate]:
        """Score and pre-screen generated candidates."""


class CandidateValidatorEngine(Protocol):
    validator_id: str

    def validate(self, request: ExternalValidationRequest) -> None:
        """Run expensive external checks and mutate candidates in place."""


@dataclass(frozen=True)
class PrefixSuffixShapeFilter:
    filter_id: str = 'prefix_suffix_shape_v2'

    def apply(self, request: FilterRequest) -> list[GeneratedCandidate]:
        return diversity_filter(
            request.generated,
            max_per_prefix2=request.max_per_prefix2,
            max_per_suffix2=request.max_per_suffix2,
            max_per_shape=request.max_per_shape,
            max_per_family=request.max_per_family,
        )


@dataclass(frozen=True)
class FamilyRuleGeneratorEngine:
    engine_id: str = 'family_rules_v2'
    diversity_filter_engine: CandidateFilter | None = None

    def generate(self, request: GenerationRequest) -> list[GeneratedCandidate]:
        return generate_candidates(
            request.scope,
            request.seeds,
            request.min_len,
            request.max_len,
            request.variation_profile,
            list(request.generator_families),
            request.family_quotas,
            request.source_atoms,
            request.max_per_prefix2,
            request.max_per_suffix2,
            request.max_per_shape,
            request.max_per_family,
            filter_engine=self.diversity_filter_engine,
        )


@dataclass(frozen=True)
class RuleScorerEngine:
    scorer_id: str = 'rule_scorer_v2'

    def score(self, request: ScoringRequest) -> list[Candidate]:
        return evaluate_candidates(
            request.scope,
            request.generated_items,
            request.similarity_fail_threshold,
            request.false_friend_fail_threshold,
            request.gibberish_fail_threshold,
            request.false_friend_rules,
        )


@dataclass(frozen=True)
class ExternalCheckValidatorEngine:
    validator_id: str = 'external_checks_v2'

    def validate(self, request: ExternalValidationRequest) -> None:
        run_external_checks(
            request.candidates,
            request.scope,
            request.throttle_ms,
            request.gate,
            request.store_countries,
            request.store_check,
            request.web_check,
            request.web_top,
            request.domain_check,
            request.require_base_com,
            request.fail_on_unknown,
            request.package_check,
            request.social_check,
            request.adversarial_fail_threshold,
            request.show_progress,
            request.degraded_network_mode,
        )


def resolve_feature_flags(args: argparse.Namespace) -> PipelineFeatureFlags:
    pipeline_version = str(getattr(args, 'pipeline_version', 'v2') or 'v2').strip().lower()
    if pipeline_version not in {'v2', 'v3'}:
        pipeline_version = 'v2'
    v3_enabled = bool(getattr(args, 'enable_v3', False) or pipeline_version == 'v3')
    use_engine_interfaces = bool(getattr(args, 'use_engine_interfaces', False) or v3_enabled)
    use_tiered_validation = bool(getattr(args, 'use_tiered_validation', False) or v3_enabled)
    return PipelineFeatureFlags(
        pipeline_version=pipeline_version,
        v3_enabled=v3_enabled,
        use_engine_interfaces=use_engine_interfaces,
        use_tiered_validation=use_tiered_validation,
    )


def normalize_alpha(text: str) -> str:
    return re.sub(r'[^a-z]+', '', text.lower())


def parse_csv_set(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(',') if part.strip()]


def parse_family_quotas(raw: str) -> dict[str, int]:
    if not raw.strip():
        return dict(DEFAULT_FAMILY_QUOTAS)
    out = dict(DEFAULT_FAMILY_QUOTAS)
    for chunk in raw.split(','):
        item = chunk.strip()
        if not item or ':' not in item:
            continue
        family, value = item.split(':', 1)
        family = family.strip()
        value = value.strip()
        if not family or not value:
            continue
        try:
            quota = int(value)
        except ValueError:
            continue
        out[family] = max(0, quota)
    return out


def emit_stage_event(enabled: bool, stage: str, **fields: object) -> None:
    if not enabled:
        return
    payload = {
        'event': 'naming_pipeline_stage',
        'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
        'stage': stage,
        **fields,
    }
    print(f'stage_event={json.dumps(payload, ensure_ascii=False)}', flush=True)


def extract_json_object(raw: str) -> str | None:
    start = raw.find('{')
    if start < 0:
        return None
    depth = 0
    for idx in range(start, len(raw)):
        ch = raw[idx]
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return raw[start : idx + 1]
    return None


def parse_llm_candidate_payload(raw_text: str) -> list[str]:
    names: list[str] = []
    cleaned = raw_text.strip()
    if not cleaned:
        return names

    data: object
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        extracted = extract_json_object(cleaned)
        if not extracted:
            return names
        try:
            data = json.loads(extracted)
        except json.JSONDecodeError:
            return names

    source: list[object] = []
    if isinstance(data, dict):
        if isinstance(data.get('candidates'), list):
            source = list(data['candidates'])
        elif isinstance(data.get('names'), list):
            source = list(data['names'])
    elif isinstance(data, list):
        source = list(data)

    for item in source:
        if isinstance(item, str):
            normalized = normalize_alpha(item)
            if normalized:
                names.append(normalized)
            continue
        if isinstance(item, dict):
            raw_name = item.get('name') or item.get('candidate')
            if isinstance(raw_name, str):
                normalized = normalize_alpha(raw_name)
                if normalized:
                    names.append(normalized)
    return names


def load_llm_fallback_candidates(
    *,
    path: str,
    max_attempts: int,
    backoff_ms: int,
    allow_text_fallback: bool,
) -> list[str]:
    p = Path(path)
    if not path or not p.exists():
        return []
    raw = p.read_text(encoding='utf-8')
    attempts = max(1, max_attempts)
    for idx in range(attempts):
        names = parse_llm_candidate_payload(raw)
        if names:
            return sorted(set(names))
        if idx < attempts - 1:
            time.sleep(max(0, backoff_ms) / 1000.0 * (idx + 1))

    if not allow_text_fallback:
        return []

    fallback_names: list[str] = []
    for line in raw.splitlines():
        text = line.strip().strip('-*').strip()
        if not text:
            continue
        normalized = normalize_alpha(text)
        if 5 <= len(normalized) <= 12:
            fallback_names.append(normalized)
    return sorted(set(fallback_names))


def load_source_atoms(
    *,
    db_path: str,
    limit: int,
    min_confidence: float,
    languages: list[str],
    categories: list[str],
) -> list[dict]:
    try:
        import naming_db as ndb
    except Exception:
        return []
    path = Path(db_path)
    if not path.exists():
        return []
    with sqlite3.connect(path) as conn:
        ndb.ensure_schema(conn)
        atoms = ndb.list_source_atoms(
            conn,
            limit=max(1, limit),
            min_confidence=max(0.0, min(1.0, min_confidence)),
            include_inactive=False,
        )

    if languages:
        want = {item.lower() for item in languages}
        atoms = [atom for atom in atoms if str(atom.get('language_hint') or '').lower() in want]
    if categories:
        want = {item.lower() for item in categories}
        atoms = [atom for atom in atoms if str(atom.get('semantic_category') or '').lower() in want]
    return atoms


def merge_generated(
    out: dict[str, GeneratedCandidate],
    *,
    name: str,
    family: str,
    lineage_atoms: list[str],
    source_confidence: float = 0.0,
) -> None:
    normalized = normalize_alpha(name)
    if not normalized:
        return
    existing = out.get(normalized)
    if existing is None or source_confidence > existing.source_confidence:
        out[normalized] = GeneratedCandidate(
            name=normalized,
            generator_family=family,
            lineage_atoms=[normalize_alpha(part) for part in lineage_atoms if normalize_alpha(part)],
            source_confidence=source_confidence,
        )


def source_atom_role(atom: dict) -> str:
    metadata = atom.get('metadata') if isinstance(atom.get('metadata'), dict) else {}
    metadata_role = normalize_alpha(str(metadata.get('morph_role') or metadata.get('role') or ''))
    if metadata_role in {'prefix', 'suffix', 'root'}:
        return metadata_role
    category = normalize_alpha(str(atom.get('semantic_category') or ''))
    if category.endswith('prefix'):
        return 'prefix'
    if category.endswith('suffix'):
        return 'suffix'
    if category in {'root', 'stem'}:
        return 'root'
    return ''


def build_morphology_pools(
    source_atoms: list[dict],
    *,
    variation_profile: str,
) -> tuple[list[tuple[str, float]], list[tuple[str, float]], list[tuple[str, float]]]:
    prefixes: list[tuple[str, float]] = []
    roots: list[tuple[str, float]] = []
    suffixes: list[tuple[str, float]] = []

    for atom in source_atoms:
        token = normalize_alpha(str(atom.get('atom_display') or atom.get('atom_normalized') or ''))
        if not token:
            continue
        conf = float(atom.get('confidence_weight') or 0.0)
        role = source_atom_role(atom)
        if role == 'prefix':
            prefixes.append((token[:5], conf))
        elif role == 'suffix':
            suffixes.append((token[-5:], conf))
        elif role == 'root':
            roots.append((token, conf))
        else:
            roots.append((token, conf))
            if len(token) >= 4:
                prefixes.append((token[:4], conf * 0.7))
                suffixes.append((token[-4:], conf * 0.7))

    if not prefixes:
        prefixes = [(token, 0.52) for token in MORPH_PREFIX_DEFAULTS]
    if not roots:
        roots = [(token, 0.56) for token in MORPH_ROOT_DEFAULTS]
    if not suffixes:
        suffixes = [(token, 0.52) for token in MORPH_SUFFIX_DEFAULTS]

    if variation_profile != 'expanded':
        prefixes = prefixes[:20]
        roots = roots[:30]
        suffixes = suffixes[:20]
    else:
        prefixes = prefixes[:42]
        roots = roots[:64]
        suffixes = suffixes[:42]
    return prefixes, roots, suffixes


def collect_family_candidates(
    *,
    scope: str,
    seeds: Iterable[str],
    variation_profile: str,
    source_atoms: list[dict],
    active_families: list[str],
) -> dict[str, list[GeneratedCandidate]]:
    families: dict[str, dict[str, GeneratedCandidate]] = {}
    active = set(active_families)

    if 'coined' in active:
        generated: dict[str, GeneratedCandidate] = {}
        for p, s in itertools.product(COINED_PREFIXES, COINED_SUFFIXES):
            merge_generated(generated, name=f'{p}{s}', family='coined', lineage_atoms=[p, s])
        families['coined'] = list(generated.values())

    if 'stem' in active:
        generated = {}
        for stem, end in itertools.product(BRAND_STEMS, BRAND_ENDINGS):
            merge_generated(generated, name=f'{stem}{end}', family='stem', lineage_atoms=[stem, end])
        families['stem'] = list(generated.values())

    if 'suggestive' in active:
        generated = {}
        roots = SUGGESTIVE_ROOTS_DACH if scope == 'dach' else SUGGESTIVE_ROOTS_GLOBAL
        for root, suf in itertools.product(roots, SHORT_SUFFIXES):
            merge_generated(generated, name=f'{root}{suf}', family='suggestive', lineage_atoms=[root, suf])
        families['suggestive'] = list(generated.values())

    if 'morphology' in active:
        generated = {}
        prefixes, roots, suffixes = build_morphology_pools(
            source_atoms,
            variation_profile=variation_profile,
        )
        for pref, p_conf in prefixes:
            for root, r_conf in roots:
                blend = f'{pref[:4]}{root[-4:]}'
                merge_generated(
                    generated,
                    name=blend,
                    family='morphology',
                    lineage_atoms=[pref, root],
                    source_confidence=(p_conf + r_conf) / 2.0,
                )
        for root, r_conf in roots:
            for suf, s_conf in suffixes:
                composed = f'{root[:6]}{suf[-3:]}'
                merge_generated(
                    generated,
                    name=composed,
                    family='morphology',
                    lineage_atoms=[root, suf],
                    source_confidence=(r_conf + s_conf) / 2.0,
                )
        # Compound morphs produce broader phonetic spread with low token overlap.
        for (left, l_conf), (right, r_conf) in itertools.product(roots[:36], roots[:36]):
            if left == right:
                continue
            compound = f'{left[:4]}{right[:4]}'
            merge_generated(
                generated,
                name=compound,
                family='morphology',
                lineage_atoms=[left, right],
                source_confidence=(l_conf + r_conf) / 2.0,
            )
        families['morphology'] = list(generated.values())

    if 'seed' in active:
        generated = {}
        for seed in seeds:
            base = normalize_alpha(seed)
            if not base:
                continue
            merge_generated(generated, name=base, family='seed', lineage_atoms=[base], source_confidence=0.6)
            for suf in SHORT_SUFFIXES:
                merge_generated(generated, name=f'{base}{suf}', family='seed', lineage_atoms=[base, suf], source_confidence=0.6)
            for end in BRAND_ENDINGS:
                merge_generated(
                    generated,
                    name=f'{base[:6]}{end}',
                    family='seed',
                    lineage_atoms=[base[:6], end],
                    source_confidence=0.6,
                )
            for p in COINED_PREFIXES[:8]:
                merge_generated(generated, name=f'{p}{base[:3]}', family='seed', lineage_atoms=[p, base[:3]], source_confidence=0.6)
        families['seed'] = list(generated.values())

    if 'expression' in active and variation_profile == 'expanded':
        generated = {}
        for expr in GLOBAL_EXPRESSIONS:
            merge_generated(generated, name=expr, family='expression', lineage_atoms=[expr], source_confidence=0.55)
            for end in BRAND_ENDINGS[:6]:
                merge_generated(
                    generated,
                    name=f'{expr[:7]}{end}',
                    family='expression',
                    lineage_atoms=[expr[:7], end],
                    source_confidence=0.55,
                )
        families['expression'] = list(generated.values())

    if 'source_pool' in active:
        generated = {}
        normalized_atoms: list[tuple[str, float]] = []
        for atom in source_atoms:
            value = normalize_alpha(str(atom.get('atom_display') or atom.get('atom_normalized') or ''))
            if not value:
                continue
            normalized_atoms.append((value, float(atom.get('confidence_weight') or 0.0)))

        for atom, conf in normalized_atoms:
            merge_generated(
                generated,
                name=atom,
                family='source_pool',
                lineage_atoms=[atom],
                source_confidence=conf,
            )
            for end in BRAND_ENDINGS + SHORT_SUFFIXES[:5]:
                merge_generated(
                    generated,
                    name=f'{atom[:8]}{end}',
                    family='source_pool',
                    lineage_atoms=[atom, end],
                    source_confidence=conf,
                )
        families['source_pool'] = list(generated.values())

    if 'blend' in active:
        generated = {}
        base_roots = GLOBAL_VARIATION_ROOTS if variation_profile == 'expanded' else GLOBAL_VARIATION_ROOTS[:10]
        atoms_for_blend: list[tuple[str, float]] = []
        for atom in source_atoms:
            value = normalize_alpha(str(atom.get('atom_display') or atom.get('atom_normalized') or ''))
            if value:
                atoms_for_blend.append((value, float(atom.get('confidence_weight') or 0.0)))
        if not atoms_for_blend:
            atoms_for_blend = [(root, 0.5) for root in base_roots]

        for (left, l_conf), (right, r_conf) in itertools.product(atoms_for_blend[:36], atoms_for_blend[:36]):
            if left == right:
                continue
            blend = f'{left[:4]}{right[-3:]}'
            merge_generated(
                generated,
                name=blend,
                family='blend',
                lineage_atoms=[left, right],
                source_confidence=(l_conf + r_conf) / 2.0,
            )
        families['blend'] = list(generated.values())

    return families


def pattern_shape(name: str) -> str:
    return ''.join('v' if ch in 'aeiouy' else 'c' for ch in name)


def diversity_filter(
    generated: list[GeneratedCandidate],
    *,
    max_per_prefix2: int,
    max_per_suffix2: int,
    max_per_shape: int,
    max_per_family: int,
) -> list[GeneratedCandidate]:
    out: list[GeneratedCandidate] = []
    prefix_counts: Counter[str] = Counter()
    suffix_counts: Counter[str] = Counter()
    shape_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()

    ordered = sorted(
        generated,
        key=lambda item: (-item.source_confidence, len(item.name), item.name),
    )
    for item in ordered:
        prefix = item.name[:2]
        suffix = item.name[-2:]
        shape = pattern_shape(item.name)
        if family_counts[item.generator_family] >= max_per_family:
            continue
        if prefix_counts[prefix] >= max_per_prefix2:
            continue
        if suffix_counts[suffix] >= max_per_suffix2:
            continue
        if shape_counts[shape] >= max_per_shape:
            continue
        out.append(item)
        family_counts[item.generator_family] += 1
        prefix_counts[prefix] += 1
        suffix_counts[suffix] += 1
        shape_counts[shape] += 1
    return out


def generate_candidates(
    scope: str,
    seeds: Iterable[str],
    min_len: int,
    max_len: int,
    variation_profile: str,
    generator_families: list[str],
    family_quotas: dict[str, int],
    source_atoms: list[dict],
    max_per_prefix2: int,
    max_per_suffix2: int,
    max_per_shape: int,
    max_per_family: int,
    filter_engine: CandidateFilter | None = None,
) -> list[GeneratedCandidate]:
    families = collect_family_candidates(
        scope=scope,
        seeds=seeds,
        variation_profile=variation_profile,
        source_atoms=source_atoms,
        active_families=generator_families,
    )

    selected: list[GeneratedCandidate] = []
    seen: set[str] = set()
    for family in generator_families:
        members = families.get(family, [])
        quota = max(0, family_quotas.get(family, DEFAULT_FAMILY_QUOTAS.get(family, 120)))
        for item in members[:quota]:
            n = normalize_alpha(item.name)
            if not n or n in seen:
                continue
            if len(n) < min_len or len(n) > max_len:
                continue
            if not re.fullmatch(r'[a-z]+', n):
                continue
            if len(n) > 2 and n[0] == n[1] == n[2]:
                continue
            selected.append(
                GeneratedCandidate(
                    name=n,
                    generator_family=item.generator_family,
                    lineage_atoms=item.lineage_atoms,
                    source_confidence=item.source_confidence,
                )
            )
            seen.add(n)

    filter_request = FilterRequest(
        generated=selected,
        max_per_prefix2=max_per_prefix2,
        max_per_suffix2=max_per_suffix2,
        max_per_shape=max_per_shape,
        max_per_family=max_per_family,
    )
    active_filter = filter_engine or PrefixSuffixShapeFilter()
    return active_filter.apply(filter_request)


def vowel_ratio(name: str) -> float:
    vowels = sum(1 for c in name if c in 'aeiouy')
    return vowels / max(1, len(name))


def length_score(name: str) -> int:
    diff = abs(len(name) - 8)
    return max(0, 35 - diff * 5)


def pronounceability_score(name: str) -> int:
    score = 35
    ratio = vowel_ratio(name)
    score -= int(min(18, abs(ratio - 0.46) * 70))
    for cluster in ('tsch', 'schr', 'xtr', 'ptk', 'qz', 'yy', 'iii'):
        if cluster in name:
            score -= 5
    if re.search(r'(.)\1\1', name):
        score -= 5
    return max(0, score)


def memorability_score(name: str) -> int:
    score = 30
    unique = len(set(name))
    if unique < 4:
        score -= 10
    if len(name) <= 6:
        score += 2
    if len(name) >= 11:
        score -= 8
    for seq in ('aiv', 'pax', 'paz', 'xo', 'xx', 'qz', 'zz', 'ass', 'cle', 'hom'):
        if seq in name:
            score -= 6
    if name.endswith(('uti', 'ass', 'cle', 'hom', 'fol')):
        score -= 6
    return max(0, min(30, score))


def quality_score(name: str) -> int:
    return max(0, min(100, length_score(name) + pronounceability_score(name) + memorability_score(name)))


def max_similarity(name: str) -> tuple[float, str]:
    best = 0.0
    closest = ''
    for mark in PROTECTED_MARKS:
        ratio = SequenceMatcher(None, name, mark).ratio()
        if name[:4] == mark[:4]:
            ratio = max(ratio, 0.82)
        if name[:5] == mark[:5]:
            ratio = max(ratio, 0.88)
        if ratio > best:
            best = ratio
            closest = mark
    return best, closest


def descriptive_risk(name: str) -> int:
    risk = 0
    for token in GENERIC_TOKENS:
        if token in name:
            risk += 18
    if any(t in name for t in ('umlage', 'neben', 'kosten', 'abrechn')):
        risk += 12
    return min(100, risk)


def scope_penalty(name: str, scope: str) -> int:
    if scope == 'dach':
        return 0
    penalty = 0
    for token in GERMAN_HEAVY_TOKENS:
        if token in name:
            penalty += 12
    if scope == 'global':
        penalty += 6 * sum(1 for t in ('immo', 'miet', 'wohn') if t in name)
    return min(60, penalty)


def challenge_risk(name: str, scope: str) -> tuple[int, int, str, int, int]:
    sim, closest = max_similarity(name)
    sim_risk = int(sim * 100)
    desc_risk = descriptive_risk(name)
    sc_pen = scope_penalty(name, scope)
    risk = int(min(100, 0.55 * sim_risk + 0.3 * desc_risk + 0.15 * sc_pen))
    return risk, sim_risk, closest, desc_risk, sc_pen


def similarity_with_prefix_boost(name: str, mark: str) -> float:
    ratio = SequenceMatcher(None, name, mark).ratio()
    if len(name) >= 4 and len(mark) >= 4 and name[:4] == mark[:4]:
        ratio = max(ratio, 0.82)
    if len(name) >= 5 and len(mark) >= 5 and name[:5] == mark[:5]:
        ratio = max(ratio, 0.88)
    if len(name) >= 3 and len(mark) >= 3 and name[-3:] == mark[-3:]:
        ratio = max(ratio, 0.76)
    return min(1.0, ratio)


def adversarial_similarity_signal(name: str) -> tuple[int, str]:
    scored: list[tuple[str, int]] = []
    for mark in ADVERSARIAL_MARKS:
        ratio = int(similarity_with_prefix_boost(name, mark) * 100)
        if ratio >= 68:
            scored.append((mark, ratio))
    scored.sort(key=lambda item: (-item[1], item[0]))
    top = scored[:3]
    if not top:
        return 0, ''
    top_str = ';'.join(f'{mark}:{score}' for mark, score in top)
    risk = min(100, max(score for _, score in top))
    return risk, top_str


def psych_spelling_risk(name: str) -> int:
    risk = 0
    if any(ch in name for ch in ('q', 'x', 'y')):
        risk += 8
    if 'ph' in name:
        risk += 6
    if 'sch' in name and len(name) <= 7:
        risk += 4
    if any(token in name for token in ('ck', 'tz', 'th', 'gh')):
        risk += 4
    if re.search(r'[aeiou]{3,}', name):
        risk += 6
    if re.search(r'[^aeiou]{4,}', name):
        risk += 6
    if name.startswith(('c', 'k')) and 'c' in name and 'k' in name:
        risk += 6
    return min(100, risk)


def psych_trust_proxy_score(name: str) -> int:
    score = 70
    if len(name) < 6 or len(name) > 11:
        score -= 10
    ratio = vowel_ratio(name)
    if ratio < 0.28 or ratio > 0.62:
        score -= 10
    if any(token in name for token in ('easy', 'smart', 'cheap', 'quick', 'fun')):
        score -= 14
    if any(token in name for token in ('audit', 'legal', 'cert', 'secure', 'trust')):
        score += 6
    score -= int(psych_spelling_risk(name) * 0.4)
    return max(0, min(100, score))


def gibberish_signal(name: str) -> tuple[int, str]:
    penalty = 0
    flags: list[str] = []

    cons_runs = re.findall(r'[^aeiouy]+', name)
    vow_runs = re.findall(r'[aeiouy]+', name)
    max_cons = max((len(run) for run in cons_runs), default=0)
    max_vow = max((len(run) for run in vow_runs), default=0)

    if max_cons >= 5:
        penalty += 30
        flags.append('cons_run_5plus')
    elif max_cons == 4:
        penalty += 18
        flags.append('cons_run_4')

    if max_vow >= 4:
        penalty += 16
        flags.append('vowel_run_4plus')

    if len(name) >= 7 and name[:3] == name[3:6]:
        penalty += 22
        flags.append('repeated_trigram')

    if re.search(r'(aa|ee|ii|oo|uu|yyy)', name):
        penalty += 12
        flags.append('double_vowel_repeat')

    if re.search(r'([a-z]{2})\1', name):
        penalty += 14
        flags.append('repeated_bigram')

    if name.endswith(('oon', 'oto')):
        penalty += 16
        flags.append('synthetic_suffix')

    unique_ratio = len(set(name)) / max(1, len(name))
    if unique_ratio < 0.45:
        penalty += 14
        flags.append('low_char_diversity')

    for bigram in GIBBERISH_BIGRAMS:
        if bigram in name:
            penalty += 8
            flags.append(f'odd_bigram_{bigram}')

    if len(name) >= 4 and not re.search(r'[aeiouy]', name[:4]):
        penalty += 10
        flags.append('no_early_vowel')

    return min(100, penalty), ';'.join(sorted(set(flags)))


def load_false_friend_rules(path: str) -> dict[str, tuple[int, str]]:
    rules = dict(FALSE_FRIEND_RULES)
    p = Path(path)
    if not p.exists():
        return rules
    for raw in p.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line.startswith('|'):
            continue
        parts = [part.strip() for part in line.strip('|').split('|')]
        if len(parts) < 3:
            continue
        token = normalize_alpha(parts[0])
        if token in {'token', 'name'} or not token:
            continue
        try:
            weight = int(parts[1])
        except ValueError:
            continue
        reason = parts[2] or 'lexicon_rule'
        rules[token] = (max(1, min(100, weight)), reason)
    return rules


def false_friend_signal(name: str, rules: dict[str, tuple[int, str]] | None = None) -> tuple[int, str]:
    active_rules = rules or FALSE_FRIEND_RULES
    total = 0
    hits: list[str] = []
    for token, (weight, reason) in active_rules.items():
        if token in name:
            total += weight
            hits.append(f'{token}:{reason}:{weight}')
    if re.search(r'(sex|xxx|porn)', name):
        total += 40
        hits.append('explicit_content:blocked:40')
    return min(100, total), ';'.join(hits[:6])


def trademark_search_urls(name: str) -> tuple[str, str, str]:
    dpma = (
        'https://register.dpma.de/DPMAregister/marke/register/erweitert'
        '?queryString='
        + parse.quote(name)
    )
    swissreg = (
        'https://www.swissreg.ch/srclient/faces/jsp/trademark/sr300.jsp'
        '?language=de&searchText='
        + parse.quote(name)
    )
    tmview = 'https://www.tmdn.org/tmview/#/tmsearch?page=1&criteria=' + parse.quote(name)
    return dpma, swissreg, tmview


def fetch_json(url: str, timeout: float = 8.0, retries: int = 2) -> dict | None:
    req = request.Request(url, headers={'User-Agent': USER_AGENT})
    for i in range(retries + 1):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode('utf-8', errors='replace')
                return json.loads(data)
        except Exception:
            if i == retries:
                return None
            time.sleep(0.4 * (i + 1))
    return None


def fetch_text(url: str, timeout: float = 8.0, retries: int = 2) -> str | None:
    req = request.Request(url, headers={'User-Agent': USER_AGENT})
    for i in range(retries + 1):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode('utf-8', errors='replace')
        except Exception:
            if i == retries:
                return None
            time.sleep(0.4 * (i + 1))
    return None


def fetch_status(url: str, timeout: float = 8.0, retries: int = 1, method: str = 'GET') -> int | None:
    req = request.Request(url, headers={'User-Agent': USER_AGENT}, method=method)
    for i in range(retries + 1):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                _ = resp.read(64)
                return int(resp.status)
        except error.HTTPError as e:
            return int(e.code)
        except Exception:
            if i == retries:
                return None
            time.sleep(0.25 * (i + 1))
    return None


def package_exists_on_pypi(name: str) -> str:
    status = fetch_status(f'https://pypi.org/pypi/{name}/json', timeout=8.0, retries=1)
    if status == 200:
        return 'yes'
    if status == 404:
        return 'no'
    return 'unknown'


def package_exists_on_npm(name: str) -> str:
    status = fetch_status(f'https://registry.npmjs.org/{name}', timeout=8.0, retries=1)
    if status == 200:
        return 'yes'
    if status == 404:
        return 'no'
    return 'unknown'


def handle_available(url: str) -> str:
    status = fetch_status(url, timeout=8.0, retries=1, method='GET')
    if status in {404, 410}:
        return 'yes'
    if status in {200, 301, 302, 307, 308, 401, 403, 429}:
        return 'no'
    return 'unknown'


def social_handle_signal(name: str) -> tuple[str, str, str, str, int, int]:
    github = handle_available(f'https://github.com/{name}')
    linkedin = handle_available(f'https://www.linkedin.com/company/{name}')
    x_handle = handle_available(f'https://x.com/{name}')
    instagram = handle_available(f'https://www.instagram.com/{name}/')
    states = [github, linkedin, x_handle, instagram]
    unavailable_count = sum(1 for s in states if s == 'no')
    unknown_count = sum(1 for s in states if s == 'unknown')
    return github, linkedin, x_handle, instagram, unavailable_count, unknown_count


def app_store_signal(name: str, country: str) -> tuple[int, bool, bool]:
    url = (
        'https://itunes.apple.com/search?'
        + parse.urlencode({'term': name, 'entity': 'software', 'country': country, 'limit': 8})
    )
    data = fetch_json(url)
    if not data:
        return -1, False, False

    count = int(data.get('resultCount', 0))
    exact = False
    for item in data.get('results', []):
        track = normalize_alpha(str(item.get('trackName', '')))
        if track == name:
            exact = True
            break
    return count, exact, True


def rdap_available(name: str, tld: str) -> str:
    endpoints = {
        'com': f'https://rdap.verisign.com/com/v1/domain/{name}.com',
        'de': f'https://rdap.denic.de/domain/{name}.de',
        'ch': f'https://rdap.nic.ch/domain/{name}.ch',
    }
    req = request.Request(endpoints[tld], headers={'User-Agent': USER_AGENT})
    try:
        with request.urlopen(req, timeout=8.0) as resp:
            _ = resp.read(64)
            if resp.status == 200:
                return 'no'
            status = 'unknown'
    except error.HTTPError as e:
        if e.code == 404:
            return 'yes'
        status = 'unknown'
    except Exception:
        status = 'unknown'

    # Secondary lookup to reduce false "unknown" responses from registry-specific RDAP endpoints.
    fallback = rdap_available_fqdn(f'{name}.{tld}')
    if fallback in {'yes', 'no'}:
        return fallback
    return status


def rdap_available_fqdn(fqdn: str) -> str:
    req = request.Request(f'https://rdap.org/domain/{fqdn}', headers={'User-Agent': USER_AGENT})
    try:
        with request.urlopen(req, timeout=8.0) as resp:
            _ = resp.read(64)
            if resp.status == 200:
                return 'no'
            return 'unknown'
    except error.HTTPError as e:
        if e.code == 404:
            return 'yes'
        return 'unknown'
    except Exception:
        return 'unknown'


def best_com_fallback(name: str) -> tuple[str, str]:
    candidates = [
        f'get{name}.com',
        f'use{name}.com',
        f'{name}app.com',
        f'{name}hq.com',
        f'{name}cloud.com',
    ]
    for fqdn in candidates:
        avail = rdap_available_fqdn(fqdn)
        if avail == 'yes':
            return 'yes', fqdn
    return 'no', ''


def extract_result_domain(raw_href: str) -> str:
    href = html.unescape(raw_href)
    if href.startswith('//'):
        href = f'https:{href}'
    if 'duckduckgo.com/l/?' in href:
        try:
            parsed = parse.urlparse(href)
            params = parse.parse_qs(parsed.query)
            target = params.get('uddg', [''])[0]
            if target:
                href = parse.unquote(target)
        except Exception:
            return ''
    if 'bing.com/ck/a' in href:
        try:
            parsed = parse.urlparse(href)
            params = parse.parse_qs(parsed.query)
            target = params.get('u', [''])[0]
            if target.startswith('a1'):
                encoded = target[2:]
                encoded += '=' * ((4 - len(encoded) % 4) % 4)
                href = base64.urlsafe_b64decode(encoded.encode('ascii')).decode('utf-8', errors='replace')
        except Exception:
            pass
    try:
        parsed = parse.urlparse(href if '://' in href else f'https://{href}')
    except Exception:
        return ''
    domain = parsed.netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain


def parse_ddg_results(page: str) -> list[tuple[str, str]]:
    return re.findall(
        r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        page,
        re.IGNORECASE | re.DOTALL,
    )


def parse_bing_results(page: str) -> list[tuple[str, str]]:
    return re.findall(
        r'<li[^>]*class="b_algo"[^>]*>.*?<h2[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        page,
        re.IGNORECASE | re.DOTALL,
    )


def fetch_search_matches(query: str) -> tuple[list[tuple[str, str]], bool, str]:
    source = 'ddg'
    url = 'https://duckduckgo.com/html/?' + parse.urlencode({'q': query})
    page = fetch_text(url, timeout=12.0, retries=2)
    if page is not None:
        return parse_ddg_results(page), True, source
    source = 'bing'
    url = 'https://www.bing.com/search?' + parse.urlencode({'q': query})
    page = fetch_text(url, timeout=12.0, retries=2)
    if page is None:
        return [], False, ''
    return parse_bing_results(page), True, source


def web_collision_signal(name: str, top_n: int) -> tuple[int, int, int, str, bool, str]:
    quoted_matches, quoted_ok, quoted_source = fetch_search_matches(f'"{name}"')
    plain_matches, plain_ok, plain_source = fetch_search_matches(name)

    if not quoted_ok and not plain_ok:
        return -1, -1, -1, '', False, ''

    if quoted_ok and plain_ok:
        source = f'{quoted_source}+{plain_source}'
    elif quoted_ok:
        source = quoted_source
    else:
        source = plain_source

    exact_hits = 0
    near_hits = 0
    sample_domains: list[str] = []
    seen_domains: set[str] = set()

    quoted_slice = quoted_matches[:top_n]
    plain_slice = plain_matches[:top_n]

    for href, raw_title in quoted_slice + plain_slice:
        title = html.unescape(re.sub(r'<[^>]+>', ' ', raw_title))
        title_lc = title.lower()
        title_norm = normalize_alpha(title)
        if title_norm == name or re.search(rf'(^|[^a-z0-9]){re.escape(name)}([^a-z0-9]|$)', title_lc):
            exact_hits += 1

    for href, raw_title in plain_slice:
        title = html.unescape(re.sub(r'<[^>]+>', ' ', raw_title))
        title_lc = title.lower()
        tokens = set(re.findall(r'[a-z]{4,}', title_lc))
        for token in tokens:
            if token == name:
                continue
            ratio = SequenceMatcher(None, token, name).ratio()
            if ratio >= 0.86 and abs(len(token) - len(name)) <= 2:
                near_hits += 1
                break

        domain = extract_result_domain(href)
        if domain and domain not in seen_domains and len(sample_domains) < 4:
            sample_domains.append(domain)
            seen_domains.add(domain)

    total_results = len(quoted_matches) + len(plain_matches)
    return exact_hits, near_hits, total_results, ';'.join(sample_domains), True, source


def required_tlds(scope: str) -> list[str]:
    if scope == 'dach':
        return ['de', 'ch']
    if scope == 'eu':
        return ['de', 'ch', 'com']
    return ['com']


def evaluate_candidates(
    scope: str,
    generated_items: list[GeneratedCandidate],
    similarity_fail_threshold: int,
    false_friend_fail_threshold: int,
    gibberish_fail_threshold: int,
    false_friend_rules: dict[str, tuple[int, str]],
) -> list[Candidate]:
    results: list[Candidate] = []
    for item in generated_items:
        n = item.name
        q = quality_score(n)
        risk, sim_risk, closest, desc_risk, sc_pen = challenge_risk(n, scope)
        adv_risk, adv_hits = adversarial_similarity_signal(n)
        gib_penalty, gib_flags = gibberish_signal(n)
        false_friend_risk, false_friend_hits = false_friend_signal(n, false_friend_rules)
        risk = min(100, risk + int(0.25 * adv_risk))
        risk = min(100, risk + int(gib_penalty * 0.4) + int(false_friend_risk * 0.65))
        total = max(0, min(100, int(q - (risk * 0.55))))
        spell_risk = psych_spelling_risk(n)
        trust_proxy = psych_trust_proxy_score(n)
        dpma_url, swissreg_url, tmview_url = trademark_search_urls(n)
        c = Candidate(
            name=n,
            generator_family=item.generator_family,
            lineage_atoms=';'.join(item.lineage_atoms),
            source_confidence=float(item.source_confidence),
            quality_score=q,
            challenge_risk=risk,
            total_score=total,
            descriptive_risk=desc_risk,
            similarity_risk=sim_risk,
            closest_mark=closest,
            scope_penalty=sc_pen,
            adversarial_risk=adv_risk,
            adversarial_top_hits=adv_hits,
            psych_spelling_risk=spell_risk,
            psych_trust_proxy=trust_proxy,
            trademark_dpma_url=dpma_url,
            trademark_swissreg_url=swissreg_url,
            trademark_tmview_url=tmview_url,
            gibberish_penalty=gib_penalty,
            gibberish_flags=gib_flags,
            false_friend_risk=false_friend_risk,
            false_friend_hits=false_friend_hits,
        )
        if sim_risk >= similarity_fail_threshold:
            c.hard_fail = True
            c.fail_reason = f'similar_to_{closest}'
        if adv_risk >= max(82, similarity_fail_threshold):
            c.hard_fail = True
            if not c.fail_reason:
                c.fail_reason = 'adversarial_similarity_risk'
        if gib_penalty >= gibberish_fail_threshold:
            c.hard_fail = True
            if not c.fail_reason:
                c.fail_reason = 'gibberish_pattern_risk'
        if false_friend_risk >= false_friend_fail_threshold:
            c.hard_fail = True
            if not c.fail_reason:
                c.fail_reason = 'false_friend_risk'
        results.append(c)
    return results


def mark_fail(c: Candidate, reason: str) -> None:
    c.hard_fail = True
    if not c.fail_reason:
        c.fail_reason = reason


def apply_external_penalty(c: Candidate) -> None:
    penalty = 0
    if c.web_exact_hits > 0:
        penalty += min(30, c.web_exact_hits * 12)
    if c.web_near_hits > 0:
        penalty += min(14, c.web_near_hits * 4)
    if c.pypi_exists == 'yes':
        penalty += 8
    if c.npm_exists == 'yes':
        penalty += 8
    if c.social_unavailable_count > 0:
        penalty += min(10, c.social_unavailable_count * 3)
    if c.social_unknown_count > 0:
        penalty += min(8, c.social_unknown_count * 2)
    if c.adversarial_risk >= 70:
        penalty += min(16, int((c.adversarial_risk - 65) * 0.6))
    # Lower trust/spelling robustness should reduce rank before user tests.
    if c.psych_trust_proxy < 55:
        penalty += int((55 - c.psych_trust_proxy) * 0.35)
    penalty += int(min(10, c.psych_spelling_risk * 0.25))
    penalty += int(min(12, c.gibberish_penalty * 0.25))
    penalty += int(min(16, c.false_friend_risk * 0.45))
    unknown_store_count = len([p for p in c.store_unknown_countries.split(',') if p.strip()])
    if unknown_store_count > 0:
        penalty += min(8, unknown_store_count * 2)
    c.external_penalty = penalty
    c.challenge_risk = min(100, c.challenge_risk + penalty)
    c.total_score = max(0, min(100, c.total_score - int(0.45 * penalty)))


def run_external_checks(
    candidates: list[Candidate],
    scope: str,
    throttle_ms: int,
    gate: str,
    store_countries: list[str],
    store_check: bool,
    web_check: bool,
    web_top: int,
    domain_check: bool,
    require_base_com: bool,
    fail_on_unknown: bool,
    package_check: bool,
    social_check: bool,
    adversarial_fail_threshold: int,
    show_progress: bool,
    degraded_network_mode: bool,
) -> None:
    total = len(candidates)
    batch_start = time.monotonic()
    req_tlds = required_tlds(scope)
    for idx, c in enumerate(candidates, start=1):
        candidate_start = time.monotonic()
        if show_progress:
            elapsed = time.monotonic() - batch_start
            print(
                f'[{idx}/{total}] checking {c.name} (elapsed={elapsed:.1f}s)',
                flush=True,
            )
        exact_countries: list[str] = []
        unknown_countries: list[str] = []
        if store_check:
            for country in store_countries:
                count, exact, ok = app_store_signal(c.name, country)
                if country == 'de':
                    c.store_de_count, c.store_de_exact = count, exact
                elif country == 'ch':
                    c.store_ch_count, c.store_ch_exact = count, exact
                elif country == 'us':
                    c.store_us_count, c.store_us_exact = count, exact
                if not ok:
                    unknown_countries.append(country)
                if exact:
                    exact_countries.append(country)
            c.store_exact_countries = ','.join(exact_countries)
            c.store_unknown_countries = ','.join(unknown_countries)
        else:
            c.store_de_count = -2
            c.store_ch_count = -2
            c.store_us_count = -2
            c.store_exact_countries = ''
            c.store_unknown_countries = ''

        if domain_check:
            c.com_available = rdap_available(c.name, 'com')
            c.de_available = rdap_available(c.name, 'de')
            c.ch_available = rdap_available(c.name, 'ch')
            c.com_fallback_available, c.com_fallback_domain = best_com_fallback(c.name)
        else:
            c.com_available = 'unknown'
            c.de_available = 'unknown'
            c.ch_available = 'unknown'
            c.com_fallback_available = 'unknown'
            c.com_fallback_domain = ''

        if web_check:
            (
                c.web_exact_hits,
                c.web_near_hits,
                c.web_result_count,
                c.web_sample_domains,
                web_ok,
                c.web_source,
            ) = web_collision_signal(c.name, top_n=web_top)
        else:
            c.web_exact_hits, c.web_near_hits, c.web_result_count, c.web_sample_domains, web_ok, c.web_source = (
                0,
                0,
                0,
                '',
                True,
                'disabled',
            )

        if package_check:
            c.pypi_exists = package_exists_on_pypi(c.name)
            c.npm_exists = package_exists_on_npm(c.name)
        else:
            c.pypi_exists = 'unknown'
            c.npm_exists = 'unknown'

        if social_check:
            (
                c.social_github_available,
                c.social_linkedin_available,
                c.social_x_available,
                c.social_instagram_available,
                c.social_unavailable_count,
                c.social_unknown_count,
            ) = social_handle_signal(c.name)
        else:
            (
                c.social_github_available,
                c.social_linkedin_available,
                c.social_x_available,
                c.social_instagram_available,
            ) = ('unknown', 'unknown', 'unknown', 'unknown')
            c.social_unavailable_count = 0
            c.social_unknown_count = 4

        apply_external_penalty(c)

        if store_check and exact_countries:
            mark_fail(c, f'exact_app_store_collision_{"-".join(exact_countries)}')

        if web_check and c.web_exact_hits > 0:
            mark_fail(c, 'web_exact_collision')

        if gate == 'strict' and web_check and c.web_near_hits >= 2:
            mark_fail(c, 'web_near_collision')

        if gate == 'strict' and package_check and (c.pypi_exists == 'yes' or c.npm_exists == 'yes'):
            mark_fail(c, 'package_namespace_collision')

        if c.adversarial_risk >= adversarial_fail_threshold:
            mark_fail(c, 'adversarial_confusion_risk')

        if domain_check:
            for tld in req_tlds:
                avail = {'com': c.com_available, 'de': c.de_available, 'ch': c.ch_available}[tld]
                if avail == 'unknown':
                    if degraded_network_mode:
                        continue
                    if fail_on_unknown:
                        mark_fail(c, f'required_domain_{tld}_unknown')
                        break
                    # In balanced mode, allow unknown domain state as soft signal.
                    continue
                # If .com is taken, allow viable fallback domain for global/eu naming exploration.
                if tld == 'com' and not require_base_com and avail != 'yes' and c.com_fallback_available == 'yes':
                    continue
                if avail != 'yes':
                    mark_fail(c, f'required_domain_{tld}_not_available')
                    break

            if require_base_com:
                if c.com_available == 'yes':
                    pass
                elif c.com_available == 'unknown' and (degraded_network_mode or not fail_on_unknown):
                    pass
                else:
                    mark_fail(c, 'base_com_not_available')

        if store_check and fail_on_unknown and c.store_unknown_countries:
            mark_fail(c, 'app_store_check_unknown')

        if fail_on_unknown and web_check and not web_ok:
            mark_fail(c, 'web_check_unknown')

        if fail_on_unknown and package_check and ('unknown' in {c.pypi_exists, c.npm_exists}):
            mark_fail(c, 'package_check_unknown')

        if fail_on_unknown and social_check and c.social_unknown_count > 0:
            mark_fail(c, 'social_check_unknown')

        if show_progress:
            duration = time.monotonic() - candidate_start
            status = 'FAIL' if c.hard_fail else 'PASS'
            print(
                f'  -> {status} {c.name} | t={duration:.1f}s | '
                f'domain(com/de/ch)={c.com_available}/{c.de_available}/{c.ch_available} | '
                f'web={c.web_exact_hits}/{c.web_near_hits} | '
                f'pkg={c.pypi_exists}/{c.npm_exists} | '
                f'adv={c.adversarial_risk} | reason={c.fail_reason or "-"}',
                flush=True,
            )

        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)

    if show_progress:
        total_duration = time.monotonic() - batch_start
        print(f'Completed external checks for {total} candidates in {total_duration:.1f}s', flush=True)


def recommendation(c: Candidate, gate: str) -> str:
    if c.hard_fail:
        return 'reject'
    if gate == 'strict':
        if c.challenge_risk <= 24 and c.total_score >= 66:
            return 'strong'
        if c.challenge_risk <= 34 and c.total_score >= 56:
            return 'consider'
        return 'weak'
    if c.challenge_risk <= 32 and c.total_score >= 62:
        return 'strong'
    if c.challenge_risk <= 45 and c.total_score >= 52:
        return 'consider'
    return 'weak'


def shortlist_bucket(name: str) -> str:
    normalized = normalize_alpha(name)
    if not normalized:
        return 'x|x|x'
    prefix = normalized[:2]
    suffix = normalized[-2:]
    shape = pattern_shape(normalized)[:6]
    return f'{prefix}|{suffix}|{shape}'


def phonetic_fingerprint(name: str) -> str:
    normalized = normalize_alpha(name)
    if not normalized:
        return ''
    folded = normalized
    replacements = (
        ('sch', 's'),
        ('ph', 'f'),
        ('ck', 'k'),
        ('qu', 'k'),
        ('x', 'ks'),
        ('z', 's'),
    )
    for src, dst in replacements:
        folded = folded.replace(src, dst)
    first = folded[0]
    tail = re.sub(r'[aeiouy]', '', folded[1:])
    collapsed = re.sub(r'(.)\1+', r'\1', tail)
    return (first + collapsed)[:6]


def rerank_with_diversity(
    candidates: list[Candidate],
    *,
    gate: str,
    shortlist_size: int,
    max_per_bucket: int,
    max_per_prefix3: int,
    max_per_phonetic: int,
) -> list[Candidate]:
    if shortlist_size <= 0 or not candidates:
        return candidates

    bucket_counts: Counter[str] = Counter()
    prefix_counts: Counter[str] = Counter()
    phonetic_counts: Counter[str] = Counter()
    selected: list[Candidate] = []
    deferred: list[Candidate] = []

    for candidate in candidates:
        if candidate.hard_fail:
            deferred.append(candidate)
            candidate.shortlist_selected = False
            candidate.shortlist_rank = 0
            candidate.shortlist_reason = 'hard_fail'
            continue

        bucket = shortlist_bucket(candidate.name)
        prefix3 = candidate.name[:3]
        phonetic_key = phonetic_fingerprint(candidate.name)
        allow = (
            bucket_counts[bucket] < max_per_bucket
            and prefix_counts[prefix3] < max_per_prefix3
            and phonetic_counts[phonetic_key] < max_per_phonetic
            and len(selected) < shortlist_size
        )
        candidate.shortlist_bucket = bucket
        if allow:
            bucket_counts[bucket] += 1
            prefix_counts[prefix3] += 1
            phonetic_counts[phonetic_key] += 1
            candidate.shortlist_selected = True
            candidate.shortlist_reason = (
                f'diversity_accept bucket={bucket} bucket_count={bucket_counts[bucket]} '
                f'prefix3={prefix3} prefix_count={prefix_counts[prefix3]} '
                f'phonetic={phonetic_key} phonetic_count={phonetic_counts[phonetic_key]}'
            )
            selected.append(candidate)
        else:
            candidate.shortlist_selected = False
            if len(selected) >= shortlist_size:
                candidate.shortlist_reason = 'shortlist_capacity_reached'
            elif bucket_counts[bucket] >= max_per_bucket:
                candidate.shortlist_reason = f'bucket_quota_reached:{bucket}'
            elif prefix_counts[prefix3] >= max_per_prefix3:
                candidate.shortlist_reason = f'prefix3_quota_reached:{prefix3}'
            elif phonetic_counts[phonetic_key] >= max_per_phonetic:
                candidate.shortlist_reason = f'phonetic_quota_reached:{phonetic_key}'
            else:
                candidate.shortlist_reason = 'deferred'
            deferred.append(candidate)

    for idx, candidate in enumerate(selected, start=1):
        candidate.shortlist_rank = idx

    ordered_deferred = sorted(
        deferred,
        key=lambda c: (
            c.hard_fail,
            {'strong': 0, 'consider': 1, 'weak': 2, 'reject': 3}[recommendation(c, gate)],
            c.challenge_risk,
            -c.total_score,
            c.name,
        ),
    )
    return selected + ordered_deferred


def write_csv(path: Path, scope: str, candidates: list[Candidate], gate: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(
            [
                'name',
                'generator_family',
                'lineage_atoms',
                'source_confidence',
                'scope',
                'gate',
                'quality_score',
                'challenge_risk',
                'total_score',
                'descriptive_risk',
                'similarity_risk',
                'closest_mark',
                'scope_penalty',
                'itunes_de_count',
                'itunes_de_exact',
                'itunes_ch_count',
                'itunes_ch_exact',
                'itunes_us_count',
                'itunes_us_exact',
                'itunes_exact_countries',
                'itunes_unknown_countries',
                'domain_com_available',
                'domain_com_fallback_available',
                'domain_com_fallback_domain',
                'domain_de_available',
                'domain_ch_available',
                'web_result_count',
                'web_exact_hits',
                'web_near_hits',
                'web_sample_domains',
                'web_source',
                'pypi_exists',
                'npm_exists',
                'social_github_available',
                'social_linkedin_available',
                'social_x_available',
                'social_instagram_available',
                'social_unavailable_count',
                'social_unknown_count',
                'adversarial_risk',
                'adversarial_top_hits',
                'psych_spelling_risk',
                'psych_trust_proxy',
                'gibberish_penalty',
                'gibberish_flags',
                'false_friend_risk',
                'false_friend_hits',
                'shortlist_selected',
                'shortlist_rank',
                'shortlist_bucket',
                'shortlist_reason',
                'trademark_dpma_url',
                'trademark_swissreg_url',
                'trademark_tmview_url',
                'external_penalty',
                'hard_fail',
                'fail_reason',
                'recommendation',
            ]
        )
        for c in candidates:
            w.writerow(
                [
                    c.name,
                    c.generator_family,
                    c.lineage_atoms,
                    f'{c.source_confidence:.2f}',
                    scope,
                    gate,
                    c.quality_score,
                    c.challenge_risk,
                    c.total_score,
                    c.descriptive_risk,
                    c.similarity_risk,
                    c.closest_mark,
                    c.scope_penalty,
                    c.store_de_count,
                    c.store_de_exact,
                    c.store_ch_count,
                    c.store_ch_exact,
                    c.store_us_count,
                    c.store_us_exact,
                    c.store_exact_countries,
                    c.store_unknown_countries,
                    c.com_available,
                    c.com_fallback_available,
                    c.com_fallback_domain,
                    c.de_available,
                    c.ch_available,
                    c.web_result_count,
                    c.web_exact_hits,
                    c.web_near_hits,
                    c.web_sample_domains,
                    c.web_source,
                    c.pypi_exists,
                    c.npm_exists,
                    c.social_github_available,
                    c.social_linkedin_available,
                    c.social_x_available,
                    c.social_instagram_available,
                    c.social_unavailable_count,
                    c.social_unknown_count,
                    c.adversarial_risk,
                    c.adversarial_top_hits,
                    c.psych_spelling_risk,
                    c.psych_trust_proxy,
                    c.gibberish_penalty,
                    c.gibberish_flags,
                    c.false_friend_risk,
                    c.false_friend_hits,
                    c.shortlist_selected,
                    c.shortlist_rank,
                    c.shortlist_bucket,
                    c.shortlist_reason,
                    c.trademark_dpma_url,
                    c.trademark_swissreg_url,
                    c.trademark_tmview_url,
                    c.external_penalty,
                    c.hard_fail,
                    c.fail_reason,
                    recommendation(c, gate),
                ]
            )


def write_json(path: Path, scope: str, gate: str, candidates: list[Candidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'generated_at': dt.datetime.now().isoformat(timespec='seconds'),
        'scope': scope,
        'gate': gate,
        'disclaimer': (
            'Automated screening only; not legal advice. '
            'Use qualified trademark counsel before adopting a name.'
        ),
        'candidates': [],
    }
    for c in candidates:
        item = asdict(c)
        item['recommendation'] = recommendation(c, gate)
        payload['candidates'].append(item)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def append_run_history(
    path: Path,
    scope: str,
    gate: str,
    args: argparse.Namespace,
    candidates: list[Candidate],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = {'strong': 0, 'consider': 0, 'weak': 0, 'reject': 0}
    family_counts: Counter[str] = Counter()
    for c in candidates:
        counts[recommendation(c, gate)] += 1
        family_counts[c.generator_family] += 1
    shortlist_selected = sum(1 for c in candidates if c.shortlist_selected)
    top = []
    for c in candidates:
        rec = recommendation(c, gate)
        if c.shortlist_selected:
            top.append(
                {
                    'name': c.name,
                    'recommendation': rec,
                    'total_score': c.total_score,
                    'challenge_risk': c.challenge_risk,
                    'adversarial_risk': c.adversarial_risk,
                    'fail_reason': c.fail_reason,
                    'shortlist_rank': c.shortlist_rank,
                    'shortlist_bucket': c.shortlist_bucket,
                }
            )
        if len(top) >= 12:
            break
    flags = resolve_feature_flags(args)
    entry = {
        'timestamp': dt.datetime.now().isoformat(timespec='seconds'),
        'scope': scope,
        'gate': gate,
        'pipeline_version': flags.pipeline_version,
        'v3_enabled': flags.v3_enabled,
        'use_engine_interfaces': flags.use_engine_interfaces,
        'use_tiered_validation': flags.use_tiered_validation,
        'variation_profile': args.variation_profile,
        'generator_families': parse_csv_set(args.generator_families),
        'family_quotas': parse_family_quotas(args.family_quotas),
        'source_pool_db': args.source_pool_db,
        'source_pool_limit': int(args.source_pool_limit),
        'source_min_confidence': float(args.source_min_confidence),
        'source_languages': parse_csv_set(args.source_languages),
        'source_categories': parse_csv_set(args.source_categories),
        'false_friend_lexicon': args.false_friend_lexicon,
        'false_friend_fail_threshold': int(args.false_friend_fail_threshold),
        'gibberish_fail_threshold': int(args.gibberish_fail_threshold),
        'llm_input': args.llm_input,
        'llm_parse_attempts': int(args.llm_parse_attempts),
        'llm_parse_backoff_ms': int(args.llm_parse_backoff_ms),
        'llm_text_fallback': bool(args.llm_text_fallback),
        'degraded_network_mode': bool(args.degraded_network_mode),
        'store_check': bool(args.store_check),
        'domain_check': bool(args.domain_check),
        'web_check': bool(args.web_check),
        'package_check': bool(args.package_check),
        'social_check': bool(args.social_check),
        'pool_size': int(args.pool_size),
        'check_limit': int(args.check_limit),
        'shortlist_size': int(args.shortlist_size),
        'shortlist_max_bucket': int(args.shortlist_max_bucket),
        'shortlist_max_prefix3': int(args.shortlist_max_prefix3),
        'candidate_count': len(candidates),
        'shortlist_selected_count': shortlist_selected,
        'recommendation_counts': counts,
        'generator_family_counts': dict(sorted(family_counts.items(), key=lambda item: (-item[1], item[0]))),
        'top_candidates': top,
    }
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(entry, ensure_ascii=False) + '\n')


def persist_to_db(
    *,
    db_path: Path,
    scope: str,
    gate: str,
    variation_profile: str,
    args: argparse.Namespace,
    candidates: list[Candidate],
) -> tuple[int, int]:
    # Keep import local to avoid dependency coupling for users who only want CSV/JSON output.
    import naming_db as ndb

    flags = resolve_feature_flags(args)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        ndb.ensure_schema(conn)
        run_id = ndb.create_run(
            conn,
            source_path=str(db_path),
            scope=scope,
            gate_mode=gate,
            variation_profile=variation_profile,
            status='completed',
            config={
                'scope': scope,
                'gate': gate,
                'variation_profile': variation_profile,
                'seeds': args.seeds,
                'candidates': args.candidates,
                'only_candidates': bool(args.only_candidates),
                'pool_size': int(args.pool_size),
                'check_limit': int(args.check_limit),
                'degraded_network_mode': bool(args.degraded_network_mode),
                'generator_families': parse_csv_set(args.generator_families),
                'family_quotas': parse_family_quotas(args.family_quotas),
                'source_pool_db': args.source_pool_db,
                'source_pool_limit': int(args.source_pool_limit),
                'source_min_confidence': float(args.source_min_confidence),
                'source_languages': parse_csv_set(args.source_languages),
                'source_categories': parse_csv_set(args.source_categories),
                'false_friend_lexicon': args.false_friend_lexicon,
                'false_friend_fail_threshold': int(args.false_friend_fail_threshold),
                'gibberish_fail_threshold': int(args.gibberish_fail_threshold),
                'llm_input': args.llm_input,
                'llm_parse_attempts': int(args.llm_parse_attempts),
                'llm_parse_backoff_ms': int(args.llm_parse_backoff_ms),
                'llm_text_fallback': bool(args.llm_text_fallback),
                'pipeline_version': flags.pipeline_version,
                'v3_enabled': flags.v3_enabled,
                'use_engine_interfaces': flags.use_engine_interfaces,
                'use_tiered_validation': flags.use_tiered_validation,
                'shortlist_size': int(args.shortlist_size),
                'shortlist_max_bucket': int(args.shortlist_max_bucket),
                'shortlist_max_prefix3': int(args.shortlist_max_prefix3),
                'stage_events': bool(args.stage_events),
            },
            summary={
                'candidate_count': len(candidates),
                'strong_or_consider_count': sum(
                    1 for c in candidates if recommendation(c, gate) in {'strong', 'consider'} and not c.hard_fail
                ),
                'shortlist_selected_count': sum(1 for c in candidates if c.shortlist_selected),
            },
        )

        for c in candidates:
            candidate_id = ndb.upsert_candidate(
                conn,
                name_display=c.name,
                total_score=float(c.total_score),
                risk_score=float(c.challenge_risk),
                recommendation=recommendation(c, gate),
                quality_score=float(c.quality_score),
                engine_id=str(c.generator_family or ''),
                parent_ids=str(c.lineage_atoms or ''),
                status='rejected' if c.hard_fail else 'scored',
                rejection_reason=str(c.fail_reason or ''),
            )
            ndb.add_source(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                source_type='rule',
                source_label=f'name_generator:{c.generator_family}',
                metadata={
                    'scope': scope,
                    'gate': gate,
                    'variation_profile': variation_profile,
                    'generator_family': c.generator_family,
                    'lineage_atoms': c.lineage_atoms,
                    'source_confidence': c.source_confidence,
                },
            )
            lineage_parts = [part.strip() for part in c.lineage_atoms.split(';') if part.strip()]
            for part in lineage_parts:
                source_atom_id = None
                atom_norm = ndb.normalize_name(part)
                if atom_norm:
                    row = conn.execute(
                        'SELECT id FROM source_atoms WHERE atom_normalized = ?',
                        (atom_norm,),
                    ).fetchone()
                    if row:
                        source_atom_id = int(row[0])
                ndb.add_candidate_lineage(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    generator_family=c.generator_family,
                    source_atom_id=source_atom_id,
                    contribution_weight=float(c.source_confidence or 0.0),
                    note=part,
                )
            ndb.add_score_snapshot(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                quality_score=float(c.quality_score),
                risk_score=float(c.challenge_risk),
                external_penalty=float(c.external_penalty),
                total_score=float(c.total_score),
                recommendation=recommendation(c, gate),
                hard_fail=bool(c.hard_fail),
                reason=str(c.fail_reason or ''),
            )
            ndb.add_shortlist_decision(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                selected=bool(c.shortlist_selected),
                shortlist_rank=int(c.shortlist_rank),
                bucket_key=str(c.shortlist_bucket or ''),
                reason=str(c.shortlist_reason or ''),
                score=float(c.total_score),
            )
        conn.commit()
    return run_id, len(candidates)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate and screen app name candidates.')
    parser.add_argument(
        '--pipeline-version',
        choices=['v2', 'v3'],
        default='v2',
        help='Pipeline contract version toggle. Default v2 keeps current behavior.',
    )
    parser.add_argument(
        '--enable-v3',
        action='store_true',
        help='Feature flag to enable v3 pipeline path while preserving existing defaults when off.',
    )
    parser.add_argument(
        '--use-engine-interfaces',
        action='store_true',
        help='Force engine/scorer/filter/validator interface adapters (v3 contract surface).',
    )
    parser.add_argument(
        '--use-tiered-validation',
        action='store_true',
        help='Enable tiered validation signaling for downstream validators.',
    )
    parser.add_argument('--scope', choices=['dach', 'eu', 'global'], default='eu')
    parser.add_argument('--gate', choices=['balanced', 'strict'], default='strict')
    parser.add_argument('--seeds', default='', help='Comma-separated seed names/roots.')
    parser.add_argument(
        '--candidates',
        default='',
        help='Comma-separated explicit candidate names to screen (always included).',
    )
    parser.add_argument(
        '--only-candidates',
        action='store_true',
        help='Do not generate names; only screen --candidates.',
    )
    parser.add_argument('--pool-size', type=int, default=300, help='Internal pool size before external checks.')
    parser.add_argument('--check-limit', type=int, default=80, help='Top N to run external checks on.')
    parser.add_argument('--min-len', type=int, default=6)
    parser.add_argument('--max-len', type=int, default=11)
    parser.add_argument(
        '--variation-profile',
        choices=['standard', 'expanded'],
        default='expanded',
        help='Candidate generation profile. expanded adds broader multilingual phonetic roots.',
    )
    parser.add_argument(
        '--generator-families',
        default=','.join(DEFAULT_GENERATOR_FAMILIES),
        help='Comma-separated generator families (coined,stem,suggestive,morphology,seed,expression,source_pool,blend).',
    )
    parser.add_argument(
        '--family-quotas',
        default='',
        help='Optional comma list like coined:120,source_pool:220,blend:180.',
    )
    parser.add_argument(
        '--source-pool-db',
        default='docs/branding/naming_pipeline_v1.db',
        help='SQLite DB path used to load curated source atoms for source_pool/blend families.',
    )
    parser.add_argument(
        '--source-pool-limit',
        type=int,
        default=500,
        help='Maximum source atoms to load from source pool DB.',
    )
    parser.add_argument(
        '--source-min-confidence',
        type=float,
        default=0.60,
        help='Minimum confidence for source atoms loaded into generation.',
    )
    parser.add_argument(
        '--source-languages',
        default='',
        help='Optional comma-separated language filters for source atoms.',
    )
    parser.add_argument(
        '--source-categories',
        default='',
        help='Optional comma-separated semantic category filters for source atoms.',
    )
    parser.add_argument(
        '--max-per-prefix2',
        type=int,
        default=24,
        help='Diversity guard: max candidates sharing same first 2 letters.',
    )
    parser.add_argument(
        '--max-per-suffix2',
        type=int,
        default=24,
        help='Diversity guard: max candidates sharing same last 2 letters.',
    )
    parser.add_argument(
        '--max-per-shape',
        type=int,
        default=18,
        help='Diversity guard: max candidates sharing same vowel/consonant shape.',
    )
    parser.add_argument(
        '--max-per-family',
        type=int,
        default=280,
        help='Diversity guard: hard cap per generator family before scoring.',
    )
    parser.add_argument(
        '--false-friend-fail-threshold',
        type=int,
        default=28,
        help='Hard-fail threshold for false-friend/negative-association risk.',
    )
    parser.add_argument(
        '--false-friend-lexicon',
        default='docs/branding/naming_false_friend_lexicon_v1.md',
        help='Markdown lexicon table file for false-friend and negative-association checks.',
    )
    parser.add_argument(
        '--gibberish-fail-threshold',
        type=int,
        default=35,
        help='Hard-fail threshold for gibberish penalty.',
    )
    parser.add_argument(
        '--llm-input',
        default='',
        help='Optional LLM output file (.json/.txt) used as fallback explicit candidates.',
    )
    parser.add_argument(
        '--llm-parse-attempts',
        type=int,
        default=3,
        help='Parse attempts for --llm-input payload before fallback.',
    )
    parser.add_argument(
        '--llm-parse-backoff-ms',
        type=int,
        default=150,
        help='Backoff milliseconds for repeated LLM payload parsing attempts.',
    )
    parser.add_argument(
        '--llm-text-fallback',
        action='store_true',
        default=True,
        help='Allow line-based fallback parsing when LLM JSON is malformed.',
    )
    parser.add_argument(
        '--no-llm-text-fallback',
        dest='llm_text_fallback',
        action='store_false',
    )
    parser.add_argument(
        '--store-countries',
        default='de,ch,us,gb,fr,it',
        help='Comma-separated country codes for App Store exact-match checks.',
    )
    parser.add_argument('--store-check', dest='store_check', action='store_true', default=True)
    parser.add_argument('--no-store-check', dest='store_check', action='store_false')
    parser.add_argument('--domain-check', dest='domain_check', action='store_true', default=True)
    parser.add_argument('--no-domain-check', dest='domain_check', action='store_false')
    parser.add_argument('--web-top', type=int, default=8, help='How many web search results to inspect.')
    parser.add_argument('--web-check', dest='web_check', action='store_true', default=True)
    parser.add_argument('--no-web-check', dest='web_check', action='store_false')
    parser.add_argument('--package-check', dest='package_check', action='store_true', default=True)
    parser.add_argument('--no-package-check', dest='package_check', action='store_false')
    parser.add_argument('--social-check', dest='social_check', action='store_true', default=True)
    parser.add_argument('--no-social-check', dest='social_check', action='store_false')
    parser.add_argument('--progress', dest='progress', action='store_true', default=True)
    parser.add_argument('--no-progress', dest='progress', action='store_false')
    parser.add_argument(
        '--degraded-network-mode',
        action='store_true',
        help='Treat unknown external-check states as soft warnings to keep local screening useful.',
    )
    parser.add_argument(
        '--adversarial-fail-threshold',
        type=int,
        default=82,
        help='Hard-fail threshold for adversarial similarity risk (0-100).',
    )
    parser.add_argument('--require-base-com', action='store_true', help='Require base <name>.com availability.')
    parser.add_argument(
        '--fail-on-unknown',
        action='store_true',
        help='Treat unknown external-check states as hard-fail.',
    )
    parser.add_argument('--throttle-ms', type=int, default=0, help='Sleep between candidate checks (ms).')
    parser.add_argument('--output', default='', help='Output CSV path.')
    parser.add_argument('--json-output', default='', help='Optional machine-readable JSON output path.')
    parser.add_argument(
        '--shortlist-size',
        type=int,
        default=50,
        help='Size of diversity-aware shortlist selection before remaining candidates.',
    )
    parser.add_argument(
        '--shortlist-max-bucket',
        type=int,
        default=2,
        help='Max shortlist entries per phonetic/string bucket.',
    )
    parser.add_argument(
        '--shortlist-max-prefix3',
        type=int,
        default=2,
        help='Max shortlist entries sharing same first 3 letters.',
    )
    parser.add_argument(
        '--shortlist-max-phonetic',
        type=int,
        default=1,
        help='Max shortlist entries sharing same phonetic fingerprint.',
    )
    parser.add_argument(
        '--stage-events',
        dest='stage_events',
        action='store_true',
        default=True,
        help='Emit structured JSON stage events for observability.',
    )
    parser.add_argument(
        '--no-stage-events',
        dest='stage_events',
        action='store_false',
    )
    parser.add_argument(
        '--persist-db',
        action='store_true',
        help='Persist scored candidates into SQLite candidate lake.',
    )
    parser.add_argument(
        '--db',
        default='docs/branding/naming_pipeline.db',
        help='SQLite DB path used when --persist-db is enabled.',
    )
    parser.add_argument(
        '--run-log',
        default='docs/branding/name_generator_runs.jsonl',
        help='Append run summary JSONL history to this path (set empty string to disable).',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    flags = resolve_feature_flags(args)
    run_started = time.monotonic()

    seeds = [s.strip() for s in args.seeds.split(',') if s.strip()]
    explicit_candidates = [normalize_alpha(s.strip()) for s in args.candidates.split(',') if s.strip()]
    explicit_candidates = [c for c in explicit_candidates if c]

    llm_fallback_candidates = load_llm_fallback_candidates(
        path=args.llm_input,
        max_attempts=max(1, args.llm_parse_attempts),
        backoff_ms=max(0, args.llm_parse_backoff_ms),
        allow_text_fallback=bool(args.llm_text_fallback),
    )
    if llm_fallback_candidates and args.progress:
        print(
            f'llm_fallback_loaded count={len(llm_fallback_candidates)} '
            f'input={args.llm_input}'
        )
    if llm_fallback_candidates:
        explicit_candidates = sorted(set(explicit_candidates + llm_fallback_candidates))
    emit_stage_event(
        args.stage_events,
        'llm_fallback',
        llm_input=bool(args.llm_input),
        fallback_count=len(llm_fallback_candidates),
    )

    active_families = parse_csv_set(args.generator_families) or list(DEFAULT_GENERATOR_FAMILIES)
    family_quotas = parse_family_quotas(args.family_quotas)
    source_languages = parse_csv_set(args.source_languages)
    source_categories = parse_csv_set(args.source_categories)
    source_load_started = time.monotonic()
    source_atoms = load_source_atoms(
        db_path=args.source_pool_db,
        limit=max(1, args.source_pool_limit),
        min_confidence=max(0.0, min(1.0, args.source_min_confidence)),
        languages=source_languages,
        categories=source_categories,
    )
    source_load_latency_ms = int((time.monotonic() - source_load_started) * 1000)
    emit_stage_event(
        args.stage_events,
        'source_pool_load',
        source_atoms=len(source_atoms),
        latency_ms=source_load_latency_ms,
        source_db=args.source_pool_db,
    )

    store_countries = [s.strip().lower() for s in args.store_countries.split(',') if re.fullmatch(r'[a-z]{2}', s.strip().lower())]
    if not store_countries:
        store_countries = ['de', 'ch', 'us']

    similarity_fail_threshold = 80 if args.gate == 'strict' else 88
    require_base_com = args.require_base_com or args.gate == 'strict'
    fail_on_unknown = (args.fail_on_unknown or args.gate == 'strict') and not args.degraded_network_mode
    false_friend_rules = load_false_friend_rules(args.false_friend_lexicon)

    explicit_generated = [
        GeneratedCandidate(name=name, generator_family='explicit', lineage_atoms=[name], source_confidence=0.95)
        for name in explicit_candidates
    ]

    generation_started = time.monotonic()
    if args.only_candidates:
        generated_items = explicit_generated
    else:
        if flags.use_engine_interfaces:
            generation_engine: CandidateGeneratorEngine = FamilyRuleGeneratorEngine(
                diversity_filter_engine=PrefixSuffixShapeFilter()
            )
            generated_items = generation_engine.generate(
                GenerationRequest(
                    scope=args.scope,
                    seeds=tuple(seeds),
                    min_len=args.min_len,
                    max_len=args.max_len,
                    variation_profile=args.variation_profile,
                    generator_families=tuple(active_families),
                    family_quotas=family_quotas,
                    source_atoms=source_atoms,
                    max_per_prefix2=max(1, args.max_per_prefix2),
                    max_per_suffix2=max(1, args.max_per_suffix2),
                    max_per_shape=max(1, args.max_per_shape),
                    max_per_family=max(1, args.max_per_family),
                )
            )
        else:
            generated_items = generate_candidates(
                args.scope,
                seeds,
                args.min_len,
                args.max_len,
                args.variation_profile,
                active_families,
                family_quotas,
                source_atoms,
                max(1, args.max_per_prefix2),
                max(1, args.max_per_suffix2),
                max(1, args.max_per_shape),
                max(1, args.max_per_family),
            )
        by_name = {item.name: item for item in generated_items}
        for item in explicit_generated:
            by_name[item.name] = item
        generated_items = sorted(by_name.values(), key=lambda item: item.name)
    generation_latency_ms = int((time.monotonic() - generation_started) * 1000)
    family_generation_counts = dict(sorted(Counter(item.generator_family for item in generated_items).items()))
    emit_stage_event(
        args.stage_events,
        'generation',
        generated_count=len(generated_items),
        family_counts=family_generation_counts,
        latency_ms=generation_latency_ms,
    )

    if args.progress:
        print(
            f'generation_config families={",".join(active_families)} '
            f'source_atoms={len(source_atoms)} source_db={args.source_pool_db} '
            f'pipeline={flags.pipeline_version} v3_enabled={flags.v3_enabled} '
            f'engine_interfaces={flags.use_engine_interfaces}'
        )

    if not generated_items:
        print('No candidates to evaluate. Provide --candidates and/or generation inputs.')
        return 1

    scoring_started = time.monotonic()
    if flags.use_engine_interfaces:
        scorer_engine: CandidateScorerEngine = RuleScorerEngine()
        evaluated = scorer_engine.score(
            ScoringRequest(
                scope=args.scope,
                generated_items=generated_items,
                similarity_fail_threshold=similarity_fail_threshold,
                false_friend_fail_threshold=max(1, args.false_friend_fail_threshold),
                gibberish_fail_threshold=max(1, args.gibberish_fail_threshold),
                false_friend_rules=false_friend_rules,
            )
        )
    else:
        evaluated = evaluate_candidates(
            args.scope,
            generated_items,
            similarity_fail_threshold,
            max(1, args.false_friend_fail_threshold),
            max(1, args.gibberish_fail_threshold),
            false_friend_rules,
        )
    scoring_latency_ms = int((time.monotonic() - scoring_started) * 1000)

    ranked_all = sorted(
        evaluated,
        key=lambda c: (c.hard_fail, -c.total_score, c.challenge_risk, -c.quality_score, c.name),
    )
    cheap_pass = [candidate for candidate in ranked_all if not candidate.hard_fail]
    cheap_fail = len(ranked_all) - len(cheap_pass)
    emit_stage_event(
        args.stage_events,
        'cheap_gate',
        evaluated_count=len(ranked_all),
        cheap_pass_count=len(cheap_pass),
        cheap_fail_count=cheap_fail,
        dropoff_count=cheap_fail,
        latency_ms=scoring_latency_ms,
    )

    if flags.use_tiered_validation:
        pool_source = cheap_pass
    else:
        pool_source = ranked_all
    pool = pool_source[: max(1, args.pool_size)]
    to_check = pool[: max(1, args.check_limit)]

    if explicit_candidates:
        by_name = {c.name: c for c in evaluated}
        in_check = {c.name for c in to_check}
        for name in explicit_candidates:
            c = by_name.get(name)
            if not c or c.name in in_check:
                continue
            if flags.use_tiered_validation and c.hard_fail:
                continue
            if c:
                to_check.append(c)
                in_check.add(c.name)
    emit_stage_event(
        args.stage_events,
        'finalist_selection',
        tiered_validation=flags.use_tiered_validation,
        pool_size=len(pool),
        finalist_count=len(to_check),
        requested_pool_size=max(1, args.pool_size),
        requested_check_limit=max(1, args.check_limit),
    )

    external_started = time.monotonic()
    if to_check:
        if flags.use_engine_interfaces:
            validator_engine: CandidateValidatorEngine = ExternalCheckValidatorEngine()
            validator_engine.validate(
                ExternalValidationRequest(
                    candidates=to_check,
                    scope=args.scope,
                    throttle_ms=args.throttle_ms,
                    gate=args.gate,
                    store_countries=store_countries,
                    store_check=args.store_check,
                    web_check=args.web_check,
                    web_top=args.web_top,
                    domain_check=args.domain_check,
                    require_base_com=require_base_com,
                    fail_on_unknown=fail_on_unknown,
                    package_check=args.package_check,
                    social_check=args.social_check,
                    adversarial_fail_threshold=max(0, min(100, args.adversarial_fail_threshold)),
                    show_progress=args.progress,
                    degraded_network_mode=args.degraded_network_mode,
                )
            )
        else:
            run_external_checks(
                to_check,
                args.scope,
                args.throttle_ms,
                args.gate,
                store_countries,
                args.store_check,
                args.web_check,
                args.web_top,
                args.domain_check,
                require_base_com,
                fail_on_unknown,
                args.package_check,
                args.social_check,
                max(0, min(100, args.adversarial_fail_threshold)),
                args.progress,
                args.degraded_network_mode,
            )
    external_latency_ms = int((time.monotonic() - external_started) * 1000)
    external_pass = sum(1 for candidate in to_check if not candidate.hard_fail)
    external_fail = len(to_check) - external_pass
    emit_stage_event(
        args.stage_events,
        'expensive_gate',
        finalists_checked=len(to_check),
        pass_count=external_pass,
        fail_count=external_fail,
        dropoff_count=external_fail,
        latency_ms=external_latency_ms,
    )

    ranked_checked = sorted(
        to_check,
        key=lambda c: (
            c.hard_fail,
            {'strong': 0, 'consider': 1, 'weak': 2, 'reject': 3}[recommendation(c, args.gate)],
            c.challenge_risk,
            -c.total_score,
            -c.source_confidence,
            c.name,
        ),
    )
    final_ranked = rerank_with_diversity(
        ranked_checked,
        gate=args.gate,
        shortlist_size=max(1, args.shortlist_size),
        max_per_bucket=max(1, args.shortlist_max_bucket),
        max_per_prefix3=max(1, args.shortlist_max_prefix3),
        max_per_phonetic=max(1, args.shortlist_max_phonetic),
    )
    shortlist_selected = sum(1 for candidate in final_ranked if candidate.shortlist_selected)
    emit_stage_event(
        args.stage_events,
        'shortlist',
        shortlisted_count=shortlist_selected,
        shortlisted_quota=max(1, args.shortlist_size),
        shortlisted_buckets=len({c.shortlist_bucket for c in final_ranked if c.shortlist_selected}),
        shortlisted_phonetics=len(
            {phonetic_fingerprint(c.name) for c in final_ranked if c.shortlist_selected}
        ),
    )

    out_path = args.output
    if not out_path:
        ts = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        out_path = f'docs/branding/generated_name_candidates_{args.scope}_{args.gate}_{ts}.csv'
    output_file = Path(out_path)
    write_csv(output_file, args.scope, final_ranked, args.gate)
    if args.json_output:
        write_json(Path(args.json_output), args.scope, args.gate, final_ranked)
    if args.persist_db:
        run_id, persisted_count = persist_to_db(
            db_path=Path(args.db),
            scope=args.scope,
            gate=args.gate,
            variation_profile=args.variation_profile,
            args=args,
            candidates=final_ranked,
        )
        print(f'Persisted {persisted_count} candidates to DB: {args.db} (run_id={run_id})')
    if args.run_log:
        append_run_history(Path(args.run_log), args.scope, args.gate, args, final_ranked)

    total_latency_ms = int((time.monotonic() - run_started) * 1000)
    emit_stage_event(
        args.stage_events,
        'complete',
        candidate_count=len(final_ranked),
        shortlist_count=shortlist_selected,
        latency_ms=total_latency_ms,
    )

    print(f'Wrote {len(final_ranked)} screened candidates: {output_file}')
    print('Top candidates:')
    shown = 0
    for c in final_ranked:
        rec = recommendation(c, args.gate)
        if rec in {'strong', 'consider'} and not c.hard_fail:
            shortlist_label = f'#{c.shortlist_rank}' if c.shortlist_selected else '-'
            print(
                f'- {c.name:12s} | fam={c.generator_family:10s} | shortlist={shortlist_label:3s} | '
                f'rec={rec:8s} | total={c.total_score:3d} | '
                f'risk={c.challenge_risk:3d} | domains(com/de/ch)='
                f'{c.com_available}/{c.de_available}/{c.ch_available} | '
                f'fallback={c.com_fallback_domain or "-"} | '
                f'store(de/ch/us)={c.store_de_count}/{c.store_ch_count}/{c.store_us_count} | '
                f'web(exact/near)={c.web_exact_hits}/{c.web_near_hits} | '
                f'pkg(pypi/npm)={c.pypi_exists}/{c.npm_exists} | '
                f'adv={c.adversarial_risk} | ff={c.false_friend_risk} | gib={c.gibberish_penalty}'
            )
            shown += 1
        if shown >= 15:
            break

    if shown == 0:
        print('No strong/consider candidates found with current constraints.')

    print('\nNext: manually verify top 5 in DPMA/IGE/EUIPO and run user trust tests.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
