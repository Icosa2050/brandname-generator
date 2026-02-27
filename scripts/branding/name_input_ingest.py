#!/usr/bin/env python3
"""Ingest curated naming input atoms into SQLite candidate lake.

Input rows are normalized into `source_atoms` and can optionally be promoted to
candidate rows for deterministic screening.

Supported input formats:
- CSV with headers: name, language_hint, semantic_category, confidence_weight,
  source_label, note
- JSON array of strings or objects with same keys
- TXT lines with either:
  - plain atom (name only)
  - pipe-separated: name|language_hint|semantic_category|confidence_weight|source_label|note
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import naming_db as ndb
import path_config as bpaths

try:
    from wordfreq import zipf_frequency as _zipf_frequency
except Exception:  # pragma: no cover - optional dependency
    _zipf_frequency = None


@dataclass
class SourceRecord:
    name: str
    language_hint: str
    semantic_category: str
    confidence_weight: float
    source_label: str
    note: str
    provenance_tags: list[str]
    metadata: dict[str, object]


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec='seconds')


AVAILABILITY_CLAIM_KEYS = {
    'domain_available',
    'domain_availability',
    'domain_free',
    'trademark_clear',
    'trademark_available',
    'trademark_status',
    'app_store_available',
    'social_available',
    'availability',
}
AVAILABILITY_TEXT_PATTERN = re.compile(
    r'\b(domain|trademark|tm|app\s*store|play\s*store|social|handle)\b.*\b(available|free|clear|not taken)\b',
    re.IGNORECASE,
)
TXT_COLON_PAIR_PATTERN = re.compile(r'^\s*([A-Z][a-z]{2,24})\s*:\s*([^|]{2,160})\s*$')
TXT_PAREN_PAIR_PATTERN = re.compile(r'\b([A-Z][a-z]{2,24})\s*\(([^()]{2,100})\)')
TXT_SIMPLE_ATOM_PATTERN = re.compile(r"^[A-Za-z][A-Za-z' -]{1,30}$")
TXT_BLOCKED_NAME_TOKENS = {
    'also',
    'and',
    'are',
    'beautiful',
    'categorized',
    'common',
    'cooperating',
    'deep',
    'expressions',
    'girl',
    'guy',
    'here',
    'hospitality',
    'like',
    'meaning',
    'meaningful',
    'nature',
    'often',
    'person',
    'popular',
    'strong',
    'symbolism',
    'the',
    'thebump',
    'these',
    'used',
    'volunteer',
    'words',
    'would',
}
EXCLUSION_FIELD_KEYS = (
    'name',
    'atom',
    'label',
    'title',
    'name_display',
    'company',
    'entity',
)


def parse_csv_set(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(',') if part.strip()]


def normalize_tags(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        tag = value.strip().lower()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
    return sorted(out)


def parse_record_tags(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return parse_csv_set(value)
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            if isinstance(item, str):
                part = item.strip()
                if part:
                    out.append(part)
        return out
    return []


def sanitize_metadata(metadata: dict[str, object]) -> tuple[dict[str, object], list[str]]:
    sanitized: dict[str, object] = {}
    removed: list[str] = []
    for key, value in metadata.items():
        key_lc = key.strip().lower()
        if not key_lc:
            continue
        if key_lc in AVAILABILITY_CLAIM_KEYS:
            removed.append(key_lc)
            continue
        if isinstance(value, str) and AVAILABILITY_TEXT_PATTERN.search(value):
            removed.append(f'text:{key_lc}')
            continue
        sanitized[key] = value
    return sanitized, sorted(set(removed))


def normalize_confidence(raw: object, fallback: float) -> float:
    if raw is None:
        return fallback
    text = str(raw).strip()
    if not text:
        return fallback
    try:
        value = float(text)
    except ValueError:
        return fallback
    return max(0.0, min(1.0, value))


def parse_csv(path: Path, default_label: str, default_conf: float) -> list[SourceRecord]:
    rows: list[SourceRecord] = []
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = str(row.get('name') or row.get('atom') or '').strip()
            if not name:
                continue
            extra_metadata: dict[str, object] = {}
            for key, value in row.items():
                key_lc = str(key or '').strip().lower()
                if key_lc in {
                    'name',
                    'atom',
                    'language_hint',
                    'semantic_category',
                    'confidence_weight',
                    'source_label',
                    'note',
                    'provenance_tags',
                }:
                    continue
                text = str(value or '').strip()
                if text:
                    extra_metadata[key_lc] = text
            rows.append(
                SourceRecord(
                    name=name,
                    language_hint=str(row.get('language_hint') or '').strip(),
                    semantic_category=str(row.get('semantic_category') or '').strip(),
                    confidence_weight=normalize_confidence(row.get('confidence_weight'), default_conf),
                    source_label=str(row.get('source_label') or default_label).strip() or default_label,
                    note=str(row.get('note') or '').strip(),
                    provenance_tags=normalize_tags(parse_record_tags(row.get('provenance_tags'))),
                    metadata=extra_metadata,
                )
            )
    return rows


def parse_json(path: Path, default_label: str, default_conf: float) -> list[SourceRecord]:
    payload = json.loads(path.read_text(encoding='utf-8'))
    if isinstance(payload, dict):
        if isinstance(payload.get('candidates'), list):
            payload = payload['candidates']
        elif isinstance(payload.get('items'), list):
            payload = payload['items']
        elif isinstance(payload.get('records'), list):
            payload = payload['records']
        else:
            return []
    if not isinstance(payload, list):
        return []
    rows: list[SourceRecord] = []
    for item in payload:
        if isinstance(item, str):
            name = item.strip()
            if not name:
                continue
            rows.append(
                SourceRecord(
                    name=name,
                    language_hint='',
                    semantic_category='',
                    confidence_weight=default_conf,
                    source_label=default_label,
                    note='',
                    provenance_tags=[],
                    metadata={},
                )
            )
            continue
        if not isinstance(item, dict):
            continue
        name = str(item.get('name') or item.get('atom') or '').strip()
        if not name:
            continue
        metadata = dict(item.get('metadata') or {}) if isinstance(item.get('metadata'), dict) else {}
        for key, value in item.items():
            key_lc = str(key).strip().lower()
            if key_lc in {
                'name',
                'atom',
                'language_hint',
                'semantic_category',
                'confidence_weight',
                'source_label',
                'note',
                'provenance_tags',
                'metadata',
            }:
                continue
            metadata[key_lc] = value
        rows.append(
            SourceRecord(
                name=name,
                language_hint=str(item.get('language_hint') or '').strip(),
                semantic_category=str(item.get('semantic_category') or '').strip(),
                confidence_weight=normalize_confidence(item.get('confidence_weight'), default_conf),
                source_label=str(item.get('source_label') or default_label).strip() or default_label,
                note=str(item.get('note') or '').strip(),
                provenance_tags=normalize_tags(parse_record_tags(item.get('provenance_tags'))),
                metadata=metadata,
            )
        )
    return rows


def _normalize_gloss(text: str) -> str:
    gloss = re.sub(r'\s+', ' ', text).strip().strip('.,;:!?')
    if gloss.lower().startswith('meaning '):
        gloss = gloss[8:].strip()
    return gloss


def _is_usable_name_token(raw_name: str) -> bool:
    if not re.fullmatch(r'[A-Z][a-z]{2,24}', raw_name.strip()):
        return False
    normalized = ndb.normalize_name(raw_name)
    if not normalized or normalized in TXT_BLOCKED_NAME_TOKENS:
        return False
    return True


def _build_gloss_record(
    name: str,
    meaning: str,
    default_label: str,
    default_conf: float,
    swahili_hint: bool,
    extraction_tag: str,
) -> SourceRecord | None:
    if not _is_usable_name_token(name):
        return None
    cleaned_meaning = _normalize_gloss(meaning)
    if not cleaned_meaning:
        return None
    lowered_meaning = cleaned_meaning.lower()
    if 'http' in lowered_meaning or '.com' in lowered_meaning:
        return None
    if len(cleaned_meaning) > 140:
        return None
    return SourceRecord(
        name=name.strip(),
        language_hint='sw' if swahili_hint else '',
        semantic_category='name_gloss',
        confidence_weight=min(1.0, default_conf + 0.03),
        source_label=f'{default_label}:txt_gloss',
        note=cleaned_meaning,
        provenance_tags=[f'extract:{extraction_tag}'],
        metadata={
            'meaning': cleaned_meaning,
            'extraction': extraction_tag,
        },
    )


def parse_txt(path: Path, default_label: str, default_conf: float) -> list[SourceRecord]:
    text = path.read_text(encoding='utf-8')
    swahili_hint = 'swahili' in text.lower()
    rows: list[SourceRecord] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        parts = [part.strip() for part in line.split('|')]
        if len(parts) == 1:
            colon_match = TXT_COLON_PAIR_PATTERN.match(line)
            if colon_match:
                record = _build_gloss_record(
                    colon_match.group(1),
                    colon_match.group(2),
                    default_label=default_label,
                    default_conf=default_conf,
                    swahili_hint=swahili_hint,
                    extraction_tag='colon_line',
                )
                if record is not None:
                    rows.append(record)
                continue
        if len(parts) == 1:
            if len(line) > 32:
                continue
            if not TXT_SIMPLE_ATOM_PATTERN.fullmatch(line):
                continue
            if len([word for word in line.split() if word]) > 2:
                continue
            rows.append(
                SourceRecord(
                    name=parts[0],
                    language_hint='',
                    semantic_category='',
                    confidence_weight=default_conf,
                    source_label=default_label,
                    note='',
                    provenance_tags=[],
                    metadata={},
                )
            )
            continue
        while len(parts) < 6:
            parts.append('')
        rows.append(
            SourceRecord(
                name=parts[0],
                language_hint=parts[1],
                semantic_category=parts[2],
                confidence_weight=normalize_confidence(parts[3], default_conf),
                source_label=parts[4] or default_label,
                note=parts[5],
                provenance_tags=[],
                metadata={},
            )
        )

    for match in TXT_PAREN_PAIR_PATTERN.finditer(text):
        record = _build_gloss_record(
            match.group(1),
            match.group(2),
            default_label=default_label,
            default_conf=default_conf,
            swahili_hint=swahili_hint,
            extraction_tag='parenthetical_pair',
        )
        if record is not None:
            rows.append(record)
    return rows


def parse_file(path: Path, default_label: str, default_conf: float) -> list[SourceRecord]:
    suffix = path.suffix.lower()
    if suffix == '.csv':
        return parse_csv(path, default_label, default_conf)
    if suffix == '.json':
        return parse_json(path, default_label, default_conf)
    return parse_txt(path, default_label, default_conf)


def _normalize_exclusion_name(value: object) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    if '|' in text:
        text = text.split('|', 1)[0].strip()
    if ',' in text:
        text = text.split(',', 1)[0].strip()
    return ndb.normalize_name(text)


def _collect_exclusions_from_json(value: Any, out: set[str]) -> None:
    if isinstance(value, str):
        normalized = _normalize_exclusion_name(value)
        if normalized:
            out.add(normalized)
        return
    if isinstance(value, list):
        for item in value:
            _collect_exclusions_from_json(item, out)
        return
    if isinstance(value, dict):
        for key in EXCLUSION_FIELD_KEYS:
            if key in value:
                normalized = _normalize_exclusion_name(value.get(key))
                if normalized:
                    out.add(normalized)
        for item in value.values():
            if isinstance(item, (list, dict)):
                _collect_exclusions_from_json(item, out)


def load_exclusion_names(paths: list[Path]) -> set[str]:
    out: set[str] = set()
    for path in paths:
        suffix = path.suffix.lower()
        if suffix == '.csv':
            with path.open('r', encoding='utf-8', newline='') as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    for key in EXCLUSION_FIELD_KEYS:
                        if key not in row:
                            continue
                        normalized = _normalize_exclusion_name(row.get(key))
                        if normalized:
                            out.add(normalized)
            continue
        if suffix == '.json':
            payload = json.loads(path.read_text(encoding='utf-8'))
            _collect_exclusions_from_json(payload, out)
            continue
        for raw in path.read_text(encoding='utf-8').splitlines():
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            normalized = _normalize_exclusion_name(line)
            if normalized:
                out.add(normalized)
    return out


def maybe_zipf_frequency(name: str, *, lang: str) -> float | None:
    if _zipf_frequency is None:
        return None
    normalized = ndb.normalize_name(name)
    if not normalized:
        return None
    try:
        return float(_zipf_frequency(normalized, lang))
    except Exception:  # pragma: no cover - defensive for optional dependency runtime
        return None


def filter_source_records(
    records: list[SourceRecord],
    *,
    exclusion_names: set[str],
    zipf_min: float,
    zipf_max: float,
    zipf_lang: str,
) -> tuple[list[SourceRecord], dict[str, object]]:
    kept: list[SourceRecord] = []
    excluded_count = 0
    zipf_low_count = 0
    zipf_high_count = 0
    excluded_samples: list[str] = []
    zipf_low_samples: list[str] = []
    zipf_high_samples: list[str] = []

    for record in records:
        normalized = ndb.normalize_name(record.name)
        if not normalized:
            continue
        if normalized in exclusion_names:
            excluded_count += 1
            if len(excluded_samples) < 20:
                excluded_samples.append(normalized)
            continue

        zipf_score = maybe_zipf_frequency(record.name, lang=zipf_lang)
        if zipf_score is not None:
            record.metadata['zipf_frequency'] = round(zipf_score, 4)
            record.metadata['zipf_language'] = zipf_lang

        if zipf_min > 0 and zipf_score is not None and zipf_score < zipf_min:
            zipf_low_count += 1
            if len(zipf_low_samples) < 20:
                zipf_low_samples.append(normalized)
            continue

        if zipf_max > 0 and zipf_score is not None and zipf_score > zipf_max:
            zipf_high_count += 1
            if len(zipf_high_samples) < 20:
                zipf_high_samples.append(normalized)
            continue

        kept.append(record)

    summary = {
        'raw_count': len(records),
        'kept_count': len(kept),
        'excluded_count': excluded_count,
        'zipf_low_count': zipf_low_count,
        'zipf_high_count': zipf_high_count,
        'excluded_samples': sorted(set(excluded_samples)),
        'zipf_low_samples': sorted(set(zipf_low_samples)),
        'zipf_high_samples': sorted(set(zipf_high_samples)),
    }
    return kept, summary


def infer_morph_role(record: SourceRecord) -> str:
    category = (record.semantic_category or '').strip().lower()
    if 'prefix' in category:
        return 'prefix'
    if 'suffix' in category:
        return 'suffix'
    if category in {'root', 'stem', 'morph_root'}:
        return 'root'
    metadata_role = str(record.metadata.get('morph_role') or '').strip().lower()
    if metadata_role in {'prefix', 'suffix', 'root'}:
        return metadata_role
    return ''


def derive_morphology_records(records: list[SourceRecord], confidence_scale: float) -> list[SourceRecord]:
    out: list[SourceRecord] = []
    for record in records:
        normalized = ndb.normalize_name(record.name)
        if len(normalized) < 5:
            continue
        base_tags = normalize_tags(record.provenance_tags + ['derived:morphology'])
        scaled_conf = max(0.05, min(1.0, record.confidence_weight * confidence_scale))
        source_label = f'{record.source_label}:morph'

        derived: list[tuple[str, str]] = [
            (normalized, 'root'),
            (normalized[:3], 'prefix'),
            (normalized[:4], 'prefix'),
            (normalized[-3:], 'suffix'),
            (normalized[-4:], 'suffix'),
        ]
        for token, role in derived:
            if len(token) < 3:
                continue
            out.append(
                SourceRecord(
                    name=token,
                    language_hint=record.language_hint,
                    semantic_category=f'morph_{role}',
                    confidence_weight=scaled_conf,
                    source_label=source_label,
                    note=f'derived from {normalized}',
                    provenance_tags=base_tags,
                    metadata={
                        'derived_from': normalized,
                        'morph_role': role,
                        'derivation': 'prefix_suffix_root_split',
                    },
                )
            )
    return out


def dedupe_records(records: list[SourceRecord]) -> list[SourceRecord]:
    by_normalized: dict[str, SourceRecord] = {}
    for item in records:
        key = ndb.normalize_name(item.name)
        if not key:
            continue
        prev = by_normalized.get(key)
        if prev is None:
            by_normalized[key] = item
            continue
        prev.provenance_tags = normalize_tags(prev.provenance_tags + item.provenance_tags)
        for meta_key, meta_value in item.metadata.items():
            if meta_key not in prev.metadata:
                prev.metadata[meta_key] = meta_value
        prev_role = str(prev.metadata.get('morph_role') or infer_morph_role(prev) or '').strip().lower()
        item_role = str(item.metadata.get('morph_role') or infer_morph_role(item) or '').strip().lower()
        existing_role_list = prev.metadata.get('morph_roles')
        existing_roles = []
        if isinstance(existing_role_list, list):
            existing_roles = [str(role).strip().lower() for role in existing_role_list if str(role).strip()]
        roles = {
            role
            for role in [*existing_roles, prev_role, item_role]
            if role in {'prefix', 'suffix', 'root'}
        }
        if roles:
            prev.metadata['morph_roles'] = sorted(roles)
            if 'morph_role' not in prev.metadata:
                prev.metadata['morph_role'] = sorted(roles)[0]
        if item.confidence_weight > prev.confidence_weight:
            item.provenance_tags = normalize_tags(item.provenance_tags + prev.provenance_tags)
            for meta_key, meta_value in prev.metadata.items():
                if meta_key not in item.metadata:
                    item.metadata[meta_key] = meta_value
            if roles:
                item.metadata['morph_roles'] = sorted(roles)
                if 'morph_role' not in item.metadata:
                    item.metadata['morph_role'] = sorted(roles)[0]
            by_normalized[key] = item
            continue
        if item.confidence_weight == prev.confidence_weight and item.note and not prev.note:
            by_normalized[key] = item
    return sorted(
        by_normalized.values(),
        key=lambda r: (r.semantic_category, r.language_hint, ndb.normalize_name(r.name)),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Ingest curated source atoms into naming DB.')
    parser.add_argument('--db', default=str(bpaths.NAMING_PIPELINE_DB), help='SQLite DB path.')
    parser.add_argument('--inputs', nargs='+', required=True, help='Input files (.csv/.json/.txt).')
    parser.add_argument(
        '--exclude-inputs',
        nargs='*',
        default=[],
        help='Optional exclusion lists (.csv/.json/.txt) of normalized names to skip.',
    )
    parser.add_argument('--source-label', default='curated_v2', help='Default source label.')
    parser.add_argument('--default-language', default='', help='Fallback language hint when missing.')
    parser.add_argument('--default-category', default='', help='Fallback semantic category when missing.')
    parser.add_argument('--default-confidence', type=float, default=0.75, help='Fallback confidence weight (0..1).')
    parser.add_argument(
        '--zipf-min',
        type=float,
        default=0.0,
        help='Optional minimum wordfreq zipf value (0 disables low-end filtering).',
    )
    parser.add_argument(
        '--zipf-max',
        type=float,
        default=0.0,
        help='Optional maximum wordfreq zipf value (0 disables high-end filtering).',
    )
    parser.add_argument(
        '--zipf-language',
        default='en',
        help='wordfreq language code used for zipf filtering (default: en).',
    )
    parser.add_argument(
        '--zipf-require-package',
        action='store_true',
        help='Fail fast if zipf filtering is requested but wordfreq is unavailable.',
    )
    parser.add_argument(
        '--derive-morphology',
        action='store_true',
        default=True,
        help='Derive morphology prefix/suffix/root source atoms for morphology composer.',
    )
    parser.add_argument(
        '--no-derive-morphology',
        dest='derive_morphology',
        action='store_false',
    )
    parser.add_argument(
        '--morph-confidence-scale',
        type=float,
        default=0.72,
        help='Confidence multiplier for derived morphology atoms.',
    )
    parser.add_argument(
        '--provenance-tags',
        default='',
        help='Comma-separated global provenance tags attached to every ingested source atom.',
    )
    parser.add_argument('--scope', choices=['dach', 'eu', 'global'], default='global')
    parser.add_argument('--gate', choices=['strict', 'balanced'], default='balanced')
    parser.add_argument('--activate', dest='activate', action='store_true', default=True)
    parser.add_argument('--deactivate', dest='activate', action='store_false')
    parser.add_argument(
        '--also-candidates',
        action='store_true',
        help='Also upsert ingested atoms into candidates table as low-priority seed candidates.',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_paths = [Path(value) for value in args.inputs]
    exclusion_paths = [Path(value) for value in args.exclude_inputs]
    missing = [path for path in [*input_paths, *exclusion_paths] if not path.exists()]
    if missing:
        print('Missing input files:')
        for path in missing:
            print(f'- {path}')
        return 1

    if args.zipf_min > 0 and args.zipf_max > 0 and args.zipf_min >= args.zipf_max:
        print(f'Invalid zipf thresholds: zipf-min={args.zipf_min} must be < zipf-max={args.zipf_max}')
        return 1
    zipf_enabled = args.zipf_min > 0 or args.zipf_max > 0
    if zipf_enabled and _zipf_frequency is None and args.zipf_require_package:
        print('zipf filtering requested but wordfreq package is unavailable (set --zipf-require-package to enforce).')
        return 1
    if zipf_enabled and _zipf_frequency is None:
        print('zipf_filter_warning wordfreq package unavailable; zipf thresholds ignored.')

    loaded: list[SourceRecord] = []
    for path in input_paths:
        loaded.extend(parse_file(path, args.source_label, args.default_confidence))

    exclusion_names = load_exclusion_names(exclusion_paths) if exclusion_paths else set()
    filtered_records, filter_summary = filter_source_records(
        loaded,
        exclusion_names=exclusion_names,
        zipf_min=max(0.0, float(args.zipf_min)),
        zipf_max=max(0.0, float(args.zipf_max)),
        zipf_lang=str(args.zipf_language or 'en').strip() or 'en',
    )

    derived_count = 0
    if args.derive_morphology:
        derived = derive_morphology_records(filtered_records, confidence_scale=max(0.05, args.morph_confidence_scale))
        filtered_records.extend(derived)
        derived_count = len(derived)

    normalized = dedupe_records(filtered_records)
    if not normalized:
        print('No valid source records found.')
        return 1
    global_provenance_tags = normalize_tags(parse_csv_set(args.provenance_tags))

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        ndb.ensure_schema(conn)
        run_id = ndb.create_run(
            conn,
            source_path=';'.join(str(path) for path in input_paths),
            scope=args.scope,
            gate_mode=args.gate,
            variation_profile='source_ingest',
            status='completed',
            config={
                'source': 'name_input_ingest',
                'input_files': [str(path) for path in input_paths],
                'exclude_input_files': [str(path) for path in exclusion_paths],
                'source_label': args.source_label,
                'also_candidates': bool(args.also_candidates),
                'activate': bool(args.activate),
                'derive_morphology': bool(args.derive_morphology),
                'morph_confidence_scale': float(args.morph_confidence_scale),
                'zipf_min': float(args.zipf_min),
                'zipf_max': float(args.zipf_max),
                'zipf_language': str(args.zipf_language or 'en'),
                'zipf_enabled': bool(zipf_enabled),
                'zipf_package_available': bool(_zipf_frequency is not None),
                'derived_morphology_records': derived_count,
                'filter_summary': filter_summary,
                'global_provenance_tags': global_provenance_tags,
                'ingested_at': now_iso(),
            },
            summary={},
        )

        inserted_atoms = 0
        linked_candidates = 0
        removed_claims_total = 0
        for record in normalized:
            language_hint = record.language_hint or args.default_language
            semantic_category = record.semantic_category or args.default_category
            record_tags = normalize_tags(global_provenance_tags + record.provenance_tags)
            raw_metadata = {
                'note': record.note,
                'source_files': [str(path) for path in input_paths],
                **record.metadata,
            }
            if record_tags:
                raw_metadata['provenance_tags'] = record_tags
            sanitized_metadata, removed_claim_fields = sanitize_metadata(raw_metadata)
            morph_role = infer_morph_role(record)
            if morph_role:
                existing_roles = sanitized_metadata.get('morph_roles')
                role_list = []
                if isinstance(existing_roles, list):
                    role_list = [str(item).strip().lower() for item in existing_roles if str(item).strip()]
                role_list = sorted({*role_list, morph_role})
                sanitized_metadata['morph_role'] = morph_role
                sanitized_metadata['morph_roles'] = role_list
            if removed_claim_fields:
                removed_claims_total += len(removed_claim_fields)
                sanitized_metadata['availability_claim_policy'] = 'ignored_and_stripped'
                sanitized_metadata['availability_claim_fields_removed'] = removed_claim_fields

            atom_id = ndb.upsert_source_atom(
                conn,
                atom_display=record.name,
                language_hint=language_hint,
                semantic_category=semantic_category,
                source_label=record.source_label or args.source_label,
                confidence_weight=record.confidence_weight,
                metadata=sanitized_metadata,
                active=bool(args.activate),
            )
            inserted_atoms += 1

            if args.also_candidates:
                source_label_lc = (record.source_label or args.source_label).strip().lower()
                has_ai_hint = (
                    any(tag.startswith('prompt_id:') or tag.startswith('llm') for tag in record_tags)
                    or bool(sanitized_metadata.get('model'))
                    or bool(sanitized_metadata.get('provider'))
                    or 'llm' in source_label_lc
                    or 'ai' in source_label_lc
                )
                candidate_id = ndb.upsert_candidate(
                    conn,
                    name_display=record.name,
                    total_score=None,
                    risk_score=None,
                    recommendation='seed',
                    quality_score=None,
                    engine_id='llm_seed' if has_ai_hint else 'source_atom',
                    parent_ids=';'.join(record_tags),
                    status='new',
                    rejection_reason='',
                )
                ndb.add_source(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    source_type='ai' if has_ai_hint else 'curated',
                    source_label=record.source_label or args.source_label,
                    metadata={
                        'origin': 'source_atom',
                        'source_atom_id': atom_id,
                        'language_hint': language_hint,
                        'semantic_category': semantic_category,
                        'confidence_weight': record.confidence_weight,
                        'provenance_tags': record_tags,
                        'availability_claim_fields_removed': removed_claim_fields,
                    },
                )
                linked_candidates += 1

        conn.execute(
            'UPDATE naming_runs SET summary_json = ? WHERE id = ?',
            (
                json.dumps(
                    {
                        'ingested_atom_count': inserted_atoms,
                        'linked_candidate_count': linked_candidates,
                        'input_count': len(normalized),
                        'raw_input_count': int(filter_summary.get('raw_count') or 0),
                        'filtered_input_count': int(filter_summary.get('kept_count') or 0),
                        'excluded_count': int(filter_summary.get('excluded_count') or 0),
                        'zipf_low_count': int(filter_summary.get('zipf_low_count') or 0),
                        'zipf_high_count': int(filter_summary.get('zipf_high_count') or 0),
                        'derived_morphology_records': derived_count,
                        'availability_claim_fields_removed': removed_claims_total,
                    },
                    ensure_ascii=False,
                ),
                run_id,
            ),
        )
        conn.commit()

    print(
        f'source_ingest_complete run_id={run_id} atoms={inserted_atoms} '
        f'linked_candidates={linked_candidates} derived_morphology={derived_count} '
        f'excluded={filter_summary.get("excluded_count", 0)} '
        f'zipf_low={filter_summary.get("zipf_low_count", 0)} '
        f'zipf_high={filter_summary.get("zipf_high_count", 0)} db={db_path}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
