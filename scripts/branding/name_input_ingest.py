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

import naming_db as ndb


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


def parse_txt(path: Path, default_label: str, default_conf: float) -> list[SourceRecord]:
    rows: list[SourceRecord] = []
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        parts = [part.strip() for part in line.split('|')]
        if len(parts) == 1:
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
    return rows


def parse_file(path: Path, default_label: str, default_conf: float) -> list[SourceRecord]:
    suffix = path.suffix.lower()
    if suffix == '.csv':
        return parse_csv(path, default_label, default_conf)
    if suffix == '.json':
        return parse_json(path, default_label, default_conf)
    return parse_txt(path, default_label, default_conf)


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
    parser.add_argument('--db', default='docs/branding/naming_pipeline.db', help='SQLite DB path.')
    parser.add_argument('--inputs', nargs='+', required=True, help='Input files (.csv/.json/.txt).')
    parser.add_argument('--source-label', default='curated_v2', help='Default source label.')
    parser.add_argument('--default-language', default='', help='Fallback language hint when missing.')
    parser.add_argument('--default-category', default='', help='Fallback semantic category when missing.')
    parser.add_argument('--default-confidence', type=float, default=0.75, help='Fallback confidence weight (0..1).')
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
    missing = [path for path in input_paths if not path.exists()]
    if missing:
        print('Missing input files:')
        for path in missing:
            print(f'- {path}')
        return 1

    loaded: list[SourceRecord] = []
    for path in input_paths:
        loaded.extend(parse_file(path, args.source_label, args.default_confidence))

    derived_count = 0
    if args.derive_morphology:
        derived = derive_morphology_records(loaded, confidence_scale=max(0.05, args.morph_confidence_scale))
        loaded.extend(derived)
        derived_count = len(derived)

    normalized = dedupe_records(loaded)
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
                'source_label': args.source_label,
                'also_candidates': bool(args.also_candidates),
                'activate': bool(args.activate),
                'derive_morphology': bool(args.derive_morphology),
                'morph_confidence_scale': float(args.morph_confidence_scale),
                'derived_morphology_records': derived_count,
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
        f'linked_candidates={linked_candidates} derived_morphology={derived_count} db={db_path}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
