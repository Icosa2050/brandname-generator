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


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec='seconds')


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
            rows.append(
                SourceRecord(
                    name=name,
                    language_hint=str(row.get('language_hint') or '').strip(),
                    semantic_category=str(row.get('semantic_category') or '').strip(),
                    confidence_weight=normalize_confidence(row.get('confidence_weight'), default_conf),
                    source_label=str(row.get('source_label') or default_label).strip() or default_label,
                    note=str(row.get('note') or '').strip(),
                )
            )
    return rows


def parse_json(path: Path, default_label: str, default_conf: float) -> list[SourceRecord]:
    payload = json.loads(path.read_text(encoding='utf-8'))
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
                )
            )
            continue
        if not isinstance(item, dict):
            continue
        name = str(item.get('name') or item.get('atom') or '').strip()
        if not name:
            continue
        rows.append(
            SourceRecord(
                name=name,
                language_hint=str(item.get('language_hint') or '').strip(),
                semantic_category=str(item.get('semantic_category') or '').strip(),
                confidence_weight=normalize_confidence(item.get('confidence_weight'), default_conf),
                source_label=str(item.get('source_label') or default_label).strip() or default_label,
                note=str(item.get('note') or '').strip(),
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
        if item.confidence_weight > prev.confidence_weight:
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

    normalized = dedupe_records(loaded)
    if not normalized:
        print('No valid source records found.')
        return 1

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
                'ingested_at': now_iso(),
            },
            summary={},
        )

        inserted_atoms = 0
        linked_candidates = 0
        for record in normalized:
            language_hint = record.language_hint or args.default_language
            semantic_category = record.semantic_category or args.default_category
            atom_id = ndb.upsert_source_atom(
                conn,
                atom_display=record.name,
                language_hint=language_hint,
                semantic_category=semantic_category,
                source_label=record.source_label or args.source_label,
                confidence_weight=record.confidence_weight,
                metadata={
                    'note': record.note,
                    'source_files': [str(path) for path in input_paths],
                },
                active=bool(args.activate),
            )
            inserted_atoms += 1

            if args.also_candidates:
                candidate_id = ndb.upsert_candidate(
                    conn,
                    name_display=record.name,
                    total_score=None,
                    risk_score=None,
                    recommendation='seed',
                )
                ndb.add_source(
                    conn,
                    candidate_id=candidate_id,
                    run_id=run_id,
                    source_type='curated',
                    source_label=record.source_label or args.source_label,
                    metadata={
                        'origin': 'source_atom',
                        'source_atom_id': atom_id,
                        'language_hint': language_hint,
                        'semantic_category': semantic_category,
                        'confidence_weight': record.confidence_weight,
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
                    },
                    ensure_ascii=False,
                ),
                run_id,
            ),
        )
        conn.commit()

    print(
        f'source_ingest_complete run_id={run_id} atoms={inserted_atoms} '
        f'linked_candidates={linked_candidates} db={db_path}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
