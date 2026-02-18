#!/usr/bin/env python3
"""Ingest LLM-ideated naming candidates into the SQLite candidate lake.

This tool supports two controlled steps:
1) Deterministic prompt rendering (template + fixed inputs) for external LLM calls.
2) Ingestion of LLM output candidates with strict provenance metadata.

Important safety rule:
- Any availability assertion in LLM output metadata (domain/trademark/app store/social)
  is ignored and stripped before persistence.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

import naming_db as ndb

DEFAULT_TEMPLATE_ID = 'brand_surface_v1'
DEFAULT_TEMPLATE_REVISION = 'v1.0.0'
DEFAULT_BRIEF = (
    'Software that creates legally robust utility-cost settlements for small landlords '
    'and property managers in Germany and Switzerland.'
)

PROMPT_TEMPLATES: dict[str, str] = {
    'brand_surface_v1': (
        'You are generating product name candidates for a B2B SaaS.\n'
        'Product brief: {brief}\n'
        'Primary market: {market_scope}\n'
        'Locale focus: {locale_focus}\n'
        'Desired themes: {keywords}\n'
        'Avoid roots/tokens: {banned_roots}\n'
        'Target candidates: {target_count}\n'
        '\n'
        'Hard constraints:\n'
        '- Names must be 5-11 letters, Latin alphabet only.\n'
        '- Prefer 2-4 syllables, high pronounceability, low typo risk.\n'
        '- Avoid direct/descriptive compounds and crowded immo* naming patterns.\n'
        '- Do NOT claim or imply availability (domain/app-store/social/trademark).\n'
        '\n'
        'Return JSON only with this exact schema:\n'
        '{{"candidates":[{{"name":"string","semantic_category":"string",'
        '"confidence_weight":0.0,"note":"string","provenance_tags":["string"]}}]}}\n'
        'No markdown. No extra keys.'
    ),
    'brand_surface_crosslingual_v1': (
        'Generate internationally pronounceable SaaS brand names.\n'
        'Brief: {brief}\n'
        'Target market: {market_scope}\n'
        'Locale focus: {locale_focus}\n'
        'Themes: {keywords}\n'
        'Banned roots: {banned_roots}\n'
        'Target candidates: {target_count}\n'
        '\n'
        'Constraints:\n'
        '- Latin alphabet only, 5-11 letters, no punctuation.\n'
        '- Favor distinct sound patterns and avoid close variants.\n'
        '- Avoid legal/commercial availability claims.\n'
        '\n'
        'Return JSON only:\n'
        '{{"candidates":[{{"name":"string","semantic_category":"string",'
        '"confidence_weight":0.0,"note":"string","provenance_tags":["string"]}}]}}'
    ),
}

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

ALLOWED_RECORD_KEYS = {
    'name',
    'candidate',
    'atom',
    'source_label',
    'confidence_weight',
    'semantic_category',
    'note',
    'provenance_tags',
    'metadata',
}


@dataclass
class IdeationCandidate:
    name: str
    source_label: str
    confidence_weight: float
    semantic_category: str = ''
    note: str = ''
    provenance_tags: list[str] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)
    removed_claim_fields: list[str] = field(default_factory=list)


def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec='seconds')


def parse_csv_set(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(',') if part.strip()]


def normalize_tags(values: list[str]) -> list[str]:
    normalized = []
    seen: set[str] = set()
    for value in values:
        tag = value.strip().lower()
        if not tag:
            continue
        if tag in seen:
            continue
        seen.add(tag)
        normalized.append(tag)
    return sorted(normalized)


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


def template_choices() -> list[str]:
    return sorted(PROMPT_TEMPLATES.keys())


def render_prompt(args: argparse.Namespace) -> tuple[str, str]:
    custom = args.prompt.strip()
    if custom:
        return custom, 'custom'

    template_id = args.template_id
    template = PROMPT_TEMPLATES.get(template_id)
    if template is None:
        raise ValueError(f'Unknown template_id: {template_id}')
    keywords = ', '.join(normalize_tags(parse_csv_set(args.keywords)))
    banned_roots = ', '.join(normalize_tags(parse_csv_set(args.banned_roots)))
    prompt = template.format(
        brief=args.brief.strip() or DEFAULT_BRIEF,
        market_scope=args.market_scope.strip() or 'DE/CH',
        locale_focus=args.locale_focus.strip() or 'de,en',
        keywords=keywords or 'clarity, trust, fairness',
        banned_roots=banned_roots or 'immo, scout, net',
        target_count=max(1, int(args.target_count)),
    )
    return prompt, template_id


def write_prompt_artifact(
    *,
    path: Path,
    prompt_text: str,
    prompt_id: str,
    prompt_revision: str,
    prompt_hash: str,
    args: argparse.Namespace,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == '.json':
        payload = {
            'prompt_id': prompt_id,
            'prompt_revision': prompt_revision,
            'prompt_hash': prompt_hash,
            'created_at': now_iso(),
            'model': args.model,
            'provider': args.provider,
            'prompt': prompt_text,
        }
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        return
    path.write_text(prompt_text.rstrip() + '\n', encoding='utf-8')


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


def parse_json_candidate_container(raw_text: str) -> list[dict[str, object]]:
    payload = json.loads(raw_text)
    if isinstance(payload, list):
        source = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get('candidates'), list):
            source = payload['candidates']
        elif isinstance(payload.get('names'), list):
            source = payload['names']
        elif isinstance(payload.get('items'), list):
            source = payload['items']
        else:
            source = []
    else:
        source = []

    out: list[dict[str, object]] = []
    for item in source:
        if isinstance(item, str):
            out.append({'name': item})
            continue
        if isinstance(item, dict):
            out.append(dict(item))
    return out


def parse_json_payload_with_fallback(
    *,
    raw_text: str,
    max_attempts: int,
    backoff_ms: int,
    allow_text_fallback: bool,
) -> tuple[list[dict[str, object]], list[str]]:
    warnings: list[str] = []
    attempts = max(1, max_attempts)

    for idx in range(attempts):
        try:
            rows = parse_json_candidate_container(raw_text)
            return rows, warnings
        except json.JSONDecodeError:
            extracted = extract_json_object(raw_text)
            if extracted:
                try:
                    rows = parse_json_candidate_container(extracted)
                    warnings.append('json_extracted_from_wrapped_payload')
                    return rows, warnings
                except json.JSONDecodeError:
                    pass
            if idx < attempts - 1:
                time.sleep(max(0, backoff_ms) / 1000.0 * (idx + 1))

    warnings.append('json_parse_failed')
    if not allow_text_fallback:
        return [], warnings

    fallback_rows: list[dict[str, object]] = []
    for raw in raw_text.splitlines():
        text = raw.strip().strip('-*').strip()
        if not text or text.startswith('#'):
            continue
        fallback_rows.append({'name': text})
    if fallback_rows:
        warnings.append('line_based_fallback_used')
    return fallback_rows, warnings


def parse_input_payload(
    path: Path,
    *,
    max_attempts: int,
    backoff_ms: int,
    allow_text_fallback: bool,
) -> tuple[list[dict[str, object]], list[str]]:
    if not path.exists():
        return [], [f'missing_input_file:{path}']
    if path.suffix.lower() != '.json':
        rows: list[dict[str, object]] = []
        for raw in path.read_text(encoding='utf-8').splitlines():
            name = raw.strip()
            if not name or name.startswith('#'):
                continue
            rows.append({'name': name})
        return rows, []

    raw_text = path.read_text(encoding='utf-8')
    return parse_json_payload_with_fallback(
        raw_text=raw_text,
        max_attempts=max_attempts,
        backoff_ms=backoff_ms,
        allow_text_fallback=allow_text_fallback,
    )


def parse_inline_names(raw: str) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for part in raw.split(','):
        name = part.strip()
        if not name:
            continue
        out.append({'name': name})
    return out


def validate_record_schema(record: dict[str, object], strict_schema: bool) -> tuple[bool, list[str]]:
    warnings: list[str] = []
    unknown_keys = sorted(key for key in record.keys() if key not in ALLOWED_RECORD_KEYS)
    if unknown_keys:
        warnings.append(f'unknown_keys:{",".join(unknown_keys)}')

    type_errors: list[str] = []
    name_value = record.get('name') or record.get('candidate') or record.get('atom')
    if name_value is not None and not isinstance(name_value, str):
        type_errors.append('name_not_string')

    source_label = record.get('source_label')
    if source_label is not None and not isinstance(source_label, str):
        type_errors.append('source_label_not_string')

    semantic_category = record.get('semantic_category')
    if semantic_category is not None and not isinstance(semantic_category, str):
        type_errors.append('semantic_category_not_string')

    note = record.get('note')
    if note is not None and not isinstance(note, str):
        type_errors.append('note_not_string')

    provenance_tags = record.get('provenance_tags')
    if provenance_tags is not None and not isinstance(provenance_tags, (str, list)):
        type_errors.append('provenance_tags_type_invalid')

    confidence_weight = record.get('confidence_weight')
    if confidence_weight is not None:
        text = str(confidence_weight).strip()
        if text:
            try:
                float(text)
            except ValueError:
                type_errors.append('confidence_weight_not_numeric')

    if type_errors:
        warnings.append(f'type_errors:{",".join(type_errors)}')
        if strict_schema:
            return False, warnings
    return True, warnings


def coerce_candidate(
    *,
    record: dict[str, object],
    default_label: str,
    default_confidence: float,
    default_tags: list[str],
    strict_schema: bool,
) -> tuple[IdeationCandidate | None, list[str]]:
    valid, warnings = validate_record_schema(record, strict_schema)
    if not valid:
        return None, warnings

    name_value = record.get('name') or record.get('candidate') or record.get('atom')
    if not isinstance(name_value, str):
        return None, warnings
    name = name_value.strip()
    if not name:
        return None, warnings
    if not ndb.normalize_name(name):
        return None, warnings

    source_label = str(record.get('source_label') or default_label).strip() or default_label
    confidence = normalize_confidence(record.get('confidence_weight'), default_confidence)
    semantic_category = str(record.get('semantic_category') or '').strip()
    note = str(record.get('note') or '').strip()
    record_tags = parse_record_tags(record.get('provenance_tags'))
    tags = normalize_tags(default_tags + record_tags)

    metadata = {}
    for key, value in record.items():
        if key in {
            'name',
            'candidate',
            'atom',
            'source_label',
            'confidence_weight',
            'semantic_category',
            'note',
            'provenance_tags',
        }:
            continue
        metadata[key] = value
    sanitized_metadata, removed_claim_fields = sanitize_metadata(metadata)

    return IdeationCandidate(
        name=name,
        source_label=source_label,
        confidence_weight=confidence,
        semantic_category=semantic_category,
        note=note,
        provenance_tags=tags,
        metadata=sanitized_metadata,
        removed_claim_fields=removed_claim_fields,
    ), warnings


def dedupe_candidates(candidates: list[IdeationCandidate]) -> list[IdeationCandidate]:
    by_norm: dict[str, IdeationCandidate] = {}
    for candidate in candidates:
        key = ndb.normalize_name(candidate.name)
        existing = by_norm.get(key)
        if existing is None:
            by_norm[key] = candidate
            continue
        existing.provenance_tags = normalize_tags(existing.provenance_tags + candidate.provenance_tags)
        existing.removed_claim_fields = sorted(set(existing.removed_claim_fields + candidate.removed_claim_fields))
        if candidate.note and not existing.note:
            existing.note = candidate.note
        if candidate.semantic_category and not existing.semantic_category:
            existing.semantic_category = candidate.semantic_category
        if candidate.confidence_weight > existing.confidence_weight:
            existing.confidence_weight = candidate.confidence_weight
            existing.source_label = candidate.source_label
        for key_meta, value_meta in candidate.metadata.items():
            if key_meta not in existing.metadata:
                existing.metadata[key_meta] = value_meta
    return sorted(by_norm.values(), key=lambda item: ndb.normalize_name(item.name))


def parse_confidence_score(
    *,
    raw_record_count: int,
    candidate_count: int,
    parser_warning_count: int,
    schema_warning_count: int,
    target_count: int,
) -> float:
    score = 1.0
    if raw_record_count > 0:
        acceptance_ratio = candidate_count / max(1, raw_record_count)
        score -= max(0.0, 1.0 - acceptance_ratio) * 0.5
    score -= min(0.25, parser_warning_count * 0.05)
    score -= min(0.25, schema_warning_count * 0.02)
    if target_count > 0 and candidate_count < max(1, int(target_count * 0.5)):
        score -= 0.15
    return max(0.0, min(1.0, score))


def export_source_atoms_json(path: Path, candidates: list[IdeationCandidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: list[dict[str, object]] = []
    for candidate in candidates:
        export_metadata = dict(candidate.metadata)
        if candidate.removed_claim_fields:
            export_metadata['availability_claim_policy'] = 'ignored_and_stripped'
            export_metadata['availability_claim_fields_removed'] = candidate.removed_claim_fields
        payload.append(
            {
                'name': candidate.name,
                'language_hint': '',
                'semantic_category': candidate.semantic_category,
                'confidence_weight': candidate.confidence_weight,
                'source_label': candidate.source_label,
                'note': candidate.note,
                'provenance_tags': candidate.provenance_tags,
                'metadata': export_metadata,
            }
        )
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Ingest LLM-generated candidate names into naming DB.')
    parser.add_argument('--db', default='docs/branding/naming_pipeline.db', help='SQLite DB path.')
    parser.add_argument('--names', default='', help='Comma-separated candidate names.')
    parser.add_argument('--input', default='', help='Optional input file (.txt or .json).')
    parser.add_argument('--scope', choices=['dach', 'eu', 'global'], default='global')
    parser.add_argument('--gate', choices=['strict', 'balanced'], default='balanced')
    parser.add_argument('--model', default='', help='AI model identifier.')
    parser.add_argument('--provider', default='', help='AI provider identifier.')
    parser.add_argument('--source-label', default='llm_ideation_v3', help='Source label for provenance.')
    parser.add_argument(
        '--strict-schema',
        action='store_true',
        default=True,
        help='Validate LLM record schema; malformed records are dropped safely.',
    )
    parser.add_argument(
        '--no-strict-schema',
        dest='strict_schema',
        action='store_false',
    )
    parser.add_argument(
        '--max-parse-attempts',
        type=int,
        default=3,
        help='Retry count when parsing malformed JSON payloads.',
    )
    parser.add_argument(
        '--parse-backoff-ms',
        type=int,
        default=120,
        help='Backoff delay for JSON parse retries.',
    )
    parser.add_argument(
        '--allow-text-fallback',
        action='store_true',
        default=True,
        help='Fallback to line-based candidate extraction if JSON payload is invalid.',
    )
    parser.add_argument(
        '--no-text-fallback',
        dest='allow_text_fallback',
        action='store_false',
    )
    parser.add_argument(
        '--parse-confidence-threshold',
        type=float,
        default=0.70,
        help='Minimum parser confidence required before ingesting fallback-derived candidates.',
    )
    parser.add_argument('--prompt', default='', help='Optional custom prompt (overrides template rendering).')
    parser.add_argument('--template-id', choices=template_choices(), default=DEFAULT_TEMPLATE_ID)
    parser.add_argument('--template-revision', default=DEFAULT_TEMPLATE_REVISION)
    parser.add_argument('--brief', default=DEFAULT_BRIEF)
    parser.add_argument('--market-scope', default='Germany/Switzerland')
    parser.add_argument('--locale-focus', default='de,en')
    parser.add_argument('--keywords', default='clarity,trust,fairness,settlement,utility')
    parser.add_argument('--banned-roots', default='immo,scout,net,kostal,costal')
    parser.add_argument('--target-count', type=int, default=80)
    parser.add_argument('--default-confidence', type=float, default=0.72)
    parser.add_argument('--provenance-tags', default='', help='Comma-separated global provenance tags.')
    parser.add_argument('--emit-prompt-file', default='', help='Write rendered prompt to .txt or .json artifact.')
    parser.add_argument('--print-prompt', action='store_true', help='Print rendered prompt to stdout.')
    parser.add_argument(
        '--export-source-json',
        default='',
        help='Optional export path for source-atom JSON compatible with name_input_ingest.py.',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prompt_text, prompt_id = render_prompt(args)
    prompt_hash = hashlib.sha256(prompt_text.encode('utf-8')).hexdigest()

    if args.emit_prompt_file:
        write_prompt_artifact(
            path=Path(args.emit_prompt_file),
            prompt_text=prompt_text,
            prompt_id=prompt_id,
            prompt_revision=args.template_revision,
            prompt_hash=prompt_hash,
            args=args,
        )
    if args.print_prompt:
        print(prompt_text)

    raw_records: list[dict[str, object]] = []
    parser_warnings: list[str] = []
    raw_records.extend(parse_inline_names(args.names))
    if args.input:
        loaded_rows, warnings = parse_input_payload(
            Path(args.input),
            max_attempts=max(1, args.max_parse_attempts),
            backoff_ms=max(0, args.parse_backoff_ms),
            allow_text_fallback=bool(args.allow_text_fallback),
        )
        raw_records.extend(loaded_rows)
        parser_warnings.extend(warnings)

    default_tags = normalize_tags(
        parse_csv_set(args.provenance_tags)
        + [f'prompt_id:{prompt_id}', f'prompt_rev:{args.template_revision}', 'llm_ideation_v3']
    )

    coerced: list[IdeationCandidate] = []
    dropped_records = 0
    schema_warnings: list[str] = []
    for record in raw_records:
        candidate, warnings = coerce_candidate(
            record=record,
            default_label=args.source_label,
            default_confidence=max(0.0, min(1.0, args.default_confidence)),
            default_tags=default_tags,
            strict_schema=bool(args.strict_schema),
        )
        schema_warnings.extend(warnings)
        if candidate is None:
            dropped_records += 1
            continue
        coerced.append(candidate)
    candidates = dedupe_candidates(coerced)
    parse_confidence = parse_confidence_score(
        raw_record_count=len(raw_records),
        candidate_count=len(candidates),
        parser_warning_count=len(parser_warnings),
        schema_warning_count=len(schema_warnings),
        target_count=max(0, int(args.target_count)),
    )

    if args.input and raw_records and parse_confidence < max(0.0, min(1.0, args.parse_confidence_threshold)):
        print(
            f'parse_confidence_low value={parse_confidence:.2f} '
            f'threshold={args.parse_confidence_threshold:.2f} '
            'ingestion_skipped=true'
        )
        return 0

    if not candidates:
        if args.emit_prompt_file or args.print_prompt:
            print(
                'prompt_ready=true candidates_ingested=0 '
                f'parse_warnings={len(parser_warnings)} schema_warnings={len(schema_warnings)} '
                f'parse_confidence={parse_confidence:.2f}'
            )
            return 0
        print(
            'No valid candidate names to ingest. '
            f'parse_warnings={len(parser_warnings)} '
            f'schema_warnings={len(schema_warnings)} dropped_records={dropped_records} '
            f'parse_confidence={parse_confidence:.2f}'
        )
        return 0

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    removed_claim_count = sum(len(candidate.removed_claim_fields) for candidate in candidates)
    with sqlite3.connect(db_path) as conn:
        ndb.ensure_schema(conn)
        run_id = ndb.create_run(
            conn,
            source_path=args.input or 'cli_names',
            scope=args.scope,
            gate_mode=args.gate,
            variation_profile='llm_ideation_v3',
            status='completed',
            config={
                'source': 'ai_ingest_v3',
                'model': args.model,
                'provider': args.provider,
                'source_label': args.source_label,
                'prompt_id': prompt_id,
                'template_revision': args.template_revision,
                'prompt_hash': prompt_hash,
                'prompt_preview': prompt_text[:400],
                'candidate_count': len(candidates),
                'parse_confidence': parse_confidence,
                'parse_confidence_threshold': float(args.parse_confidence_threshold),
                'ingested_at': now_iso(),
            },
            summary={
                'ingested_count': len(candidates),
                'availability_claim_fields_removed': removed_claim_count,
                'parse_warning_count': len(parser_warnings),
                'schema_warning_count': len(schema_warnings),
                'dropped_record_count': dropped_records,
                'parse_confidence': parse_confidence,
            },
        )

        inserted = 0
        for candidate in candidates:
            candidate_id = ndb.upsert_candidate(
                conn,
                name_display=candidate.name,
                total_score=None,
                risk_score=None,
                recommendation=None,
                quality_score=None,
                engine_id='llm_ideation',
                parent_ids=';'.join(candidate.provenance_tags),
                status='new',
                rejection_reason='',
            )
            ndb.add_source(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                source_type='ai',
                source_label=candidate.source_label,
                metadata={
                    'model': args.model,
                    'provider': args.provider,
                    'prompt_id': prompt_id,
                    'template_revision': args.template_revision,
                    'prompt_hash': prompt_hash,
                    'semantic_category': candidate.semantic_category,
                    'confidence_weight': candidate.confidence_weight,
                    'note': candidate.note,
                    'provenance_tags': candidate.provenance_tags,
                    'availability_claim_policy': 'ignored_and_stripped',
                    'availability_claim_fields_removed': candidate.removed_claim_fields,
                    'metadata': candidate.metadata,
                },
            )
            inserted += 1
        conn.commit()

    if args.export_source_json:
        export_source_atoms_json(Path(args.export_source_json), candidates)

    if parser_warnings or schema_warnings:
        sample = sorted(set(parser_warnings + schema_warnings))[:8]
        print(f'ingest_warnings count={len(parser_warnings) + len(schema_warnings)} sample={sample}')

    print(
        f'ai_ingest_complete run_id={run_id} candidates={inserted} '
        f'db={db_path} source_label={args.source_label} '
        f'prompt_id={prompt_id} prompt_hash={prompt_hash[:12]} '
        f'claims_removed={removed_claim_count} dropped={dropped_records} '
        f'parse_warnings={len(parser_warnings)} schema_warnings={len(schema_warnings)} '
        f'parse_confidence={parse_confidence:.2f}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
