#!/usr/bin/env python3
"""Run acceptance-tail and async validation on the same reviewed input file."""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import naming_db as ndb


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_ASYNC_CHECKS = (
    'adversarial,psych,descriptive,tm_cheap,company_cheap,domain,web,'
    'web_google_like,tm_registry_global,app_store,package,social'
)


@dataclass(frozen=True)
class ReviewRow:
    name_display: str
    name_normalized: str
    decision_tag: str
    decision_notes: str
    recommendation: str
    score: float
    risk: float


def _normalize_name(raw: str | None) -> str:
    return ''.join(ch for ch in str(raw or '').strip().lower() if ch.isalpha())


def _is_x(raw: str | None) -> bool:
    return str(raw or '').strip().lower() == 'x'


def _to_float(raw: object, default: float = 0.0) -> float:
    try:
        if raw is None or str(raw).strip() == '':
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default


def _to_bool(raw: str | None) -> bool:
    return str(raw or '').strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8', newline='') as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, object]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, '') for key in headers})


def _run(cmd: list[str], *, dry_run: bool) -> None:
    print('$ ' + ' '.join(cmd), flush=True)
    if dry_run:
        return
    subprocess.run(cmd, cwd=str(ROOT_DIR), check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run dual validation bundle from one reviewed CSV.')
    parser.add_argument('--review-csv', required=True, help='Compatible review CSV with keep/maybe/drop columns.')
    parser.add_argument('--out-dir', required=True, help='Bundle output directory.')
    parser.add_argument('--mode', choices=['keep', 'keep_maybe'], default='keep_maybe')
    parser.add_argument('--scope', default='global')
    parser.add_argument('--gate', default='strict')
    parser.add_argument('--python-bin', default=sys.executable)
    parser.add_argument('--keep-top-n', type=int, default=12)
    parser.add_argument('--maybe-top-n', type=int, default=12)
    parser.add_argument('--final-top-n', type=int, default=8)
    parser.add_argument('--recommended-top-n', type=int, default=6)
    parser.add_argument('--countries', default='de,ch,it')
    parser.add_argument('--registry-top-n', type=int, default=8)
    parser.add_argument('--web-top-n', type=int, default=8)
    parser.add_argument('--print-top', type=int, default=12)
    parser.add_argument('--euipo-timeout-ms', type=int, default=20000)
    parser.add_argument('--euipo-settle-ms', type=int, default=2500)
    parser.add_argument('--euipo-headful', action='store_true')
    parser.add_argument('--swissreg-timeout-ms', type=int, default=20000)
    parser.add_argument('--swissreg-settle-ms', type=int, default=2500)
    parser.add_argument('--swissreg-headful', action='store_true')
    parser.add_argument('--async-checks', default=DEFAULT_ASYNC_CHECKS)
    parser.add_argument('--async-concurrency', type=int, default=6)
    parser.add_argument('--skip-legal-research', action='store_true')
    parser.add_argument('--no-euipo-probe', action='store_true')
    parser.add_argument('--no-swissreg-ui-probe', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    return parser.parse_args()


def load_selected_rows(path: Path, mode: str) -> list[ReviewRow]:
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        headers = set(reader.fieldnames or [])
        required = {'keep', 'maybe', 'drop'}
        missing = sorted(required - headers)
        if missing:
            raise SystemExit(f'review csv missing columns {missing}: {path}')
        selected: dict[str, ReviewRow] = {}
        for row in reader:
            keep = _is_x(row.get('keep'))
            maybe = _is_x(row.get('maybe'))
            include = keep or (mode == 'keep_maybe' and maybe)
            if not include:
                continue
            decision_tag = 'keep' if keep else 'maybe'
            name_display = str(row.get('name_display') or row.get('name_normalized') or '').strip()
            name_normalized = _normalize_name(row.get('name_normalized') or name_display)
            if not name_normalized:
                continue
            current = ReviewRow(
                name_display=name_display or name_normalized,
                name_normalized=name_normalized,
                decision_tag=decision_tag,
                decision_notes=str(row.get('decision_notes') or '').strip(),
                recommendation=str(
                    row.get('current_recommendation') or row.get('recommendation') or ''
                ).strip().lower(),
                score=_to_float(row.get('score'), _to_float(row.get('current_score'))),
                risk=_to_float(row.get('risk'), _to_float(row.get('current_risk'))),
            )
            previous = selected.get(name_normalized)
            if previous is None:
                selected[name_normalized] = current
                continue
            previous_key = (
                0 if previous.decision_tag == 'keep' else 1,
                -previous.score,
                previous.name_normalized,
            )
            current_key = (
                0 if current.decision_tag == 'keep' else 1,
                -current.score,
                current.name_normalized,
            )
            if current_key < previous_key:
                selected[name_normalized] = current
    rows = list(selected.values())
    rows.sort(key=lambda row: (0 if row.decision_tag == 'keep' else 1, -row.score, row.name_normalized))
    return rows


def write_selected_names(path: Path, rows: list[ReviewRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = '\n'.join(row.name_normalized for row in rows)
    path.write_text(content + ('\n' if content else ''), encoding='utf-8')


def import_review_rows(
    *,
    review_rows: list[ReviewRow],
    review_csv: Path,
    db_path: Path,
    scope: str,
    gate: str,
) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with ndb.open_connection(db_path, wal=True) as conn:
        ndb.ensure_schema(conn, wal=True)
        run_id = ndb.create_run(
            conn,
            source_path=str(review_csv),
            scope=scope,
            gate_mode=gate,
            variation_profile='manual_review_input',
            status='completed',
            config={
                'review_csv': str(review_csv),
                'input_mode': 'review_csv',
                'selected_count': len(review_rows),
            },
            summary={
                'selected_count': len(review_rows),
                'keep_count': sum(1 for row in review_rows if row.decision_tag == 'keep'),
                'maybe_count': sum(1 for row in review_rows if row.decision_tag == 'maybe'),
            },
        )
        for idx, row in enumerate(review_rows, start=1):
            candidate_id = ndb.upsert_candidate(
                conn,
                name_display=row.name_display,
                total_score=row.score,
                risk_score=row.risk,
                recommendation=row.recommendation or None,
                quality_score=row.score,
                engine_id='manual_review',
                parent_ids='',
                status='scored',
                rejection_reason='',
            )
            ndb.add_source(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                source_type='manual_review',
                source_label=f'review_csv:{row.decision_tag}',
                metadata={
                    'review_csv': str(review_csv),
                    'decision_tag': row.decision_tag,
                    'decision_notes': row.decision_notes,
                },
            )
            ndb.add_score_snapshot(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                quality_score=row.score,
                risk_score=row.risk,
                external_penalty=0.0,
                total_score=row.score,
                recommendation=row.recommendation or None,
                hard_fail=False,
                reason='manual_review_import',
            )
            ndb.add_shortlist_decision(
                conn,
                candidate_id=candidate_id,
                run_id=run_id,
                selected=True,
                shortlist_rank=idx,
                bucket_key=row.decision_tag,
                reason=row.decision_notes,
                score=row.score,
            )
        conn.commit()
    return run_id


def load_acceptance_live_results(
    *,
    acceptance_dir: Path,
    keep_count: int,
    maybe_count: int,
) -> dict[str, dict[str, object]]:
    files: list[tuple[str, Path]] = []
    if keep_count > 0:
        files.append(('keep', acceptance_dir / f'finalist_top{keep_count}_live_screening.csv'))
    if maybe_count > 0:
        files.append(('maybe', acceptance_dir / 'maybe_only_live_screening.csv'))
    results: dict[str, dict[str, object]] = {}
    for lane, path in files:
        for row in _read_csv(path):
            name = _normalize_name(row.get('name') or row.get('name_display') or row.get('name_normalized'))
            if not name:
                continue
            live_pass = not _to_bool(row.get('hard_fail')) and not str(row.get('fail_reason') or '').strip()
            results[name] = {
                'acceptance_lane': lane,
                'acceptance_live_status': 'pass' if live_pass else 'fail',
                'acceptance_recommendation': str(row.get('recommendation') or '').strip().lower(),
                'acceptance_total_score': _to_float(row.get('total_score')),
                'acceptance_risk': _to_float(row.get('challenge_risk')),
                'acceptance_fail_reason': str(row.get('fail_reason') or '').strip(),
                'acceptance_domain_com_available': str(row.get('domain_com_available') or '').strip().lower(),
                'acceptance_domain_de_available': str(row.get('domain_de_available') or '').strip().lower(),
                'acceptance_domain_ch_available': str(row.get('domain_ch_available') or '').strip().lower(),
                'acceptance_web_exact_hits': str(row.get('web_exact_hits') or '').strip(),
                'acceptance_web_near_hits': str(row.get('web_near_hits') or '').strip(),
            }
    return results


def load_legal_results(*, acceptance_dir: Path, total_count: int) -> dict[str, dict[str, str]]:
    path = acceptance_dir / f'legal_brand_research_final{total_count}.csv'
    out: dict[str, dict[str, str]] = {}
    for row in _read_csv(path):
        name = _normalize_name(row.get('name') or row.get('name_normalized') or '')
        if not name:
            continue
        out[name] = {
            'legal_status': str(row.get('legal_status') or '').strip().lower(),
            'brand_status': str(row.get('brand_status') or '').strip().lower(),
            'overall_status': str(row.get('overall_status') or '').strip().lower(),
            'legal_notes': str(row.get('notes') or '').strip(),
        }
    return out


def load_async_results(postrank_dir: Path) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    bucket_paths = {
        'survivor': postrank_dir / 'validated_survivors.csv',
        'review': postrank_dir / 'validated_review_queue.csv',
        'rejected': postrank_dir / 'validated_rejected.csv',
        'pending_coverage': postrank_dir / 'validated_pending_coverage.csv',
    }
    for bucket, path in bucket_paths.items():
        for row in _read_csv(path):
            name = _normalize_name(row.get('name') or '')
            if not name:
                continue
            out[name] = {
                'async_publish_bucket': bucket,
                'async_recommendation': str(row.get('recommendation') or '').strip().lower(),
                'async_total_score': str(row.get('total_score') or '').strip(),
                'async_blocker_reasons': str(row.get('blocker_reasons') or '').strip(),
                'async_review_reasons': str(row.get('review_reasons') or '').strip(),
            }
    return out


def combined_status_for(row: dict[str, object]) -> str:
    acceptance_status = str(row.get('acceptance_live_status') or '').strip().lower()
    async_bucket = str(row.get('async_publish_bucket') or '').strip().lower()
    legal_overall = str(row.get('overall_status') or '').strip().lower()
    if acceptance_status == 'fail' or async_bucket == 'rejected':
        return 'blocked'
    if acceptance_status == 'pass' and async_bucket == 'survivor':
        if legal_overall == 'review':
            return 'needs_legal_review'
        if legal_overall == 'block':
            return 'blocked'
        return 'dual_clear'
    if acceptance_status == 'pass' and async_bucket in {'review', 'pending_coverage'}:
        return 'follow_up'
    if acceptance_status == 'pass':
        return 'acceptance_only_pass'
    if async_bucket:
        return 'async_only_signal'
    return 'needs_review'


def combine_results(
    *,
    review_rows: list[ReviewRow],
    acceptance_rows: dict[str, dict[str, object]],
    legal_rows: dict[str, dict[str, str]],
    async_rows: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    combined: list[dict[str, object]] = []
    for row in review_rows:
        payload: dict[str, object] = {
            'name': row.name_normalized,
            'name_display': row.name_display,
            'decision_tag': row.decision_tag,
            'decision_notes': row.decision_notes,
            'input_recommendation': row.recommendation,
            'input_score': f'{row.score:.2f}',
            'input_risk': f'{row.risk:.2f}',
            'acceptance_lane': '',
            'acceptance_live_status': '',
            'acceptance_recommendation': '',
            'acceptance_total_score': '',
            'acceptance_risk': '',
            'acceptance_fail_reason': '',
            'acceptance_domain_com_available': '',
            'acceptance_domain_de_available': '',
            'acceptance_domain_ch_available': '',
            'acceptance_web_exact_hits': '',
            'acceptance_web_near_hits': '',
            'legal_status': '',
            'brand_status': '',
            'overall_status': '',
            'legal_notes': '',
            'async_publish_bucket': '',
            'async_recommendation': '',
            'async_total_score': '',
            'async_blocker_reasons': '',
            'async_review_reasons': '',
        }
        payload.update(acceptance_rows.get(row.name_normalized, {}))
        payload.update(legal_rows.get(row.name_normalized, {}))
        payload.update(async_rows.get(row.name_normalized, {}))
        payload['combined_status'] = combined_status_for(payload)
        combined.append(payload)
    combined.sort(
        key=lambda item: (
            {'dual_clear': 0, 'needs_legal_review': 1, 'follow_up': 2, 'acceptance_only_pass': 3, 'async_only_signal': 4, 'needs_review': 5, 'blocked': 6}.get(
                str(item.get('combined_status') or ''), 9
            ),
            0 if str(item.get('decision_tag') or '') == 'keep' else 1,
            -_to_float(item.get('input_score')),
            str(item.get('name') or ''),
        )
    )
    return combined


def write_combined_summary(path: Path, rows: list[dict[str, object]]) -> None:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get('combined_status') or 'unknown')
        counts[key] = counts.get(key, 0) + 1
    lines = ['# Dual Validation Summary', '']
    lines.append(f'- input_count: {len(rows)}')
    for key in sorted(counts):
        lines.append(f'- {key}: {counts[key]}')
    lines.append('')
    if rows:
        lines.append('| name | decision | combined | acceptance | async | legal | notes |')
        lines.append('|---|---|---|---|---|---|---|')
        for row in rows:
            lines.append(
                '| {name} | {decision} | {combined} | {acceptance} | {async_bucket} | {legal} | {notes} |'.format(
                    name=str(row.get('name') or ''),
                    decision=str(row.get('decision_tag') or ''),
                    combined=str(row.get('combined_status') or ''),
                    acceptance=str(row.get('acceptance_live_status') or ''),
                    async_bucket=str(row.get('async_publish_bucket') or ''),
                    legal=str(row.get('overall_status') or ''),
                    notes=str(row.get('decision_notes') or '').replace('|', '/'),
                )
            )
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> int:
    args = parse_args()
    review_csv = Path(args.review_csv).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    review_rows = load_selected_rows(review_csv, args.mode)
    if not review_rows:
        raise SystemExit(f'no selected review rows found in {review_csv}')
    keep_count = sum(1 for row in review_rows if row.decision_tag == 'keep')
    maybe_count = sum(1 for row in review_rows if row.decision_tag == 'maybe')
    total_count = len(review_rows)

    acceptance_dir = out_dir / 'acceptance_tail'
    db_path = out_dir / 'naming_campaign.db'
    acceptance_cmd = [
        args.python_bin,
        str(ROOT_DIR / 'scripts/branding/run_acceptance_tail.py'),
        '--pack-dir',
        str(out_dir),
        '--decision-csv',
        str(review_csv),
        '--acceptance-dir',
        str(acceptance_dir),
        '--db',
        str(db_path),
        '--mode',
        args.mode,
        '--keep-top-n',
        str(max(0, min(keep_count, int(args.keep_top_n)))),
        '--maybe-top-n',
        str(max(0, min(maybe_count, int(args.maybe_top_n)))),
        '--final-top-n',
        str(max(1, int(args.final_top_n))),
        '--recommended-top-n',
        str(max(1, int(args.recommended_top_n))),
        '--scope',
        args.scope,
        '--gate',
        args.gate,
        '--countries',
        args.countries,
        '--registry-top-n',
        str(max(1, int(args.registry_top_n))),
        '--web-top-n',
        str(max(1, int(args.web_top_n))),
        '--print-top',
        str(max(1, int(args.print_top))),
        '--euipo-timeout-ms',
        str(max(1000, int(args.euipo_timeout_ms))),
        '--euipo-settle-ms',
        str(max(0, int(args.euipo_settle_ms))),
        '--swissreg-timeout-ms',
        str(max(1000, int(args.swissreg_timeout_ms))),
        '--swissreg-settle-ms',
        str(max(0, int(args.swissreg_settle_ms))),
    ]
    if args.skip_legal_research:
        acceptance_cmd.append('--skip-legal-research')
    if args.no_euipo_probe:
        acceptance_cmd.append('--no-euipo-probe')
    if args.no_swissreg_ui_probe:
        acceptance_cmd.append('--no-swissreg-ui-probe')
    if args.euipo_headful:
        acceptance_cmd.append('--euipo-headful')
    if args.swissreg_headful:
        acceptance_cmd.append('--swissreg-headful')
    import_run_id = '<dry_run_import_run_id>'
    async_cmd = [
        args.python_bin,
        str(ROOT_DIR / 'scripts/branding/naming_validate_async.py'),
        '--db',
        str(db_path),
        '--pipeline-version=v3',
        '--enable-v3',
        '--candidate-source',
        'shortlist_selected',
        '--shortlist-source-run-id',
        str(import_run_id),
        '--candidate-limit',
        str(max(1, total_count)),
        '--expensive-finalist-limit',
        str(max(1, total_count)),
        '--validation-tier',
        'all',
        '--scope',
        args.scope,
        '--gate',
        args.gate,
        '--checks',
        args.async_checks,
        '--concurrency',
        str(max(1, int(args.async_concurrency))),
    ]
    if args.dry_run:
        _run(acceptance_cmd, dry_run=True)
        _run(async_cmd, dry_run=True)
        print(f'review_validation_bundle_done out_dir={out_dir} dry_run=true')
        print(f'selected_count={total_count}')
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)

    selected_names = out_dir / 'selected_review_names.txt'
    write_selected_names(selected_names, review_rows)

    import_run_id = import_review_rows(
        review_rows=review_rows,
        review_csv=review_csv,
        db_path=db_path,
        scope=args.scope,
        gate=args.gate,
    )
    async_cmd[9] = str(import_run_id)
    _run(acceptance_cmd, dry_run=False)

    _run(async_cmd, dry_run=False)

    acceptance_rows = load_acceptance_live_results(
        acceptance_dir=acceptance_dir,
        keep_count=keep_count,
        maybe_count=maybe_count,
    )
    legal_rows = load_legal_results(acceptance_dir=acceptance_dir, total_count=total_count)
    async_rows = load_async_results(out_dir / 'postrank')
    combined = combine_results(
        review_rows=review_rows,
        acceptance_rows=acceptance_rows,
        legal_rows=legal_rows,
        async_rows=async_rows,
    )

    combined_csv = out_dir / 'combined_validation_results.csv'
    combined_md = out_dir / 'combined_validation_summary.md'
    headers = [
        'name',
        'name_display',
        'decision_tag',
        'decision_notes',
        'input_recommendation',
        'input_score',
        'input_risk',
        'acceptance_lane',
        'acceptance_live_status',
        'acceptance_recommendation',
        'acceptance_total_score',
        'acceptance_risk',
        'acceptance_fail_reason',
        'acceptance_domain_com_available',
        'acceptance_domain_de_available',
        'acceptance_domain_ch_available',
        'acceptance_web_exact_hits',
        'acceptance_web_near_hits',
        'legal_status',
        'brand_status',
        'overall_status',
        'legal_notes',
        'async_publish_bucket',
        'async_recommendation',
        'async_total_score',
        'async_blocker_reasons',
        'async_review_reasons',
        'combined_status',
    ]
    _write_csv(combined_csv, combined, headers)
    write_combined_summary(combined_md, combined)

    print(f'review_validation_bundle_done out_dir={out_dir}')
    print(f'import_run_id={import_run_id}')
    print(f'selected_names_txt={selected_names}')
    print(f'combined_csv={combined_csv}')
    print(f'combined_md={combined_md}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
