#!/usr/bin/env python3
"""Automate acceptance-tail checks from a decision pack.

This script turns the manual "last mile" into one repeatable flow:
1) Build acceptance preflight from curated keep/maybe decisions.
2) Select lane candidates (keep / maybe).
3) Run strict live screening for each lane.
4) Merge strict pass survivors into final N names.
5) Run automated legal+brand precheck on final survivors.
6) Emit a compact recommendation list and summary.
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent


def _to_bool(value: str | None) -> bool:
    token = str(value or '').strip().lower()
    return token in {'1', 'true', 'yes', 'y', 'on'}


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _rec_rank(value: str | None) -> int:
    token = str(value or '').strip().lower()
    if token == 'strong':
        return 3
    if token == 'consider':
        return 2
    if token == 'weak':
        return 1
    return 0


def _status_rank(value: str | None) -> int:
    token = str(value or '').strip().lower()
    if token == 'clear':
        return 0
    if token == 'review':
        return 1
    if token == 'block':
        return 2
    return 3


def _normalize_name(raw: str | None) -> str:
    return ''.join(ch for ch in str(raw or '').strip().lower() if ch.isalpha())


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


def _write_names(path: Path, names: Iterable[str]) -> None:
    cleaned = [str(name).strip() for name in names if str(name).strip()]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(cleaned) + ('\n' if cleaned else ''), encoding='utf-8')


def _run(cmd: list[str]) -> None:
    print('$ ' + ' '.join(cmd), flush=True)
    proc = subprocess.run(cmd, cwd=str(ROOT_DIR))
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


@dataclass(frozen=True)
class LaneConfig:
    lane: str
    top_n: int
    candidates_txt: Path
    candidates_csv: Path
    live_csv: Path
    live_json: Path
    live_runs: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Automate acceptance-tail filtering + checks.')
    parser.add_argument('--pack-dir', required=True, help='Decision pack directory.')
    parser.add_argument(
        '--db',
        default='',
        help='Path to naming_campaign.db (default: <pack-parent>/naming_campaign.db).',
    )
    parser.add_argument(
        '--decision-csv',
        default='',
        help='Curated decision CSV (default: <pack-dir>/review_unique_top120.csv).',
    )
    parser.add_argument(
        '--acceptance-dir',
        default='',
        help='Acceptance output dir (default: <pack-dir>/acceptance_keep_only).',
    )
    parser.add_argument('--mode', choices=['keep', 'keep_maybe'], default='keep_maybe')
    parser.add_argument('--keep-top-n', type=int, default=12)
    parser.add_argument('--maybe-top-n', type=int, default=12)
    parser.add_argument('--final-top-n', type=int, default=8)
    parser.add_argument('--recommended-top-n', type=int, default=6)
    parser.add_argument('--scope', default='global')
    parser.add_argument('--gate', default='strict')
    parser.add_argument('--countries', default='de,ch,it')
    parser.add_argument('--skip-live-screening', action='store_true')
    parser.add_argument('--skip-legal-research', action='store_true')
    parser.add_argument('--registry-top-n', type=int, default=8)
    parser.add_argument('--web-top-n', type=int, default=8)
    parser.add_argument('--print-top', type=int, default=12)
    parser.add_argument('--euipo-probe', dest='euipo_probe', action='store_true', default=True)
    parser.add_argument('--no-euipo-probe', dest='euipo_probe', action='store_false')
    parser.add_argument('--euipo-timeout-ms', type=int, default=20000)
    parser.add_argument('--euipo-settle-ms', type=int, default=2500)
    parser.add_argument('--euipo-headful', action='store_true')
    parser.add_argument('--swissreg-ui-probe', dest='swissreg_ui_probe', action='store_true', default=True)
    parser.add_argument('--no-swissreg-ui-probe', dest='swissreg_ui_probe', action='store_false')
    parser.add_argument('--swissreg-timeout-ms', type=int, default=20000)
    parser.add_argument('--swissreg-settle-ms', type=int, default=2500)
    parser.add_argument('--swissreg-headful', action='store_true')
    parser.add_argument('--python-bin', default=sys.executable, help='Python interpreter to use.')
    return parser.parse_args()


def select_lane(
    ranked_rows: list[dict[str, str]],
    *,
    lane: str,
    top_n: int,
) -> list[dict[str, str]]:
    if top_n <= 0:
        return []
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in ranked_rows:
        if str(row.get('decision_tag') or '').strip().lower() != lane:
            continue
        name = _normalize_name(row.get('name_normalized') or row.get('name_display') or '')
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(row)
        if len(out) >= top_n:
            break
    return out


def parse_pass_rows(path: Path, lane: str) -> list[dict[str, object]]:
    rows = _read_csv(path)
    survivors: list[dict[str, object]] = []
    for row in rows:
        name = _normalize_name(row.get('name') or row.get('name_display') or row.get('name_normalized') or '')
        if not name:
            continue
        hard_fail = _to_bool(row.get('hard_fail'))
        fail_reason = str(row.get('fail_reason') or '').strip()
        if hard_fail or fail_reason:
            continue
        survivors.append(
            {
                'name': name,
                'lane': lane,
                'recommendation': str(row.get('recommendation') or '').strip().lower(),
                'total_score': _to_float(row.get('total_score')),
                'challenge_risk': _to_float(row.get('challenge_risk')),
                'domain_com_available': str(row.get('domain_com_available') or '').strip().lower(),
                'domain_de_available': str(row.get('domain_de_available') or '').strip().lower(),
                'domain_ch_available': str(row.get('domain_ch_available') or '').strip().lower(),
                'itunes_de_count': _to_int(row.get('itunes_de_count')),
                'itunes_ch_count': _to_int(row.get('itunes_ch_count')),
                'web_exact_hits': _to_int(row.get('web_exact_hits')),
                'web_near_hits': _to_int(row.get('web_near_hits')),
                'trademark_dpma_url': str(row.get('trademark_dpma_url') or '').strip(),
                'trademark_swissreg_url': str(row.get('trademark_swissreg_url') or '').strip(),
                'trademark_tmview_url': str(row.get('trademark_tmview_url') or '').strip(),
            }
        )
    return survivors


def rank_survivors(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    dedup: dict[str, dict[str, object]] = {}
    for row in rows:
        name = str(row.get('name') or '')
        if not name:
            continue
        current = dedup.get(name)
        if current is None:
            dedup[name] = row
            continue
        cur_key = (
            _rec_rank(str(current.get('recommendation') or '')),
            _to_float(current.get('total_score')),
            -_to_float(current.get('challenge_risk')),
            -_to_int(current.get('web_exact_hits')),
            -_to_int(current.get('web_near_hits')),
            0 if str(current.get('lane') or '') == 'keep' else 1,
        )
        new_key = (
            _rec_rank(str(row.get('recommendation') or '')),
            _to_float(row.get('total_score')),
            -_to_float(row.get('challenge_risk')),
            -_to_int(row.get('web_exact_hits')),
            -_to_int(row.get('web_near_hits')),
            0 if str(row.get('lane') or '') == 'keep' else 1,
        )
        if new_key > cur_key:
            dedup[name] = row

    ranked = list(dedup.values())
    ranked.sort(
        key=lambda row: (
            -_rec_rank(str(row.get('recommendation') or '')),
            -_to_float(row.get('total_score')),
            _to_float(row.get('challenge_risk')),
            _to_int(row.get('web_exact_hits')),
            _to_int(row.get('web_near_hits')),
            0 if str(row.get('lane') or '') == 'keep' else 1,
            str(row.get('name') or ''),
        )
    )
    for idx, row in enumerate(ranked, start=1):
        row['rank'] = idx
    return ranked


def choose_recommended(
    survivors: list[dict[str, object]],
    legal_rows: list[dict[str, str]],
    *,
    top_n: int,
) -> list[dict[str, object]]:
    by_name = {_normalize_name(row.get('name') or ''): row for row in survivors}
    merged: list[dict[str, object]] = []
    if legal_rows:
        for row in legal_rows:
            name = _normalize_name(row.get('name') or '')
            source = by_name.get(name)
            if not name or source is None:
                continue
            merged_row = dict(source)
            merged_row['legal_status'] = str(row.get('legal_status') or '').strip().lower()
            merged_row['brand_status'] = str(row.get('brand_status') or '').strip().lower()
            merged_row['overall_status'] = str(row.get('overall_status') or '').strip().lower()
            merged_row['notes'] = str(row.get('notes') or '').strip()
            merged.append(merged_row)
    else:
        merged = [dict(row) for row in survivors]
        for row in merged:
            row['legal_status'] = 'unknown'
            row['brand_status'] = 'unknown'
            row['overall_status'] = 'unknown'
            row['notes'] = ''

    merged.sort(
        key=lambda row: (
            _status_rank(str(row.get('overall_status') or '')),
            -_rec_rank(str(row.get('recommendation') or '')),
            -_to_float(row.get('total_score')),
            _to_float(row.get('challenge_risk')),
            str(row.get('name') or ''),
        )
    )
    out = merged[: max(1, int(top_n))]
    for idx, row in enumerate(out, start=1):
        row['recommended_rank'] = idx
    return out


def write_summary(
    path: Path,
    *,
    pack_dir: Path,
    db_path: Path,
    keep_count: int,
    maybe_count: int,
    keep_survivors: int,
    maybe_survivors: int,
    final_survivors: list[dict[str, object]],
    legal_csv: Path | None,
    recommended_rows: list[dict[str, object]],
) -> None:
    lines: list[str] = []
    lines.append('# Acceptance Tail Summary')
    lines.append('')
    lines.append(f'- pack_dir: `{pack_dir}`')
    lines.append(f'- db: `{db_path}`')
    lines.append(f'- keep_candidates: {keep_count}')
    lines.append(f'- maybe_candidates: {maybe_count}')
    lines.append(f'- keep_live_survivors: {keep_survivors}')
    lines.append(f'- maybe_live_survivors: {maybe_survivors}')
    lines.append(f'- final_survivors: {len(final_survivors)}')
    if legal_csv is not None:
        lines.append(f'- legal_research_csv: `{legal_csv}`')
    lines.append('')
    if final_survivors:
        lines.append('## Final Survivors')
        lines.append('')
        lines.append('| rank | name | lane | rec | total | risk |')
        lines.append('|---:|---|---|---|---:|---:|')
        for row in final_survivors:
            lines.append(
                '| {rank} | {name} | {lane} | {rec} | {total:.1f} | {risk:.1f} |'.format(
                    rank=_to_int(row.get('rank')),
                    name=str(row.get('name') or ''),
                    lane=str(row.get('lane') or ''),
                    rec=str(row.get('recommendation') or ''),
                    total=_to_float(row.get('total_score')),
                    risk=_to_float(row.get('challenge_risk')),
                )
            )
        lines.append('')
    if recommended_rows:
        lines.append('## Recommended For Counsel')
        lines.append('')
        lines.append('| rank | name | overall | legal | brand | rec | total | risk |')
        lines.append('|---:|---|---|---|---|---|---:|---:|')
        for row in recommended_rows:
            lines.append(
                '| {rank} | {name} | {overall} | {legal} | {brand} | {rec} | {total:.1f} | {risk:.1f} |'.format(
                    rank=_to_int(row.get('recommended_rank')),
                    name=str(row.get('name') or ''),
                    overall=str(row.get('overall_status') or ''),
                    legal=str(row.get('legal_status') or ''),
                    brand=str(row.get('brand_status') or ''),
                    rec=str(row.get('recommendation') or ''),
                    total=_to_float(row.get('total_score')),
                    risk=_to_float(row.get('challenge_risk')),
                )
            )
        lines.append('')
    path.write_text('\n'.join(lines), encoding='utf-8')


def main() -> int:
    args = parse_args()
    pack_dir = Path(args.pack_dir).expanduser().resolve()
    if not pack_dir.exists():
        raise SystemExit(f'pack dir not found: {pack_dir}')

    decision_csv = Path(args.decision_csv).expanduser().resolve() if args.decision_csv else pack_dir / 'review_unique_top120.csv'
    acceptance_dir = Path(args.acceptance_dir).expanduser().resolve() if args.acceptance_dir else pack_dir / 'acceptance_keep_only'
    db_path = Path(args.db).expanduser().resolve() if args.db else pack_dir.parent / 'naming_campaign.db'

    if not decision_csv.exists():
        raise SystemExit(f'decision csv not found: {decision_csv}')
    if not db_path.exists():
        raise SystemExit(f'db not found: {db_path}')
    acceptance_dir.mkdir(parents=True, exist_ok=True)

    preflight_cmd = [
        args.python_bin,
        str(ROOT_DIR / 'scripts/branding/build_acceptance_preflight.py'),
        '--decision-csv',
        str(decision_csv),
        '--db',
        str(db_path),
        '--out-dir',
        str(acceptance_dir),
        '--mode',
        args.mode,
        '--top-n',
        str(max(args.keep_top_n, args.maybe_top_n, args.print_top)),
    ]
    _run(preflight_cmd)

    ranked_csv = acceptance_dir / 'acceptance_ranked.csv'
    ranked_rows = _read_csv(ranked_csv)
    if not ranked_rows:
        raise SystemExit(f'no ranked rows in {ranked_csv}')

    keep_lane = LaneConfig(
        lane='keep',
        top_n=max(0, int(args.keep_top_n)),
        candidates_txt=acceptance_dir / f'finalist_top{max(0, int(args.keep_top_n))}.txt',
        candidates_csv=acceptance_dir / f'finalist_top{max(0, int(args.keep_top_n))}.csv',
        live_csv=acceptance_dir / f'finalist_top{max(0, int(args.keep_top_n))}_live_screening.csv',
        live_json=acceptance_dir / f'finalist_top{max(0, int(args.keep_top_n))}_live_screening.json',
        live_runs=acceptance_dir / f'finalist_top{max(0, int(args.keep_top_n))}_live_screening_runs.jsonl',
    )
    maybe_lane = LaneConfig(
        lane='maybe',
        top_n=max(0, int(args.maybe_top_n)),
        candidates_txt=acceptance_dir / 'maybe_only.txt',
        candidates_csv=acceptance_dir / 'maybe_only.csv',
        live_csv=acceptance_dir / 'maybe_only_live_screening.csv',
        live_json=acceptance_dir / 'maybe_only_live_screening.json',
        live_runs=acceptance_dir / 'maybe_only_live_screening_runs.jsonl',
    )

    lanes: list[LaneConfig] = [keep_lane]
    if args.mode == 'keep_maybe' and maybe_lane.top_n > 0:
        lanes.append(maybe_lane)

    lane_candidates: dict[str, list[dict[str, str]]] = {}
    for lane_cfg in lanes:
        selected = select_lane(ranked_rows, lane=lane_cfg.lane, top_n=lane_cfg.top_n)
        lane_candidates[lane_cfg.lane] = selected
        out_rows = [
            {
                'rank': _to_int(row.get('rank')),
                'name': _normalize_name(row.get('name_normalized') or row.get('name_display') or ''),
                'name_display': str(row.get('name_display') or ''),
                'decision_tag': str(row.get('decision_tag') or ''),
                'recommendation': str(row.get('current_recommendation') or ''),
                'score': _to_float(row.get('current_score')),
                'risk': _to_float(row.get('current_risk')),
                'strict_pass': _to_int(row.get('strict_pass')),
            }
            for row in selected
        ]
        _write_csv(
            lane_cfg.candidates_csv,
            out_rows,
            ['rank', 'name', 'name_display', 'decision_tag', 'recommendation', 'score', 'risk', 'strict_pass'],
        )
        _write_names(lane_cfg.candidates_txt, [row['name'] for row in out_rows])

    if not args.skip_live_screening:
        for lane_cfg in lanes:
            selected = lane_candidates.get(lane_cfg.lane, [])
            names = [_normalize_name(row.get('name_normalized') or row.get('name_display') or '') for row in selected]
            names = [name for name in names if name]
            if not names:
                continue
            cmd = [
                args.python_bin,
                str(ROOT_DIR / 'scripts/branding/name_generator.py'),
                '--scope',
                args.scope,
                '--gate',
                args.gate,
                '--candidates',
                ','.join(names),
                '--only-candidates',
                '--pool-size',
                str(len(names)),
                '--check-limit',
                str(len(names)),
                '--store-countries',
                args.countries,
                '--output',
                str(lane_cfg.live_csv),
                '--json-output',
                str(lane_cfg.live_json),
                '--run-log',
                str(lane_cfg.live_runs),
            ]
            _run(cmd)

    all_survivors: list[dict[str, object]] = []
    keep_survivors = 0
    maybe_survivors = 0
    for lane_cfg in lanes:
        if not lane_cfg.live_csv.exists():
            continue
        survivors = parse_pass_rows(lane_cfg.live_csv, lane_cfg.lane)
        all_survivors.extend(survivors)
        if lane_cfg.lane == 'keep':
            keep_survivors = len(survivors)
        elif lane_cfg.lane == 'maybe':
            maybe_survivors = len(survivors)

    ranked_survivors = rank_survivors(all_survivors)
    final_survivors = ranked_survivors[: max(1, int(args.final_top_n))]

    final_csv = acceptance_dir / f'final_survivors_{max(1, int(args.final_top_n))}.csv'
    final_txt = acceptance_dir / f'final_survivors_{max(1, int(args.final_top_n))}.txt'
    final_headers = [
        'rank',
        'name',
        'lane',
        'recommendation',
        'total_score',
        'challenge_risk',
        'domain_com_available',
        'domain_de_available',
        'domain_ch_available',
        'itunes_de_count',
        'itunes_ch_count',
        'web_exact_hits',
        'web_near_hits',
        'trademark_dpma_url',
        'trademark_swissreg_url',
        'trademark_tmview_url',
    ]
    _write_csv(final_csv, final_survivors, final_headers)
    _write_names(final_txt, [str(row.get('name') or '') for row in final_survivors])

    legal_csv: Path | None = None
    legal_rows: list[dict[str, str]] = []
    if not args.skip_legal_research and final_survivors:
        legal_prefix = acceptance_dir / f'legal_brand_research_final{max(1, int(args.final_top_n))}'
        legal_cmd = [
            args.python_bin,
            str(ROOT_DIR / 'scripts/branding/legal_brand_research.py'),
            '--names-file',
            str(final_txt),
            '--countries',
            args.countries,
            '--registry-top-n',
            str(max(1, int(args.registry_top_n))),
            '--web-top-n',
            str(max(1, int(args.web_top_n))),
            '--output-csv',
            str(legal_prefix) + '.csv',
            '--output-json',
            str(legal_prefix) + '.json',
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
        legal_cmd.append('--euipo-probe' if args.euipo_probe else '--no-euipo-probe')
        legal_cmd.append('--swissreg-ui-probe' if args.swissreg_ui_probe else '--no-swissreg-ui-probe')
        if args.euipo_headful:
            legal_cmd.append('--euipo-headful')
        if args.swissreg_headful:
            legal_cmd.append('--swissreg-headful')
        _run(legal_cmd)
        legal_csv = Path(str(legal_prefix) + '.csv')
        legal_rows = _read_csv(legal_csv)

    recommended = choose_recommended(
        final_survivors,
        legal_rows,
        top_n=max(1, int(args.recommended_top_n)),
    )
    recommended_csv = acceptance_dir / f'recommended_top{max(1, int(args.recommended_top_n))}_for_legal.csv'
    recommended_md = acceptance_dir / f'recommended_top{max(1, int(args.recommended_top_n))}_for_legal.md'
    _write_csv(
        recommended_csv,
        recommended,
        [
            'recommended_rank',
            'name',
            'lane',
            'overall_status',
            'legal_status',
            'brand_status',
            'recommendation',
            'total_score',
            'challenge_risk',
            'domain_com_available',
            'domain_de_available',
            'domain_ch_available',
            'itunes_de_count',
            'itunes_ch_count',
            'web_exact_hits',
            'web_near_hits',
            'trademark_dpma_url',
            'trademark_swissreg_url',
            'trademark_tmview_url',
            'notes',
        ],
    )
    md_lines = [
        f'# Recommended Top {max(1, int(args.recommended_top_n))} For Legal',
        '',
        '| rank | name | lane | overall | legal | brand | rec | total | risk |',
        '|---:|---|---|---|---|---|---|---:|---:|',
    ]
    for row in recommended:
        md_lines.append(
            '| {rank} | {name} | {lane} | {overall} | {legal} | {brand} | {rec} | {total:.1f} | {risk:.1f} |'.format(
                rank=_to_int(row.get('recommended_rank')),
                name=str(row.get('name') or ''),
                lane=str(row.get('lane') or ''),
                overall=str(row.get('overall_status') or ''),
                legal=str(row.get('legal_status') or ''),
                brand=str(row.get('brand_status') or ''),
                rec=str(row.get('recommendation') or ''),
                total=_to_float(row.get('total_score')),
                risk=_to_float(row.get('challenge_risk')),
            )
        )
    recommended_md.write_text('\n'.join(md_lines) + '\n', encoding='utf-8')

    summary_md = acceptance_dir / 'acceptance_tail_summary.md'
    write_summary(
        summary_md,
        pack_dir=pack_dir,
        db_path=db_path,
        keep_count=len(lane_candidates.get('keep', [])),
        maybe_count=len(lane_candidates.get('maybe', [])),
        keep_survivors=keep_survivors,
        maybe_survivors=maybe_survivors,
        final_survivors=final_survivors,
        legal_csv=legal_csv,
        recommended_rows=recommended,
    )

    print(f'acceptance_tail_done pack_dir={pack_dir}')
    print(f'acceptance_dir={acceptance_dir}')
    print(f'final_survivors_csv={final_csv}')
    print(f'final_survivors_txt={final_txt}')
    if legal_csv is not None:
        print(f'legal_research_csv={legal_csv}')
    print(f'recommended_csv={recommended_csv}')
    print(f'recommended_md={recommended_md}')
    print(f'summary_md={summary_md}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
