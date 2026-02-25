#!/usr/bin/env python3
"""Run post-review validation lane from config."""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT_DIR / 'resources/branding/configs/validation_lane.default.toml'
REQUIRED_REVIEW_COLUMNS = ('keep', 'maybe', 'drop')


def _resolve_path(value: str, *, base: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (base / path).resolve()
    return path


def _load_config(path: Path) -> dict[str, Any]:
    with path.open('rb') as handle:
        raw = tomllib.load(handle)
    cfg = raw.get('validation')
    if not isinstance(cfg, dict):
        raise SystemExit(f'Invalid config (missing [validation]): {path}')
    return cfg


def _is_x(value: str | None) -> bool:
    return str(value or '').strip().lower() == 'x'


def _count_review_decisions(path: Path) -> tuple[int, int]:
    total_rows = 0
    decisions = 0
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        missing = [col for col in REQUIRED_REVIEW_COLUMNS if col not in (reader.fieldnames or [])]
        if missing:
            raise SystemExit(f'review csv missing columns {missing}: {path}')
        for row in reader:
            total_rows += 1
            if _is_x(row.get('keep')) or _is_x(row.get('maybe')) or _is_x(row.get('drop')):
                decisions += 1
    return total_rows, decisions


def _run(cmd: list[str], *, dry_run: bool) -> None:
    print('$ ' + ' '.join(cmd), flush=True)
    if dry_run:
        return
    subprocess.run(cmd, cwd=str(ROOT_DIR), check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run post-review validation lane from TOML config.')
    parser.add_argument('--config', default=str(DEFAULT_CONFIG), help='Path to validation lane TOML config')
    parser.add_argument('--pack-dir', default='', help='Decision pack directory override')
    parser.add_argument('--decision-csv', default='', help='Manual review CSV override')
    parser.add_argument('--dry-run', action='store_true', help='Print commands only')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg_path = _resolve_path(args.config, base=ROOT_DIR)
    cfg = _load_config(cfg_path)

    pack_dir_raw = args.pack_dir.strip() or str(cfg.get('pack_dir') or '').strip()
    if not pack_dir_raw:
        raise SystemExit('pack_dir missing (config [validation].pack_dir or --pack-dir)')
    pack_dir = _resolve_path(pack_dir_raw, base=ROOT_DIR)
    if not pack_dir.exists():
        raise SystemExit(f'pack_dir not found: {pack_dir}')

    decision_csv_raw = args.decision_csv.strip() or str(cfg.get('decision_csv') or 'review_unique_top120.csv').strip()
    decision_csv = _resolve_path(decision_csv_raw, base=pack_dir if not Path(decision_csv_raw).is_absolute() else ROOT_DIR)
    if not decision_csv.exists():
        raise SystemExit(f'decision csv not found: {decision_csv}')

    require_human_decisions = bool(cfg.get('require_human_decisions', True))
    min_human_decisions = max(0, int(cfg.get('min_human_decisions', 1) or 1))
    total_rows, decisions = _count_review_decisions(decision_csv)
    if require_human_decisions and decisions < min_human_decisions:
        raise SystemExit(
            f'review csv has insufficient manual decisions: {decisions} < {min_human_decisions} ({decision_csv})'
        )

    python_bin = str(cfg.get('python_bin') or sys.executable).strip() or sys.executable
    cmd = [
        python_bin,
        str(ROOT_DIR / 'scripts/branding/run_acceptance_tail.py'),
        '--pack-dir',
        str(pack_dir),
        '--decision-csv',
        str(decision_csv),
        '--mode',
        str(cfg.get('mode', 'keep_maybe')),
        '--keep-top-n',
        str(int(cfg.get('keep_top_n', 12))),
        '--maybe-top-n',
        str(int(cfg.get('maybe_top_n', 12))),
        '--final-top-n',
        str(int(cfg.get('final_top_n', 8))),
        '--recommended-top-n',
        str(int(cfg.get('recommended_top_n', 6))),
        '--scope',
        str(cfg.get('scope', 'global')),
        '--gate',
        str(cfg.get('gate', 'strict')),
        '--countries',
        str(cfg.get('countries', 'de,ch,it')),
        '--registry-top-n',
        str(int(cfg.get('registry_top_n', 8))),
        '--web-top-n',
        str(int(cfg.get('web_top_n', 8))),
        '--print-top',
        str(int(cfg.get('print_top', 12))),
        '--euipo-timeout-ms',
        str(int(cfg.get('euipo_timeout_ms', 20000))),
        '--euipo-settle-ms',
        str(int(cfg.get('euipo_settle_ms', 2500))),
        '--swissreg-timeout-ms',
        str(int(cfg.get('swissreg_timeout_ms', 20000))),
        '--swissreg-settle-ms',
        str(int(cfg.get('swissreg_settle_ms', 2500))),
    ]

    if bool(cfg.get('skip_live_screening', False)):
        cmd.append('--skip-live-screening')
    if bool(cfg.get('skip_legal_research', False)):
        cmd.append('--skip-legal-research')
    cmd.append('--euipo-probe' if bool(cfg.get('euipo_probe', True)) else '--no-euipo-probe')
    cmd.append('--swissreg-ui-probe' if bool(cfg.get('swissreg_ui_probe', True)) else '--no-swissreg-ui-probe')
    if bool(cfg.get('euipo_headful', False)):
        cmd.append('--euipo-headful')
    if bool(cfg.get('swissreg_headful', False)):
        cmd.append('--swissreg-headful')

    dry_run = bool(cfg.get('dry_run', False)) or bool(args.dry_run)
    _run(cmd, dry_run=dry_run)

    print('validation_lane_done')
    print(f'validation_pack_dir={pack_dir}')
    print(f'validation_decision_csv={decision_csv}')
    print(f'validation_review_rows={total_rows}')
    print(f'validation_manual_decisions={decisions}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
