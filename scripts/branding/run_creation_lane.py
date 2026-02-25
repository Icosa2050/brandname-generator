#!/usr/bin/env python3
"""Run compact creation lane from config.

Creation lane boundary:
- optional generation command
- decision-pack build for manual review
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT_DIR / 'resources/branding/configs/creation_lane.default.toml'


def _resolve_path(value: str, *, base: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (base / path).resolve()
    return path


def _fmt(value: str, *, out_dir: Path, db_path: Path) -> str:
    return str(value).format(
        repo_root=str(ROOT_DIR),
        out_dir=str(out_dir),
        db=str(db_path),
    )


def _load_config(path: Path) -> dict[str, Any]:
    with path.open('rb') as handle:
        raw = tomllib.load(handle)
    cfg = raw.get('creation')
    if not isinstance(cfg, dict):
        raise SystemExit(f'Invalid config (missing [creation]): {path}')
    return cfg


def _prepare_cmd(raw_cmd: Any, *, out_dir: Path, db_path: Path) -> list[str]:
    if isinstance(raw_cmd, str):
        text = _fmt(raw_cmd, out_dir=out_dir, db_path=db_path)
        return shlex.split(text)
    if isinstance(raw_cmd, list):
        out: list[str] = []
        for part in raw_cmd:
            out.append(_fmt(str(part), out_dir=out_dir, db_path=db_path))
        return out
    return []


def _run(cmd: list[str], *, timeout_s: int, dry_run: bool) -> None:
    print('$ ' + ' '.join(cmd), flush=True)
    if dry_run:
        return
    subprocess.run(
        cmd,
        cwd=str(ROOT_DIR),
        check=True,
        timeout=None if timeout_s <= 0 else timeout_s,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run compact creation lane from TOML config.')
    parser.add_argument('--config', default=str(DEFAULT_CONFIG), help='Path to creation lane TOML config')
    parser.add_argument('--out-dir', default='', help='Override creation out_dir from config')
    parser.add_argument('--db', default='', help='Override DB path (default: <out_dir>/naming_campaign.db)')
    parser.add_argument('--run-generation', action='store_true', help='Force generation command execution')
    parser.add_argument('--no-run-generation', action='store_true', help='Skip generation command execution')
    parser.add_argument('--dry-run', action='store_true', help='Print commands only')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg_path = _resolve_path(args.config, base=ROOT_DIR)
    cfg = _load_config(cfg_path)

    out_dir_raw = args.out_dir.strip() or str(cfg.get('out_dir') or '')
    if not out_dir_raw:
        raise SystemExit('creation out_dir missing (config [creation].out_dir or --out-dir)')
    out_dir = _resolve_path(out_dir_raw, base=ROOT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    db_raw = args.db.strip() or str(cfg.get('db') or '').strip()
    db_path = _resolve_path(db_raw, base=ROOT_DIR) if db_raw else (out_dir / 'naming_campaign.db')

    run_generation_cfg = bool(cfg.get('run_generation', False))
    if args.run_generation:
        run_generation = True
    elif args.no_run_generation:
        run_generation = False
    else:
        run_generation = run_generation_cfg

    dry_run = bool(cfg.get('dry_run', False)) or bool(args.dry_run)
    generation_timeout_s = int(cfg.get('generation_timeout_s', 0) or 0)
    generation_cmd = _prepare_cmd(cfg.get('generation_command', []), out_dir=out_dir, db_path=db_path)

    if run_generation:
        if not generation_cmd:
            raise SystemExit('run_generation=true but no generation_command set in config')
        _run(generation_cmd, timeout_s=generation_timeout_s, dry_run=dry_run)

    if not db_path.exists():
        if run_generation:
            if dry_run:
                print(f'creation_lane_warn db not found after generation step in dry-run: {db_path}')
            else:
                raise SystemExit(f'db not found after generation step: {db_path}')
        else:
            raise SystemExit(f'db not found: {db_path}')

    pack_output_raw = str(cfg.get('pack_output_dir') or '{out_dir}')
    pack_output = _resolve_path(_fmt(pack_output_raw, out_dir=out_dir, db_path=db_path), base=ROOT_DIR)
    pack_output.mkdir(parents=True, exist_ok=True)
    pack_prefix = str(cfg.get('pack_prefix') or 'decision_pack').strip() or 'decision_pack'
    review_tiers = str(cfg.get('review_tiers') or '120,50')
    include_unchecked = bool(cfg.get('include_unchecked', False))
    python_bin = str(cfg.get('python_bin') or sys.executable).strip() or sys.executable

    cmd = [
        python_bin,
        str(ROOT_DIR / 'scripts/branding/build_decision_pack.py'),
        '--db',
        str(db_path),
        '--out-dir',
        str(pack_output),
        '--pack-prefix',
        pack_prefix,
        '--review-tiers',
        review_tiers,
    ]
    if include_unchecked:
        cmd.append('--include-unchecked')
    _run(cmd, timeout_s=0, dry_run=dry_run)

    print('creation_lane_done')
    print(f'creation_out_dir={out_dir}')
    print(f'creation_db={db_path}')
    print(f'creation_pack_output_dir={pack_output}')
    if dry_run:
        print('next_step=manual_review_then_run_validation_lane')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
