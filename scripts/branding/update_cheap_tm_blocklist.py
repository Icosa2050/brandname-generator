#!/usr/bin/env python3
"""Update cheap trademark collision blocklist tokens.

Supports manual token input and optional import from legal_brand_research CSV output.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def tokenize(raw: str) -> list[str]:
    value = (raw or '').strip().lower()
    if not value:
        return []
    for sep in ('/', ',', ';', '|', ':'):
        value = value.replace(sep, ' ')
    out: list[str] = []
    for chunk in value.split():
        token = ''.join(ch for ch in chunk if 'a' <= ch <= 'z')
        if len(token) >= 3:
            out.append(token)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Update cheap TM collision blocklist.')
    parser.add_argument(
        '--blocklist-file',
        default='resources/branding/inputs/cheap_tm_collision_blocklist_v1.txt',
        help='Blocklist file to update.',
    )
    parser.add_argument(
        '--tokens',
        default='',
        help='Comma-separated tokens/stems to add.',
    )
    parser.add_argument(
        '--tokens-file',
        default='',
        help='Optional newline file with tokens/stems to add.',
    )
    parser.add_argument(
        '--legal-report-csv',
        default='',
        help='Optional legal_brand_research CSV. Adds non-clear names as tokens.',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.blocklist_file).expanduser()
    if not path.exists():
        raise SystemExit(f'blocklist file not found: {path}')

    existing_raw = path.read_text(encoding='utf-8').splitlines()
    existing_tokens: set[str] = set()
    for line in existing_raw:
        if line.strip().startswith('#'):
            continue
        existing_tokens.update(tokenize(line))

    additions: set[str] = set(tokenize(args.tokens))

    if args.tokens_file:
        tf = Path(args.tokens_file).expanduser()
        if tf.exists():
            for line in tf.read_text(encoding='utf-8').splitlines():
                additions.update(tokenize(line))

    if args.legal_report_csv:
        rp = Path(args.legal_report_csv).expanduser()
        if rp.exists():
            with rp.open('r', encoding='utf-8', newline='') as handle:
                for row in csv.DictReader(handle):
                    overall = str(row.get('overall_status') or '').strip().lower()
                    name = str(row.get('name') or '').strip().lower()
                    if name and overall and overall != 'clear':
                        additions.update(tokenize(name))

    new_tokens = sorted(token for token in additions if token and token not in existing_tokens)
    if not new_tokens:
        print('no new tokens to add')
        return 0

    auto_header = '# auto-added tokens'
    lines = existing_raw[:]
    if auto_header not in lines:
        if lines and lines[-1].strip():
            lines.append('')
        lines.append(auto_header)
    elif lines and lines[-1].strip() == auto_header:
        # Keep file tidy when the header is currently the last line.
        pass
    lines.extend(new_tokens)
    path.write_text('\n'.join(lines).rstrip() + '\n', encoding='utf-8')
    print(f'added_tokens={len(new_tokens)} file={path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
