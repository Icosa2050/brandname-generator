#!/usr/bin/env python3
"""Generate candidate prefixes and pre-screen web collision risk.

Purpose:
- front-load collision elimination before expensive legal filing work
- provide a ranked list of low-collision prefixes for downstream name synthesis
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import name_generator as ng


START_SYLLABLES = [
    'ba', 'be', 'bi', 'bo',
    'ca', 'ce', 'ci', 'co',
    'da', 'de', 'di', 'do',
    'fa', 'fe', 'fi', 'fo',
    'ga', 'ge', 'gi', 'go',
    'la', 'le', 'li', 'lo',
    'ma', 'me', 'mi', 'mo',
    'na', 'ne', 'ni', 'no',
    'pa', 'pe', 'pi', 'po',
    'ra', 're', 'ri', 'ro',
    'sa', 'se', 'si', 'so',
    'ta', 'te', 'ti', 'to',
    'va', 've', 'vi', 'vo',
    'bra', 'bre', 'bri', 'bro',
    'dra', 'dre', 'dri', 'dro',
    'fra', 'fre', 'fri', 'fro',
    'gra', 'gre', 'gri', 'gro',
    'pra', 'pre', 'pri', 'pro',
    'tra', 'tre', 'tri', 'tro',
]
END_SYLLABLES = [
    'la', 'ra', 'na', 'ta', 'sa',
    'lo', 'ro', 'no', 'to', 'so',
    'li', 'ri', 'ni', 'ti', 'si',
    'ma', 'va', 'ca', 'da', 'ga',
    'len', 'ren', 'lin', 'ron', 'lan',
    'dor', 'tor', 'vor', 'mon', 'nor',
]
BAD_CLUSTERS = {
    'qz', 'zx', 'xq', 'jj', 'vvv', 'kkk', 'zzz', 'oae', 'eoa', 'iia', 'uua', 'wq', 'qh', 'hx',
}
LOWER_QUALITY_PATTERNS = (
    'aa', 'ee', 'ii', 'uu', 'yy',
    'aei', 'eio', 'ioa', 'oua', 'aou',
    'ao', 'eo', 'oe', 'uo',
)


@dataclass
class PrefixAuditRow:
    prefix: str
    pronounce_score: int
    pronounce_flags: str
    web_exact_hits: int
    web_near_hits: int
    web_result_count: int
    web_query_ok: bool
    web_source: str
    web_sample_domains: str
    com_available: str
    de_available: str
    ch_available: str
    risk_score: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate + audit candidate prefixes for collision risk.')
    parser.add_argument('--count', type=int, default=40, help='Number of accepted prefixes to return.')
    parser.add_argument('--pool-size', type=int, default=240, help='Raw generated pool before collision checks.')
    parser.add_argument('--min-len', type=int, default=5)
    parser.add_argument('--max-len', type=int, default=8)
    parser.add_argument('--web-top', type=int, default=8)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument(
        '--collision-blocklist-file',
        default='resources/branding/inputs/cheap_tm_collision_blocklist_v1.txt',
        help='Token/stem blocklist to avoid at generation time.',
    )
    parser.add_argument(
        '--output-csv',
        default='test_outputs/branding/prefix_collision_audit/latest_prefix_audit.csv',
        help='Output CSV path.',
    )
    parser.add_argument(
        '--output-json',
        default='test_outputs/branding/prefix_collision_audit/latest_prefix_audit.json',
        help='Output JSON path.',
    )
    parser.add_argument('--print-top', type=int, default=20)
    parser.add_argument(
        '--min-pronounce-score',
        type=int,
        default=78,
        help='Minimum pronounceability score required for accepted prefixes.',
    )
    return parser.parse_args()


def load_blocklist(path: str) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    out: set[str] = set()
    for raw in p.read_text(encoding='utf-8').splitlines():
        line = raw.strip().lower()
        if not line or line.startswith('#'):
            continue
        for part in line.replace(',', ' ').replace(';', ' ').replace('|', ' ').split():
            token = ng.normalize_alpha(part)
            if len(token) >= 3:
                out.add(token)
    return out


def pronounceability_score(prefix: str) -> tuple[int, str]:
    score = 100
    flags: list[str] = []
    vowels = sum(1 for ch in prefix if ch in 'aeiouy')
    vowel_ratio = vowels / max(1, len(prefix))

    if vowels < 2:
        score -= 20
        flags.append('too_few_vowels')
    if vowel_ratio < 0.28:
        score -= 12
        flags.append('low_vowel_ratio')
    if vowels > max(2, len(prefix) - 2):
        score -= 12
        flags.append('too_many_vowels')

    for bad in BAD_CLUSTERS:
        if bad in prefix:
            score -= 18
            flags.append(f'bad_cluster:{bad}')
    for pattern in LOWER_QUALITY_PATTERNS:
        if pattern in prefix:
            score -= 8
            flags.append(f'awkward_vowel_seq:{pattern}')

    if re.search(r'[^aeiouy]{4,}', prefix):
        score -= 24
        flags.append('long_consonant_run')
    if any(prefix.startswith(x) for x in ('x', 'q', 'zz', 'tz', 'xh')):
        score -= 14
        flags.append('bad_start')
    if any(prefix.endswith(x) for x in ('x', 'q', 'zz', 'tz', 'ckr')):
        score -= 10
        flags.append('bad_end')

    if re.search(r'(.)\1\1', prefix):
        score -= 14
        flags.append('triple_repeat')
    if len(set(prefix)) < max(4, len(prefix) // 2):
        score -= 8
        flags.append('low_letter_variety')

    if prefix.count('r') > 2:
        score -= 6
        flags.append('heavy_r')
    if prefix.count('z') > 1:
        score -= 10
        flags.append('heavy_z')

    if prefix.endswith(('ium', 'eum', 'aio', 'eio')):
        score -= 10
        flags.append('awkward_ending')
    if re.search(r'(s|t|r|n)(s|t|r|n)(s|t|r|n)$', prefix):
        score -= 10
        flags.append('hard_terminal_cluster')
    if not re.search(r'[aeiouy].*[aeiouy]', prefix):
        score -= 10
        flags.append('single_vowel_path')
    if len(prefix) >= 8 and re.search(r'[aeiouy]{3,}', prefix):
        score -= 8
        flags.append('long_vowel_run')
    if len(prefix) < 5 or len(prefix) > 8:
        score -= 16
        flags.append('length_out_of_band')

    return max(0, min(100, score)), ';'.join(sorted(set(flags))[:8])


def is_western_pronounceable(prefix: str, min_score: int) -> bool:
    score, _ = pronounceability_score(prefix)
    if score < min_score:
        return False
    shape = ''.join('V' if ch in 'aeiouy' else 'C' for ch in prefix)
    if 'CCCC' in shape:
        return False
    if shape.startswith('CCC') or shape.endswith('CCC'):
        return False
    return True


def random_prefix(rng: random.Random, min_len: int, max_len: int) -> str:
    token = rng.choice(START_SYLLABLES) + rng.choice(END_SYLLABLES)
    token = ng.normalize_alpha(token)
    if len(token) > max_len:
        token = token[:max_len]
    if len(token) < min_len:
        token += rng.choice(['ra', 'na', 'la', 'ro', 'no'])
    return ng.normalize_alpha(token)


def generate_pool(
    *,
    rng: random.Random,
    n: int,
    min_len: int,
    max_len: int,
    blocklist: set[str],
    min_pronounce_score: int,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    attempts = 0
    max_attempts = max(1000, n * 80)
    while len(out) < n and attempts < max_attempts:
        attempts += 1
        name = random_prefix(rng, min_len=min_len, max_len=max_len)
        if len(name) < min_len or len(name) > max_len:
            continue
        if name in seen:
            continue
        if any(tok in name for tok in blocklist):
            continue
        if not is_western_pronounceable(name, min_score=min_pronounce_score):
            continue
        seen.add(name)
        out.append(name)
    return out


def row_for_prefix(prefix: str, web_top: int) -> PrefixAuditRow:
    pronounce_score, pronounce_flags = pronounceability_score(prefix)
    exact_hits, near_hits, result_count, sample_domains, ok, source = ng.web_collision_signal(prefix, web_top)
    com_avail = ng.rdap_available(prefix, 'com')
    de_avail = ng.rdap_available(prefix, 'de')
    ch_avail = ng.rdap_available(prefix, 'ch')

    risk = 0.0
    if not ok:
        risk += 35.0
    else:
        risk += max(0, exact_hits) * 45.0
        risk += max(0, near_hits) * 14.0
        if result_count > 60:
            risk += min(12.0, (result_count - 60) * 0.12)
    if com_avail == 'no':
        risk += 10.0
    if de_avail == 'no':
        risk += 5.0
    if ch_avail == 'no':
        risk += 5.0

    return PrefixAuditRow(
        prefix=prefix,
        pronounce_score=pronounce_score,
        pronounce_flags=pronounce_flags,
        web_exact_hits=exact_hits,
        web_near_hits=near_hits,
        web_result_count=result_count,
        web_query_ok=bool(ok),
        web_source=source,
        web_sample_domains=sample_domains,
        com_available=com_avail,
        de_available=de_avail,
        ch_available=ch_avail,
        risk_score=round(risk, 2),
    )


def write_outputs(rows: list[PrefixAuditRow], csv_path: Path, json_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    payload = [asdict(item) for item in rows]
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

    fieldnames = list(payload[0].keys()) if payload else [
        'prefix', 'pronounce_score', 'pronounce_flags', 'web_exact_hits', 'web_near_hits', 'web_result_count',
        'web_query_ok', 'web_source', 'web_sample_domains', 'com_available', 'de_available', 'ch_available', 'risk_score',
    ]
    with csv_path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in payload:
            writer.writerow(row)


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)
    blocklist = load_blocklist(args.collision_blocklist_file)

    pool = generate_pool(
        rng=rng,
        n=max(args.count, args.pool_size),
        min_len=max(3, int(args.min_len)),
        max_len=max(int(args.min_len), int(args.max_len)),
        blocklist=blocklist,
        min_pronounce_score=max(0, min(100, int(args.min_pronounce_score))),
    )

    rows = [row_for_prefix(prefix, args.web_top) for prefix in pool]
    rows.sort(
        key=lambda r: (
            r.risk_score,
            r.web_exact_hits if r.web_exact_hits >= 0 else 99,
            r.web_near_hits if r.web_near_hits >= 0 else 99,
            -(r.pronounce_score),
            r.prefix,
        )
    )

    winners = rows[: max(1, int(args.count))]
    write_outputs(winners, Path(args.output_csv), Path(args.output_json))

    print(
        f'prefix_collision_audit total_checked={len(rows)} selected={len(winners)} '
        f'output_csv={args.output_csv} output_json={args.output_json}'
    )
    for row in winners[: max(1, int(args.print_top))]:
        print(
            f"- {row.prefix:10s} risk={row.risk_score:5.1f} "
            f"web={row.web_exact_hits}/{row.web_near_hits}/{row.web_result_count} "
            f"pron={row.pronounce_score:3d} rdap={row.com_available}/{row.de_available}/{row.ch_available}"
        )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
