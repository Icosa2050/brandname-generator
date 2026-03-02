#!/usr/bin/env python3
"""Deterministic post-ranker for brand names (DE/EN sayability rubric)."""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path


VOWELS = set('aeiouy')
HARD_CLUSTER_PATTERNS = (
    'qz',
    'xq',
    'zx',
    'jq',
    'qj',
    'tsch',
    'schl',
    'tzs',
)
SPELLING_AMBIGUOUS_PATTERNS = (
    'ph',
    'gh',
    'ck',
    'qu',
    'x',
    'yy',
    'aeo',
    'eoa',
)
GENERIC_STEMS = (
    'pay',
    'bill',
    'settle',
    'utility',
    'meter',
    'cost',
    'tariff',
    'charge',
    'split',
    'rent',
    'tenant',
    'ledger',
    'sync',
    'flow',
    'trust',
)
VERTICAL_LOCK_STEMS = (
    'bill',
    'pay',
    'settle',
    'meter',
    'rent',
    'tenant',
    'tariff',
    'charge',
)
NEGATIVE_SUBSTRINGS = (
    'arse',
    'ass',
    'shit',
    'fuk',
    'nazi',
    'hure',
    'arsch',
    'dumm',
    'krank',
)
LAZY_CATEGORY_STEMS = (
    'tenant',
    'settle',
    'rent',
    'lease',
    'bill',
    'pay',
    'meter',
    'tariff',
    'charge',
    'cost',
    'ledger',
    'utility',
    'split',
    'flow',
    'sync',
    'trust',
)
LAZY_SUFFIXES = {'lo', 'to', 'ta', 'no', 'ly', 'ify', 'io', 'co', 'ya'}

TRUE_VALUES = {'1', 'true', 'yes', 'y', 'x'}


@dataclass
class RubricScore:
    name: str
    raw_total_score: float
    total_score: float
    sayability_score: float
    spellability_score: float
    distinctiveness_score: float
    stretch_score: float
    negative_score: float
    recommendation: str
    reasons: list[str]
    source_shortlist_selected: str
    source_recommendation: str


@dataclass
class ScoreContext:
    total_names: int
    trigram_freq: dict[str, int]
    prefix2_freq: dict[str, int]
    suffix2_freq: dict[str, int]
    prefix3_freq: dict[str, int]
    suffix3_freq: dict[str, int]
    nearest_similarity: dict[str, float]


def normalize_name(raw: str) -> str:
    return re.sub(r'[^a-z]', '', str(raw or '').strip().lower())


def estimate_syllables(name: str) -> int:
    groups = re.findall(r'[aeiouy]+', name)
    return max(1, len(groups))


def max_consonant_run(name: str) -> int:
    longest = 0
    current = 0
    for ch in name:
        if ch in VOWELS:
            current = 0
            continue
        current += 1
        longest = max(longest, current)
    return longest


def bounded_score(raw: float) -> float:
    return max(0.0, min(20.0, round(raw, 2)))


def count_contains(name: str, patterns: tuple[str, ...]) -> int:
    return sum(1 for pattern in patterns if pattern and pattern in name)


def closeness(value: float, *, ideal: float, tolerance: float) -> float:
    if tolerance <= 0.0:
        return 1.0 if value == ideal else 0.0
    return max(0.0, 1.0 - (abs(value - ideal) / tolerance))


def percent(part: int, whole: int) -> float:
    if whole <= 0:
        return 0.0
    return float(part) / float(whole)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    idx = (len(sorted_vals) - 1) * (p / 100.0)
    lo = int(idx)
    hi = min(len(sorted_vals) - 1, lo + 1)
    if hi == lo:
        return float(sorted_vals[lo])
    frac = idx - lo
    return float(sorted_vals[lo] + ((sorted_vals[hi] - sorted_vals[lo]) * frac))


def trigrams(name: str) -> list[str]:
    if len(name) < 3:
        return []
    return [name[i : i + 3] for i in range(len(name) - 2)]


def is_lazy_category_suffix(name: str) -> bool:
    lower = name.lower().strip()
    for stem in LAZY_CATEGORY_STEMS:
        if not lower.startswith(stem):
            continue
        remainder = lower[len(stem) :]
        if not remainder:
            return True
        if remainder in LAZY_SUFFIXES:
            return True
        if len(remainder) <= 3:
            return True
    return False


def jaccard_similarity(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return float(len(a & b)) / float(len(a | b))


def build_score_context(names: list[str]) -> ScoreContext:
    trigram_counter: Counter[str] = Counter()
    prefix2_counter: Counter[str] = Counter()
    suffix2_counter: Counter[str] = Counter()
    prefix3_counter: Counter[str] = Counter()
    suffix3_counter: Counter[str] = Counter()
    trigram_sets: dict[str, set[str]] = {}

    for name in names:
        if not name:
            continue
        unique_trigrams = set(trigrams(name))
        trigram_sets[name] = unique_trigrams
        trigram_counter.update(unique_trigrams)
        if len(name) >= 2:
            prefix2_counter.update([name[:2]])
            suffix2_counter.update([name[-2:]])
        if len(name) >= 3:
            prefix3_counter.update([name[:3]])
            suffix3_counter.update([name[-3:]])

    nearest_similarity: dict[str, float] = {}
    for left in names:
        left_set = trigram_sets.get(left, set())
        max_sim = 0.0
        for right in names:
            if right == left:
                continue
            sim = jaccard_similarity(left_set, trigram_sets.get(right, set()))
            if sim > max_sim:
                max_sim = sim
        nearest_similarity[left] = max_sim

    return ScoreContext(
        total_names=max(1, len(names)),
        trigram_freq=dict(trigram_counter),
        prefix2_freq=dict(prefix2_counter),
        suffix2_freq=dict(suffix2_counter),
        prefix3_freq=dict(prefix3_counter),
        suffix3_freq=dict(suffix3_counter),
        nearest_similarity=nearest_similarity,
    )


def score_sayability(name: str) -> tuple[float, list[str]]:
    reasons: list[str] = []
    vowel_ratio = percent(sum(1 for ch in name if ch in VOWELS), len(name))
    syllables = estimate_syllables(name)
    cons_run = max_consonant_run(name)
    hard_hits = count_contains(name, HARD_CLUSTER_PATTERNS)

    ratio_component = closeness(vowel_ratio, ideal=0.42, tolerance=0.20)
    len_component = closeness(float(len(name)), ideal=8.0, tolerance=4.0)
    syll_component = closeness(float(syllables), ideal=3.0, tolerance=2.0)
    cons_component = max(0.0, 1.0 - max(0, cons_run - 2) * 0.18)

    if ratio_component < 0.65:
        reasons.append(f'sayability:vowel_ratio={vowel_ratio:.2f}')
    if len_component < 0.55:
        reasons.append(f'sayability:length={len(name)}')
    if syll_component < 0.55:
        reasons.append(f'sayability:syllables={syllables}')
    if cons_run >= 4:
        reasons.append(f'sayability:consonant_run={cons_run}')
    if hard_hits:
        reasons.append(f'sayability:hard_clusters={hard_hits}')

    raw = 20.0 * (
        (ratio_component * 0.35)
        + (len_component * 0.25)
        + (syll_component * 0.25)
        + (cons_component * 0.15)
    )
    raw -= min(4.0, hard_hits * 1.2)
    return bounded_score(raw), reasons


def score_spellability(name: str) -> tuple[float, list[str]]:
    reasons: list[str] = []
    ambiguous_hits = count_contains(name, SPELLING_AMBIGUOUS_PATTERNS)
    rare_letters = sum(1 for ch in name if ch in {'q', 'x', 'z', 'j'})
    doubled_letters = len(re.findall(r'(.)\1', name))

    len_component = closeness(float(len(name)), ideal=8.0, tolerance=5.0)
    ambiguity_component = max(0.0, 1.0 - min(0.9, ambiguous_hits * 0.12))
    rare_component = max(0.0, 1.0 - min(0.8, rare_letters * 0.16))
    repeat_component = max(0.0, 1.0 - min(0.6, doubled_letters * 0.12))

    if ambiguous_hits:
        reasons.append(f'spellability:ambiguous_patterns={ambiguous_hits}')
    if rare_letters >= 2:
        reasons.append(f'spellability:rare_letters={rare_letters}')
    if len(name) >= 12:
        reasons.append(f'spellability:length={len(name)}')
    if re.search(r'(.)\1\1', name):
        reasons.append('spellability:triple_letter')

    raw = 20.0 * (
        (len_component * 0.35)
        + (ambiguity_component * 0.35)
        + (rare_component * 0.20)
        + (repeat_component * 0.10)
    )
    return bounded_score(raw), reasons


def score_distinctiveness(name: str, *, context: ScoreContext) -> tuple[float, list[str]]:
    reasons: list[str] = []
    name_trigrams = trigrams(name)
    trigram_diversity = percent(len(set(name_trigrams)), len(name_trigrams)) if name_trigrams else 0.0

    if name_trigrams:
        corpus_commonness = statistics.fmean(
            percent(int(context.trigram_freq.get(tri, 0)), context.total_names)
            for tri in set(name_trigrams)
        )
    else:
        corpus_commonness = 1.0

    prefix = name[:2] if len(name) >= 2 else ''
    suffix = name[-2:] if len(name) >= 2 else ''
    prefix3 = name[:3] if len(name) >= 3 else ''
    suffix3 = name[-3:] if len(name) >= 3 else ''
    affix_commonness = max(
        percent(int(context.prefix2_freq.get(prefix, 0)), context.total_names) if prefix else 0.0,
        percent(int(context.suffix2_freq.get(suffix, 0)), context.total_names) if suffix else 0.0,
        percent(int(context.prefix3_freq.get(prefix3, 0)), context.total_names) if prefix3 else 0.0,
        percent(int(context.suffix3_freq.get(suffix3, 0)), context.total_names) if suffix3 else 0.0,
    )
    nearest_similarity = float(context.nearest_similarity.get(name, 0.0))

    generic_hits = count_contains(name, GENERIC_STEMS)

    if generic_hits:
        reasons.append(f'distinctiveness:generic_stems={generic_hits}')
    if corpus_commonness > 0.40:
        reasons.append(f'distinctiveness:common_ngrams={corpus_commonness:.2f}')
    if affix_commonness > 0.35:
        reasons.append(f'distinctiveness:common_affix={affix_commonness:.2f}')
    if nearest_similarity > 0.45:
        reasons.append(f'distinctiveness:near_duplicate={nearest_similarity:.2f}')

    raw = 20.0 * (
        (trigram_diversity * 0.38)
        + ((1.0 - corpus_commonness) * 0.34)
        + ((1.0 - affix_commonness) * 0.28)
    )
    raw -= min(8.0, generic_hits * 2.2)
    raw -= min(7.0, nearest_similarity * 7.0)
    return bounded_score(raw), reasons


def score_stretch(name: str) -> tuple[float, list[str]]:
    reasons: list[str] = []
    vertical_hits = count_contains(name, VERTICAL_LOCK_STEMS)
    length_component = closeness(float(len(name)), ideal=8.0, tolerance=5.0)

    if vertical_hits:
        reasons.append(f'stretch:vertical_lock={vertical_hits}')
    if re.search(r'(pay|bill|rent|cost)(ify|ly|io|ora|sy|ix)$', name):
        reasons.append('stretch:category_suffix_pattern')
    if is_lazy_category_suffix(name):
        reasons.append('stretch:lazy_category_suffix')

    raw = 20.0 * length_component
    raw -= min(10.0, vertical_hits * 2.8)
    if any(reason == 'stretch:category_suffix_pattern' for reason in reasons):
        raw -= 2.5
    if any(reason == 'stretch:lazy_category_suffix' for reason in reasons):
        raw -= 8.0
    return bounded_score(raw), reasons


def score_negative_checks(name: str) -> tuple[float, list[str]]:
    reasons: list[str] = []
    hits = count_contains(name, NEGATIVE_SUBSTRINGS)
    harsh_repeats = 1 if re.search(r'(xx|zz|qq)', name) else 0
    harsh_cluster = 1 if max_consonant_run(name) >= 5 else 0

    if hits:
        reasons.append(f'negative:lexical_hits={hits}')
    if harsh_repeats:
        reasons.append('negative:awkward_repeat')
    if harsh_cluster:
        reasons.append('negative:harsh_phonetics')

    raw = 20.0
    raw -= min(14.0, hits * 7.0)
    raw -= harsh_repeats * 2.0
    raw -= harsh_cluster * 2.5
    return bounded_score(raw), reasons


def recommend_from_score(total_score: float, negative_score: float) -> str:
    if negative_score < 8.0:
        return 'drop'
    if total_score >= 86.0:
        return 'strong'
    if total_score >= 74.0:
        return 'consider'
    if total_score >= 62.0:
        return 'maybe'
    return 'drop'


def score_name(
    name: str,
    *,
    source_shortlist_selected: str,
    source_recommendation: str,
    context: ScoreContext | None = None,
) -> RubricScore:
    score_context = context or build_score_context([name])
    sayability, sayability_reasons = score_sayability(name)
    spellability, spellability_reasons = score_spellability(name)
    distinctiveness, distinctiveness_reasons = score_distinctiveness(name, context=score_context)
    stretch, stretch_reasons = score_stretch(name)
    negative, negative_reasons = score_negative_checks(name)

    weighted_total = (
        (sayability * 0.29)
        + (spellability * 0.23)
        + (distinctiveness * 0.24)
        + (stretch * 0.16)
        + (negative * 0.08)
    )
    total_score = round(weighted_total * 5.0, 2)
    reasons = sayability_reasons + spellability_reasons + distinctiveness_reasons + stretch_reasons + negative_reasons
    recommendation = recommend_from_score(total_score=total_score, negative_score=negative)
    return RubricScore(
        name=name,
        raw_total_score=total_score,
        total_score=total_score,
        sayability_score=sayability,
        spellability_score=spellability,
        distinctiveness_score=distinctiveness,
        stretch_score=stretch,
        negative_score=negative,
        recommendation=recommendation,
        reasons=reasons,
        source_shortlist_selected=source_shortlist_selected,
        source_recommendation=source_recommendation,
    )


def latest_run_csv(out_dir: Path) -> Path:
    run_dir = out_dir / 'runs'
    run_files = sorted(run_dir.glob('run_*.csv'))
    if not run_files:
        raise ValueError(f'run_csv_not_found:{run_dir}')
    return run_files[-1]


def should_keep_row(row: dict[str, str], include_non_shortlist: bool) -> bool:
    if include_non_shortlist:
        return True
    return str(row.get('shortlist_selected') or '').strip().lower() in TRUE_VALUES


def load_names(path: Path, *, include_non_shortlist: bool) -> list[tuple[str, str, str]]:
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        names: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for row in reader:
            raw_name = str(row.get('name') or '').strip()
            name = normalize_name(raw_name)
            if not name or name in seen:
                continue
            if not should_keep_row(row, include_non_shortlist):
                continue
            seen.add(name)
            names.append(
                (
                    name,
                    str(row.get('shortlist_selected') or ''),
                    str(row.get('recommendation') or ''),
                )
            )
    return names


def write_ranked_csv(path: Path, rows: list[RubricScore]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        headers = [
            'rank',
            'name',
            'raw_total_score',
            'total_score',
            'sayability_score',
            'spellability_score',
            'distinctiveness_score',
            'stretch_score',
            'negative_score',
            'recommendation',
            'reasons',
            'source_shortlist_selected',
            'source_recommendation',
        ]
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for idx, item in enumerate(rows, start=1):
            writer.writerow(
                {
                    'rank': idx,
                    'name': item.name,
                    'raw_total_score': f'{item.raw_total_score:.2f}',
                    'total_score': f'{item.total_score:.2f}',
                    'sayability_score': f'{item.sayability_score:.2f}',
                    'spellability_score': f'{item.spellability_score:.2f}',
                    'distinctiveness_score': f'{item.distinctiveness_score:.2f}',
                    'stretch_score': f'{item.stretch_score:.2f}',
                    'negative_score': f'{item.negative_score:.2f}',
                    'recommendation': item.recommendation,
                    'reasons': ';'.join(item.reasons[:12]),
                    'source_shortlist_selected': item.source_shortlist_selected,
                    'source_recommendation': item.source_recommendation,
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Deterministically rerank shortlist names with DE/EN pronounceability rubric.')
    parser.add_argument('--out-dir', default='', help='Campaign output dir (used to locate latest run csv).')
    parser.add_argument('--input-csv', default='', help='Explicit run csv path (overrides --out-dir lookup).')
    parser.add_argument('--output-csv', default='', help='Output ranked csv path.')
    parser.add_argument('--output-json', default='', help='Output summary json path.')
    parser.add_argument('--top-n', type=int, default=40, help='Top N names to keep in output.')
    parser.add_argument('--include-non-shortlist', action='store_true', help='Score all names from run csv, not just shortlist-selected names.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.input_csv:
        input_csv = Path(args.input_csv).expanduser().resolve()
    else:
        out_dir_raw = str(args.out_dir or '').strip()
        if not out_dir_raw:
            raise SystemExit('Provide --input-csv or --out-dir.')
        input_csv = latest_run_csv(Path(out_dir_raw).expanduser().resolve())

    if not input_csv.exists():
        raise SystemExit(f'input_csv_not_found:{input_csv}')

    rows = load_names(input_csv, include_non_shortlist=bool(args.include_non_shortlist))
    if not rows:
        raise SystemExit(f'no_names_to_score:{input_csv}')

    score_context = build_score_context([name for name, _sel, _rec in rows])
    scored = [
        score_name(
            name=name,
            source_shortlist_selected=shortlist_selected,
            source_recommendation=source_recommendation,
            context=score_context,
        )
        for name, shortlist_selected, source_recommendation in rows
    ]
    scored = sorted(
        scored,
        key=lambda item: (item.raw_total_score, item.negative_score, item.distinctiveness_score, item.name),
        reverse=True,
    )
    all_scored_count = len(scored)
    for idx, item in enumerate(scored):
        percentile_rank = 100.0 if all_scored_count <= 1 else (100.0 * float(all_scored_count - 1 - idx) / float(all_scored_count - 1))
        calibrated_total = round((item.raw_total_score * 0.62) + (percentile_rank * 0.38), 2)
        item.total_score = calibrated_total
        item.recommendation = recommend_from_score(total_score=calibrated_total, negative_score=item.negative_score)

    scored = sorted(
        scored,
        key=lambda item: (item.total_score, item.raw_total_score, item.negative_score, item.distinctiveness_score, item.name),
        reverse=True,
    )
    all_score_values = [item.total_score for item in scored]
    top_n = max(1, int(args.top_n))
    scored = scored[:top_n]

    base_out_dir = input_csv.parents[1] if input_csv.parent.name == 'runs' else input_csv.parent
    output_csv = (
        Path(args.output_csv).expanduser().resolve()
        if args.output_csv
        else base_out_dir / 'postrank' / 'deterministic_rubric_rank.csv'
    )
    output_json = (
        Path(args.output_json).expanduser().resolve()
        if args.output_json
        else base_out_dir / 'postrank' / 'deterministic_rubric_summary.json'
    )

    write_ranked_csv(output_csv, scored)

    p25 = percentile(all_score_values, 25.0)
    p75 = percentile(all_score_values, 75.0)
    iqr = round(p75 - p25, 2)
    score_ceiling_share = round(percent(sum(1 for value in all_score_values if value >= 99.5), len(all_score_values)), 4)

    summary = {
        'input_csv': str(input_csv),
        'output_csv': str(output_csv),
        'top_n': len(scored),
        'name_count_scored': len(rows),
        'mean_total_score': round(statistics.fmean(all_score_values), 2),
        'median_total_score': round(statistics.median(all_score_values), 2),
        'min_total_score': round(min(all_score_values), 2),
        'max_total_score': round(max(all_score_values), 2),
        'iqr_total_score': iqr,
        'score_ceiling_share': score_ceiling_share,
        'discrimination_warning': bool(iqr < 10.0 or score_ceiling_share > 0.20),
        'recommendation_counts': {
            label: sum(1 for item in scored if item.recommendation == label)
            for label in ('strong', 'consider', 'maybe', 'drop')
        },
        'top_names': [asdict(item) for item in scored[:10]],
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

    print(
        f'postrank_complete input={input_csv} output_csv={output_csv} output_json={output_json} '
        f'top_n={len(scored)} iqr={iqr:.2f} ceiling_share={score_ceiling_share:.2f}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
