#!/usr/bin/env python3
"""Automated legal + brand precheck research for candidate names.

This tool performs a lightweight, repeatable pre-legal sweep:
- trademark registry search signals (DPMA / Swissreg / TMview) via search-index probes
- app-store collision signals
- web collision signals
- domain availability signals

It is screening automation only and not legal advice.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable

# Keep import stable when script is launched from outside repo root.
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import name_generator as ng  # noqa: E402
from euipo_esearch_probe import EuipoProbe, EuipoProbeResult, build_euipo_url  # noqa: E402
from swissreg_ui_probe import SwissregProbeResult, SwissregUIProbe  # noqa: E402


@dataclass
class RegistrySignal:
    exact_hits: int
    near_hits: int
    result_count: int
    query_ok: bool
    source: str
    sample_domains: str


@dataclass
class CandidateResearch:
    name: str
    dpma_url: str
    swissreg_url: str
    swissreg_ui_url: str
    tmview_url: str
    euipo_url: str
    dpma_exact_hits: int
    dpma_near_hits: int
    dpma_result_count: int
    dpma_query_ok: bool
    dpma_source: str
    dpma_sample_domains: str
    swissreg_exact_hits: int
    swissreg_near_hits: int
    swissreg_result_count: int
    swissreg_query_ok: bool
    swissreg_source: str
    swissreg_sample_domains: str
    swissreg_ui_mark_count: int
    swissreg_ui_query_ok: bool
    swissreg_ui_source: str
    swissreg_ui_error: str
    tmview_exact_hits: int
    tmview_near_hits: int
    tmview_result_count: int
    tmview_query_ok: bool
    tmview_source: str
    tmview_sample_domains: str
    euipo_exact_hits: int
    euipo_near_hits: int
    euipo_result_count: int
    euipo_query_ok: bool
    euipo_source: str
    euipo_sample_text: str
    euipo_error: str
    registry_exact_total: int
    registry_near_total: int
    app_de_count: int
    app_de_exact: bool
    app_ch_count: int
    app_ch_exact: bool
    app_it_count: int
    app_it_exact: bool
    app_unknown_count: int
    web_exact_hits: int
    web_near_hits: int
    web_result_count: int
    web_sample_domains: str
    web_query_ok: bool
    web_source: str
    com_available: str
    de_available: str
    ch_available: str
    legal_status: str
    brand_status: str
    overall_status: str
    notes: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Automated legal + brand precheck research.')
    parser.add_argument('--names', default='', help='Comma-separated names.')
    parser.add_argument('--names-file', default='', help='Optional newline-delimited names file.')
    parser.add_argument(
        '--countries',
        default='de,ch,it',
        help='Comma list of App Store country codes for checks.',
    )
    parser.add_argument(
        '--registry-top-n',
        type=int,
        default=8,
        help='Top N search results to inspect per legal registry signal.',
    )
    parser.add_argument(
        '--web-top-n',
        type=int,
        default=8,
        help='Top N search results to inspect for generic web collision signal.',
    )
    parser.add_argument(
        '--output-csv',
        required=True,
        help='Output CSV path.',
    )
    parser.add_argument(
        '--output-json',
        default='',
        help='Optional JSON output path.',
    )
    parser.add_argument(
        '--print-top',
        type=int,
        default=12,
        help='Print top N rows by best overall_status and lower risk signals.',
    )
    parser.add_argument(
        '--euipo-probe',
        dest='euipo_probe',
        action='store_true',
        default=True,
        help='Enable EUIPO eSearch probe via Playwright (recommended for final shortlist).',
    )
    parser.add_argument(
        '--no-euipo-probe',
        dest='euipo_probe',
        action='store_false',
    )
    parser.add_argument(
        '--euipo-timeout-ms',
        type=int,
        default=20000,
        help='EUIPO browser navigation timeout per name (ms).',
    )
    parser.add_argument(
        '--euipo-settle-ms',
        type=int,
        default=2500,
        help='EUIPO post-load settle wait for client rendering (ms).',
    )
    parser.add_argument(
        '--euipo-headful',
        action='store_true',
        help='Run EUIPO probe with visible browser (debug only).',
    )
    parser.add_argument(
        '--swissreg-ui-probe',
        dest='swissreg_ui_probe',
        action='store_true',
        default=True,
        help='Enable Swissreg UI probe via Playwright to read Marken count.',
    )
    parser.add_argument(
        '--no-swissreg-ui-probe',
        dest='swissreg_ui_probe',
        action='store_false',
    )
    parser.add_argument(
        '--swissreg-timeout-ms',
        type=int,
        default=20000,
        help='Swissreg UI probe timeout per name (ms).',
    )
    parser.add_argument(
        '--swissreg-settle-ms',
        type=int,
        default=2500,
        help='Swissreg UI post-load settle wait (ms).',
    )
    parser.add_argument(
        '--swissreg-headful',
        action='store_true',
        help='Run Swissreg UI probe with visible browser (debug only).',
    )
    return parser.parse_args()


def parse_names(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or '').split(','):
        normalized = ng.normalize_alpha(token.strip())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def load_names_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    out: list[str] = []
    seen: set[str] = set()
    for line in path.read_text(encoding='utf-8').splitlines():
        normalized = ng.normalize_alpha(line.strip())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def merge_names(parts: Iterable[list[str]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for chunk in parts:
        for name in chunk:
            if name in seen:
                continue
            seen.add(name)
            out.append(name)
    return out


def _title_exact_or_near(name: str, raw_title: str) -> tuple[bool, bool]:
    title = re.sub(r'<[^>]+>', ' ', raw_title)
    title = title.strip().lower()
    if not title:
        return False, False
    title_norm = ng.normalize_alpha(title)
    if title_norm == name:
        return True, False
    if re.search(rf'(^|[^a-z0-9]){re.escape(name)}([^a-z0-9]|$)', title):
        return True, False
    tokens = set(re.findall(r'[a-z]{4,}', title))
    for token in tokens:
        if token == name:
            continue
        ratio = SequenceMatcher(None, token, name).ratio()
        if ratio >= 0.86 and abs(len(token) - len(name)) <= 2:
            return False, True
    return False, False


def probe_registry_signal(name: str, *, site_query: str, top_n: int) -> RegistrySignal:
    query = f'site:{site_query} "{name}"'
    matches, ok, source = ng.fetch_search_matches(query)
    if not ok:
        return RegistrySignal(
            exact_hits=-1,
            near_hits=-1,
            result_count=-1,
            query_ok=False,
            source='',
            sample_domains='',
        )

    exact = 0
    near = 0
    sample_domains: list[str] = []
    seen_domains: set[str] = set()
    limited = matches[: max(1, int(top_n))]
    for href, raw_title in limited:
        is_exact, is_near = _title_exact_or_near(name, raw_title)
        if is_exact:
            exact += 1
        elif is_near:
            near += 1
        domain = ng.extract_result_domain(href)
        if domain and domain not in seen_domains and len(sample_domains) < 4:
            seen_domains.add(domain)
            sample_domains.append(domain)

    return RegistrySignal(
        exact_hits=exact,
        near_hits=near,
        result_count=len(matches),
        query_ok=True,
        source=source,
        sample_domains=';'.join(sample_domains),
    )


def classify_status(
    *,
    registry_exact_total: int,
    registry_near_total: int,
    registry_query_all_ok: bool,
    app_exact_any: bool,
    app_unknown_count: int,
    web_exact_hits: int,
    web_near_hits: int,
    web_query_ok: bool,
    domain_triplet: tuple[str, str, str],
) -> tuple[str, str, str, str]:
    legal_status = 'clear'
    brand_status = 'clear'
    notes: list[str] = []

    if registry_exact_total > 0:
        legal_status = 'block'
        notes.append('registry_exact_hit')
    elif registry_near_total > 0:
        legal_status = 'review'
        notes.append('registry_near_hit')
    elif not registry_query_all_ok:
        legal_status = 'review'
        notes.append('registry_unknown')

    if app_exact_any:
        brand_status = 'block'
        notes.append('app_store_exact_hit')
    elif app_unknown_count > 0:
        brand_status = 'review'
        notes.append('app_store_unknown')
    elif web_exact_hits > 0:
        brand_status = 'review'
        notes.append('web_exact_hit')
    elif web_near_hits >= 2:
        brand_status = 'review'
        notes.append('web_near_hits>=2')
    elif not web_query_ok:
        brand_status = 'review'
        notes.append('web_unknown')

    if any(state != 'yes' for state in domain_triplet):
        if brand_status == 'clear':
            brand_status = 'review'
        notes.append('domain_not_all_yes')

    if legal_status == 'block' or brand_status == 'block':
        overall = 'block'
    elif legal_status == 'review' or brand_status == 'review':
        overall = 'review'
    else:
        overall = 'clear'

    return legal_status, brand_status, overall, ','.join(notes)


def run_research(
    *,
    names: list[str],
    countries: list[str],
    registry_top_n: int,
    web_top_n: int,
    euipo_probe: bool,
    euipo_timeout_ms: int,
    euipo_settle_ms: int,
    euipo_headful: bool,
    swissreg_ui_probe: bool,
    swissreg_timeout_ms: int,
    swissreg_settle_ms: int,
    swissreg_headful: bool,
) -> list[CandidateResearch]:
    rows: list[CandidateResearch] = []
    with EuipoProbe(
        timeout_ms=max(3000, int(euipo_timeout_ms)),
        settle_ms=max(0, int(euipo_settle_ms)),
        headless=not euipo_headful,
    ) as euipo_runner:
        with SwissregUIProbe(
            timeout_ms=max(3000, int(swissreg_timeout_ms)),
            settle_ms=max(0, int(swissreg_settle_ms)),
            headless=not swissreg_headful,
        ) as swissreg_runner:
            for name in names:
                dpma_url, swissreg_url, tmview_url = ng.trademark_search_urls(name)
                euipo_url = build_euipo_url(name)

                dpma = probe_registry_signal(name, site_query='register.dpma.de', top_n=registry_top_n)
                swissreg = probe_registry_signal(name, site_query='swissreg.ch', top_n=registry_top_n)
                tmview = probe_registry_signal(name, site_query='tmdn.org/tmview', top_n=registry_top_n)

                if euipo_probe:
                    euipo_result = euipo_runner.probe_name(name)
                else:
                    euipo_result = EuipoProbeResult(
                        name=name,
                        url=euipo_url,
                        query_ok=True,
                        source='disabled',
                        exact_hits=0,
                        near_hits=0,
                        result_count=0,
                        sample_text='',
                        error='',
                    )

                if swissreg_ui_probe:
                    swissreg_ui_result = swissreg_runner.probe_name(name)
                else:
                    swissreg_ui_result = SwissregProbeResult(
                        name=name,
                        url='https://www.swissreg.ch/database-client/home',
                        query_ok=True,
                        source='disabled',
                        mark_count=0,
                        error='',
                    )

                app_counts: dict[str, tuple[int, bool, bool]] = {}
                for country in countries:
                    app_counts[country] = ng.app_store_signal(name, country)
                app_unknown_count = sum(1 for (count, _exact, ok) in app_counts.values() if not ok or count < 0)
                app_exact_any = any(exact for (_count, exact, _ok) in app_counts.values())
                app_de_count, app_de_exact, _ = app_counts.get('de', (-1, False, False))
                app_ch_count, app_ch_exact, _ = app_counts.get('ch', (-1, False, False))
                app_it_count, app_it_exact, _ = app_counts.get('it', (-1, False, False))

                web_exact, web_near, web_total, web_sample, web_ok, web_source = ng.web_collision_signal(name, max(1, web_top_n))
                com = ng.rdap_available(name, 'com')
                de = ng.rdap_available(name, 'de')
                ch = ng.rdap_available(name, 'ch')

                tmview_exact_signal = max(max(0, tmview.exact_hits), max(0, euipo_result.exact_hits))
                swissreg_ui_near = 1 if swissreg_ui_result.mark_count > 0 else 0
                tmview_near_signal = max(max(0, tmview.near_hits), max(0, euipo_result.near_hits))
                registry_exact_total = (
                    max(0, dpma.exact_hits)
                    + max(0, swissreg.exact_hits)
                    + tmview_exact_signal
                )
                registry_near_total = (
                    max(0, dpma.near_hits)
                    + max(0, swissreg.near_hits)
                    + tmview_near_signal
                    + swissreg_ui_near
                )
                registry_query_all_ok = (
                    bool(dpma.query_ok)
                    and (bool(tmview.query_ok) or bool(euipo_result.query_ok))
                    and (bool(swissreg.query_ok) or bool(swissreg_ui_result.query_ok))
                )

                legal_status, brand_status, overall_status, notes = classify_status(
                    registry_exact_total=registry_exact_total,
                    registry_near_total=registry_near_total,
                    registry_query_all_ok=registry_query_all_ok,
                    app_exact_any=app_exact_any,
                    app_unknown_count=app_unknown_count,
                    web_exact_hits=max(0, web_exact),
                    web_near_hits=max(0, web_near),
                    web_query_ok=bool(web_ok),
                    domain_triplet=(com, de, ch),
                )

                rows.append(
                    CandidateResearch(
                        name=name,
                        dpma_url=dpma_url,
                        swissreg_url=swissreg_url,
                        swissreg_ui_url=swissreg_ui_result.url,
                        tmview_url=tmview_url,
                        euipo_url=euipo_result.url,
                        dpma_exact_hits=dpma.exact_hits,
                        dpma_near_hits=dpma.near_hits,
                        dpma_result_count=dpma.result_count,
                        dpma_query_ok=dpma.query_ok,
                        dpma_source=dpma.source,
                        dpma_sample_domains=dpma.sample_domains,
                        swissreg_exact_hits=swissreg.exact_hits,
                        swissreg_near_hits=swissreg.near_hits,
                        swissreg_result_count=swissreg.result_count,
                        swissreg_query_ok=swissreg.query_ok,
                        swissreg_source=swissreg.source,
                        swissreg_sample_domains=swissreg.sample_domains,
                        swissreg_ui_mark_count=swissreg_ui_result.mark_count,
                        swissreg_ui_query_ok=swissreg_ui_result.query_ok,
                        swissreg_ui_source=swissreg_ui_result.source,
                        swissreg_ui_error=swissreg_ui_result.error,
                        tmview_exact_hits=tmview.exact_hits,
                        tmview_near_hits=tmview.near_hits,
                        tmview_result_count=tmview.result_count,
                        tmview_query_ok=tmview.query_ok,
                        tmview_source=tmview.source,
                        tmview_sample_domains=tmview.sample_domains,
                        euipo_exact_hits=euipo_result.exact_hits,
                        euipo_near_hits=euipo_result.near_hits,
                        euipo_result_count=euipo_result.result_count,
                        euipo_query_ok=euipo_result.query_ok,
                        euipo_source=euipo_result.source,
                        euipo_sample_text=euipo_result.sample_text,
                        euipo_error=euipo_result.error,
                        registry_exact_total=registry_exact_total,
                        registry_near_total=registry_near_total,
                        app_de_count=app_de_count,
                        app_de_exact=bool(app_de_exact),
                        app_ch_count=app_ch_count,
                        app_ch_exact=bool(app_ch_exact),
                        app_it_count=app_it_count,
                        app_it_exact=bool(app_it_exact),
                        app_unknown_count=app_unknown_count,
                        web_exact_hits=web_exact,
                        web_near_hits=web_near,
                        web_result_count=web_total,
                        web_sample_domains=web_sample,
                        web_query_ok=bool(web_ok),
                        web_source=web_source,
                        com_available=com,
                        de_available=de,
                        ch_available=ch,
                        legal_status=legal_status,
                        brand_status=brand_status,
                        overall_status=overall_status,
                        notes=notes,
                    )
                )
    return rows


def write_csv(path: Path, rows: list[CandidateResearch]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = [asdict(r) for r in rows]
    if not data:
        path.write_text('', encoding='utf-8')
        return
    fieldnames = list(data[0].keys())
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def write_json(path: Path, rows: list[CandidateResearch]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(r) for r in rows]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def status_rank(value: str) -> int:
    token = str(value or '').strip().lower()
    if token == 'clear':
        return 2
    if token == 'review':
        return 1
    return 0


def print_summary(rows: list[CandidateResearch], top_n: int) -> None:
    if not rows:
        print('legal_brand_research no_rows')
        return

    sorted_rows = sorted(
        rows,
        key=lambda r: (
            -status_rank(r.overall_status),
            -status_rank(r.legal_status),
            -status_rank(r.brand_status),
            r.registry_exact_total,
            r.registry_near_total,
            max(0, r.web_exact_hits),
            max(0, r.web_near_hits),
            r.name,
        ),
    )

    clear_count = sum(1 for r in rows if r.overall_status == 'clear')
    review_count = sum(1 for r in rows if r.overall_status == 'review')
    block_count = sum(1 for r in rows if r.overall_status == 'block')
    print(
        'legal_brand_research_summary '
        f'total={len(rows)} clear={clear_count} review={review_count} block={block_count}'
    )
    print('Top candidates by precheck status:')
    for row in sorted_rows[: max(1, int(top_n))]:
        print(
            f'- {row.name:12s} overall={row.overall_status:6s} '
            f'legal={row.legal_status:6s} brand={row.brand_status:6s} '
            f'registry_exact={row.registry_exact_total} registry_near={row.registry_near_total} '
            f'web={row.web_exact_hits}/{row.web_near_hits} '
            f'domain={row.com_available}/{row.de_available}/{row.ch_available} '
            f'notes={row.notes or "-"}'
        )


def main() -> int:
    args = parse_args()
    names = merge_names(
        [
            parse_names(args.names),
            load_names_file(Path(args.names_file).expanduser()) if args.names_file else [],
        ]
    )
    if not names:
        print('No names provided. Use --names and/or --names-file.')
        return 2

    countries = [token.strip().lower() for token in str(args.countries or '').split(',') if re.fullmatch(r'[a-z]{2}', token.strip().lower())]
    if not countries:
        countries = ['de', 'ch', 'it']

    rows = run_research(
        names=names,
        countries=countries,
        registry_top_n=max(1, int(args.registry_top_n)),
        web_top_n=max(1, int(args.web_top_n)),
        euipo_probe=bool(args.euipo_probe),
        euipo_timeout_ms=max(3000, int(args.euipo_timeout_ms)),
        euipo_settle_ms=max(0, int(args.euipo_settle_ms)),
        euipo_headful=bool(args.euipo_headful),
        swissreg_ui_probe=bool(args.swissreg_ui_probe),
        swissreg_timeout_ms=max(3000, int(args.swissreg_timeout_ms)),
        swissreg_settle_ms=max(0, int(args.swissreg_settle_ms)),
        swissreg_headful=bool(args.swissreg_headful),
    )
    write_csv(Path(args.output_csv).expanduser(), rows)
    if args.output_json:
        write_json(Path(args.output_json).expanduser(), rows)
    print_summary(rows, top_n=max(1, int(args.print_top)))
    print(f'output_csv={Path(args.output_csv).expanduser()}')
    if args.output_json:
        print(f'output_json={Path(args.output_json).expanduser()}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
