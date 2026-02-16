#!/usr/bin/env python3
"""Generate and screen brand-name candidates for the app.

Pipeline:
1) Generate broad candidate pool (coined + suggestive + optional seeds)
2) Score each candidate for brand quality and challenge risk
3) Run external checks (App Store and RDAP domain availability)
4) Export ranked CSV and print best candidates

This is a practical pre-screening tool, not legal advice.
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import html
import itertools
import json
import re
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable
from urllib import error, parse, request

PROTECTED_MARKS = [
    'immoscout24',
    'immoscout',
    'immonet',
    'immowelt',
    'immocloud',
    'immoware24',
    'objego',
    'scalara',
    'wohnify',
    'hausify',
    'kostal',
    'costal',
    'saldeo',
    'saldio',
    'utilaro',
]

GENERIC_TOKENS = {
    'immo',
    'haus',
    'miet',
    'wohn',
    'neben',
    'umlage',
    'kosten',
    'abrechnung',
    'saldo',
    'property',
    'rent',
    'utility',
}

GERMAN_HEAVY_TOKENS = {
    'neben',
    'umlage',
    'miet',
    'wohn',
    'haus',
    'abrechn',
    'kosten',
    'betrieb',
}

COINED_PREFIXES = [
    'util',
    'sald',
    'nov',
    'vera',
    'luma',
    'vanta',
    'nexa',
    'cora',
    'tiva',
    'solva',
    'domi',
    'resi',
    'folio',
    'mora',
    'alto',
    'sora',
    'urba',
    'vala',
    'mera',
    'fina',
]

BRAND_STEMS = [
    'utila',
    'saldi',
    'doma',
    'reli',
    'talo',
    'niva',
    'vero',
    'lumo',
    'zeno',
    'cava',
    'mora',
    'voro',
    'keli',
    'soli',
    'rivo',
]

BRAND_ENDINGS = ['ro', 'ra', 'rio', 'via', 'va', 'la', 'lo', 'na', 'no', 'ta', 'to']

COINED_SUFFIXES = [
    'ro',
    'rio',
    'ra',
    'ria',
    'via',
    'va',
    'za',
    'lo',
    'lio',
    'neo',
    'no',
    'xa',
    'xo',
    'aro',
    'ivo',
]

SUGGESTIVE_ROOTS_DACH = [
    'saldo',
    'klar',
    'immo',
    'haus',
    'miet',
    'wohn',
    'objekt',
    'neben',
    'umlage',
]

SUGGESTIVE_ROOTS_GLOBAL = [
    'saldo',
    'rento',
    'casa',
    'domus',
    'terra',
    'folio',
    'ledger',
    'nesta',
]

SHORT_SUFFIXES = ['on', 'io', 'ra', 'ro', 'ly', 'eo', 'ex', 'a', 'o']

USER_AGENT = 'kostula-name-generator/1.0'


@dataclass
class Candidate:
    name: str
    quality_score: int
    challenge_risk: int
    total_score: int
    descriptive_risk: int
    similarity_risk: int
    closest_mark: str
    scope_penalty: int
    store_de_count: int = -1
    store_de_exact: bool = False
    store_ch_count: int = -1
    store_ch_exact: bool = False
    store_us_count: int = -1
    store_us_exact: bool = False
    store_exact_countries: str = ''
    store_unknown_countries: str = ''
    com_available: str = 'unknown'
    com_fallback_available: str = 'unknown'
    com_fallback_domain: str = ''
    de_available: str = 'unknown'
    ch_available: str = 'unknown'
    web_result_count: int = -1
    web_exact_hits: int = -1
    web_near_hits: int = -1
    web_sample_domains: str = ''
    web_source: str = ''
    external_penalty: int = 0
    hard_fail: bool = False
    fail_reason: str = ''


def normalize_alpha(text: str) -> str:
    return re.sub(r'[^a-z]+', '', text.lower())


def generate_candidates(scope: str, seeds: Iterable[str], min_len: int, max_len: int) -> list[str]:
    names: set[str] = set()

    for p, s in itertools.product(COINED_PREFIXES, COINED_SUFFIXES):
        names.add(f'{p}{s}')

    for stem, end in itertools.product(BRAND_STEMS, BRAND_ENDINGS):
        names.add(f'{stem}{end}')

    roots = SUGGESTIVE_ROOTS_DACH if scope == 'dach' else SUGGESTIVE_ROOTS_GLOBAL
    for root, suf in itertools.product(roots, SHORT_SUFFIXES):
        names.add(f'{root}{suf}')

    for seed in seeds:
        base = normalize_alpha(seed)
        if not base:
            continue
        names.add(base)
        for suf in SHORT_SUFFIXES:
            names.add(f'{base}{suf}')
        for end in BRAND_ENDINGS:
            names.add(f'{base[:6]}{end}')
        for p in COINED_PREFIXES[:8]:
            names.add(f'{p}{base[:3]}')

    cleaned = []
    for raw in names:
        n = normalize_alpha(raw)
        if not n:
            continue
        if len(n) < min_len or len(n) > max_len:
            continue
        if not re.fullmatch(r'[a-z]+', n):
            continue
        if len(n) > 2 and n[0] == n[1] == n[2]:
            continue
        cleaned.append(n)

    return sorted(set(cleaned))


def vowel_ratio(name: str) -> float:
    vowels = sum(1 for c in name if c in 'aeiouy')
    return vowels / max(1, len(name))


def length_score(name: str) -> int:
    diff = abs(len(name) - 8)
    return max(0, 35 - diff * 5)


def pronounceability_score(name: str) -> int:
    score = 35
    ratio = vowel_ratio(name)
    score -= int(min(18, abs(ratio - 0.46) * 70))
    for cluster in ('tsch', 'schr', 'xtr', 'ptk', 'qz', 'yy', 'iii'):
        if cluster in name:
            score -= 5
    if re.search(r'(.)\1\1', name):
        score -= 5
    return max(0, score)


def memorability_score(name: str) -> int:
    score = 30
    unique = len(set(name))
    if unique < 4:
        score -= 10
    if len(name) <= 6:
        score += 2
    if len(name) >= 11:
        score -= 8
    for seq in ('aiv', 'pax', 'paz', 'xo', 'xx', 'qz', 'zz', 'ass', 'cle', 'hom'):
        if seq in name:
            score -= 6
    if name.endswith(('uti', 'ass', 'cle', 'hom', 'fol')):
        score -= 6
    return max(0, min(30, score))


def quality_score(name: str) -> int:
    return max(0, min(100, length_score(name) + pronounceability_score(name) + memorability_score(name)))


def max_similarity(name: str) -> tuple[float, str]:
    best = 0.0
    closest = ''
    for mark in PROTECTED_MARKS:
        ratio = SequenceMatcher(None, name, mark).ratio()
        if name[:4] == mark[:4]:
            ratio = max(ratio, 0.82)
        if name[:5] == mark[:5]:
            ratio = max(ratio, 0.88)
        if ratio > best:
            best = ratio
            closest = mark
    return best, closest


def descriptive_risk(name: str) -> int:
    risk = 0
    for token in GENERIC_TOKENS:
        if token in name:
            risk += 18
    if any(t in name for t in ('umlage', 'neben', 'kosten', 'abrechn')):
        risk += 12
    return min(100, risk)


def scope_penalty(name: str, scope: str) -> int:
    if scope == 'dach':
        return 0
    penalty = 0
    for token in GERMAN_HEAVY_TOKENS:
        if token in name:
            penalty += 12
    if scope == 'global':
        penalty += 6 * sum(1 for t in ('immo', 'miet', 'wohn') if t in name)
    return min(60, penalty)


def challenge_risk(name: str, scope: str) -> tuple[int, int, str, int, int]:
    sim, closest = max_similarity(name)
    sim_risk = int(sim * 100)
    desc_risk = descriptive_risk(name)
    sc_pen = scope_penalty(name, scope)
    risk = int(min(100, 0.55 * sim_risk + 0.3 * desc_risk + 0.15 * sc_pen))
    return risk, sim_risk, closest, desc_risk, sc_pen


def fetch_json(url: str, timeout: float = 8.0, retries: int = 2) -> dict | None:
    req = request.Request(url, headers={'User-Agent': USER_AGENT})
    for i in range(retries + 1):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode('utf-8', errors='replace')
                return json.loads(data)
        except Exception:
            if i == retries:
                return None
            time.sleep(0.4 * (i + 1))
    return None


def fetch_text(url: str, timeout: float = 8.0, retries: int = 2) -> str | None:
    req = request.Request(url, headers={'User-Agent': USER_AGENT})
    for i in range(retries + 1):
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode('utf-8', errors='replace')
        except Exception:
            if i == retries:
                return None
            time.sleep(0.4 * (i + 1))
    return None


def app_store_signal(name: str, country: str) -> tuple[int, bool, bool]:
    url = (
        'https://itunes.apple.com/search?'
        + parse.urlencode({'term': name, 'entity': 'software', 'country': country, 'limit': 8})
    )
    data = fetch_json(url)
    if not data:
        return -1, False, False

    count = int(data.get('resultCount', 0))
    exact = False
    for item in data.get('results', []):
        track = normalize_alpha(str(item.get('trackName', '')))
        if track == name:
            exact = True
            break
    return count, exact, True


def rdap_available(name: str, tld: str) -> str:
    endpoints = {
        'com': f'https://rdap.verisign.com/com/v1/domain/{name}.com',
        'de': f'https://rdap.denic.de/domain/{name}.de',
        'ch': f'https://rdap.nic.ch/domain/{name}.ch',
    }
    req = request.Request(endpoints[tld], headers={'User-Agent': USER_AGENT})
    try:
        with request.urlopen(req, timeout=8.0) as resp:
            _ = resp.read(64)
            if resp.status == 200:
                return 'no'
            status = 'unknown'
    except error.HTTPError as e:
        if e.code == 404:
            return 'yes'
        status = 'unknown'
    except Exception:
        status = 'unknown'

    # Secondary lookup to reduce false "unknown" responses from registry-specific RDAP endpoints.
    fallback = rdap_available_fqdn(f'{name}.{tld}')
    if fallback in {'yes', 'no'}:
        return fallback
    return status


def rdap_available_fqdn(fqdn: str) -> str:
    req = request.Request(f'https://rdap.org/domain/{fqdn}', headers={'User-Agent': USER_AGENT})
    try:
        with request.urlopen(req, timeout=8.0) as resp:
            _ = resp.read(64)
            if resp.status == 200:
                return 'no'
            return 'unknown'
    except error.HTTPError as e:
        if e.code == 404:
            return 'yes'
        return 'unknown'
    except Exception:
        return 'unknown'


def best_com_fallback(name: str) -> tuple[str, str]:
    candidates = [
        f'get{name}.com',
        f'use{name}.com',
        f'{name}app.com',
        f'{name}hq.com',
        f'{name}cloud.com',
    ]
    for fqdn in candidates:
        avail = rdap_available_fqdn(fqdn)
        if avail == 'yes':
            return 'yes', fqdn
    return 'no', ''


def extract_result_domain(raw_href: str) -> str:
    href = html.unescape(raw_href)
    if href.startswith('//'):
        href = f'https:{href}'
    if 'duckduckgo.com/l/?' in href:
        try:
            parsed = parse.urlparse(href)
            params = parse.parse_qs(parsed.query)
            target = params.get('uddg', [''])[0]
            if target:
                href = parse.unquote(target)
        except Exception:
            return ''
    if 'bing.com/ck/a' in href:
        try:
            parsed = parse.urlparse(href)
            params = parse.parse_qs(parsed.query)
            target = params.get('u', [''])[0]
            if target.startswith('a1'):
                encoded = target[2:]
                encoded += '=' * ((4 - len(encoded) % 4) % 4)
                href = base64.urlsafe_b64decode(encoded.encode('ascii')).decode('utf-8', errors='replace')
        except Exception:
            pass
    try:
        parsed = parse.urlparse(href if '://' in href else f'https://{href}')
    except Exception:
        return ''
    domain = parsed.netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain


def parse_ddg_results(page: str) -> list[tuple[str, str]]:
    return re.findall(
        r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        page,
        re.IGNORECASE | re.DOTALL,
    )


def parse_bing_results(page: str) -> list[tuple[str, str]]:
    return re.findall(
        r'<li[^>]*class="b_algo"[^>]*>.*?<h2[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        page,
        re.IGNORECASE | re.DOTALL,
    )


def fetch_search_matches(query: str) -> tuple[list[tuple[str, str]], bool, str]:
    source = 'ddg'
    url = 'https://duckduckgo.com/html/?' + parse.urlencode({'q': query})
    page = fetch_text(url, timeout=12.0, retries=2)
    if page is not None:
        return parse_ddg_results(page), True, source
    source = 'bing'
    url = 'https://www.bing.com/search?' + parse.urlencode({'q': query})
    page = fetch_text(url, timeout=12.0, retries=2)
    if page is None:
        return [], False, ''
    return parse_bing_results(page), True, source


def web_collision_signal(name: str, top_n: int) -> tuple[int, int, int, str, bool, str]:
    quoted_matches, quoted_ok, quoted_source = fetch_search_matches(f'"{name}"')
    plain_matches, plain_ok, plain_source = fetch_search_matches(name)

    if not quoted_ok and not plain_ok:
        return -1, -1, -1, '', False, ''

    if quoted_ok and plain_ok:
        source = f'{quoted_source}+{plain_source}'
    elif quoted_ok:
        source = quoted_source
    else:
        source = plain_source

    exact_hits = 0
    near_hits = 0
    sample_domains: list[str] = []
    seen_domains: set[str] = set()

    quoted_slice = quoted_matches[:top_n]
    plain_slice = plain_matches[:top_n]

    for href, raw_title in quoted_slice + plain_slice:
        title = html.unescape(re.sub(r'<[^>]+>', ' ', raw_title))
        title_lc = title.lower()
        title_norm = normalize_alpha(title)
        if title_norm == name or re.search(rf'(^|[^a-z0-9]){re.escape(name)}([^a-z0-9]|$)', title_lc):
            exact_hits += 1

    for href, raw_title in plain_slice:
        title = html.unescape(re.sub(r'<[^>]+>', ' ', raw_title))
        title_lc = title.lower()
        tokens = set(re.findall(r'[a-z]{4,}', title_lc))
        for token in tokens:
            if token == name:
                continue
            ratio = SequenceMatcher(None, token, name).ratio()
            if ratio >= 0.86 and abs(len(token) - len(name)) <= 2:
                near_hits += 1
                break

        domain = extract_result_domain(href)
        if domain and domain not in seen_domains and len(sample_domains) < 4:
            sample_domains.append(domain)
            seen_domains.add(domain)

    total_results = len(quoted_matches) + len(plain_matches)
    return exact_hits, near_hits, total_results, ';'.join(sample_domains), True, source


def required_tlds(scope: str) -> list[str]:
    if scope == 'dach':
        return ['de', 'ch']
    if scope == 'eu':
        return ['de', 'ch', 'com']
    return ['com']


def evaluate_candidates(scope: str, names: list[str], similarity_fail_threshold: int) -> list[Candidate]:
    results: list[Candidate] = []
    for n in names:
        q = quality_score(n)
        risk, sim_risk, closest, desc_risk, sc_pen = challenge_risk(n, scope)
        total = max(0, min(100, int(q - (risk * 0.55))))
        c = Candidate(
            name=n,
            quality_score=q,
            challenge_risk=risk,
            total_score=total,
            descriptive_risk=desc_risk,
            similarity_risk=sim_risk,
            closest_mark=closest,
            scope_penalty=sc_pen,
        )
        if sim_risk >= similarity_fail_threshold:
            c.hard_fail = True
            c.fail_reason = f'similar_to_{closest}'
        results.append(c)
    return results


def mark_fail(c: Candidate, reason: str) -> None:
    c.hard_fail = True
    if not c.fail_reason:
        c.fail_reason = reason


def apply_external_penalty(c: Candidate) -> None:
    penalty = 0
    if c.web_exact_hits > 0:
        penalty += min(30, c.web_exact_hits * 12)
    if c.web_near_hits > 0:
        penalty += min(14, c.web_near_hits * 4)
    unknown_store_count = len([p for p in c.store_unknown_countries.split(',') if p.strip()])
    if unknown_store_count > 0:
        penalty += min(8, unknown_store_count * 2)
    c.external_penalty = penalty
    c.challenge_risk = min(100, c.challenge_risk + penalty)
    c.total_score = max(0, min(100, c.total_score - int(0.45 * penalty)))


def run_external_checks(
    candidates: list[Candidate],
    scope: str,
    throttle_ms: int,
    gate: str,
    store_countries: list[str],
    web_check: bool,
    web_top: int,
    require_base_com: bool,
    fail_on_unknown: bool,
) -> None:
    req_tlds = required_tlds(scope)
    for c in candidates:
        exact_countries: list[str] = []
        unknown_countries: list[str] = []
        for country in store_countries:
            count, exact, ok = app_store_signal(c.name, country)
            if country == 'de':
                c.store_de_count, c.store_de_exact = count, exact
            elif country == 'ch':
                c.store_ch_count, c.store_ch_exact = count, exact
            elif country == 'us':
                c.store_us_count, c.store_us_exact = count, exact
            if not ok:
                unknown_countries.append(country)
            if exact:
                exact_countries.append(country)
        c.store_exact_countries = ','.join(exact_countries)
        c.store_unknown_countries = ','.join(unknown_countries)

        c.com_available = rdap_available(c.name, 'com')
        c.de_available = rdap_available(c.name, 'de')
        c.ch_available = rdap_available(c.name, 'ch')
        c.com_fallback_available, c.com_fallback_domain = best_com_fallback(c.name)

        if web_check:
            (
                c.web_exact_hits,
                c.web_near_hits,
                c.web_result_count,
                c.web_sample_domains,
                web_ok,
                c.web_source,
            ) = web_collision_signal(c.name, top_n=web_top)
        else:
            c.web_exact_hits, c.web_near_hits, c.web_result_count, c.web_sample_domains, web_ok, c.web_source = (
                0,
                0,
                0,
                '',
                True,
                'disabled',
            )

        apply_external_penalty(c)

        if exact_countries:
            mark_fail(c, f'exact_app_store_collision_{"-".join(exact_countries)}')

        if web_check and c.web_exact_hits > 0:
            mark_fail(c, 'web_exact_collision')

        if gate == 'strict' and web_check and c.web_near_hits >= 2:
            mark_fail(c, 'web_near_collision')

        for tld in req_tlds:
            avail = {'com': c.com_available, 'de': c.de_available, 'ch': c.ch_available}[tld]
            # If .com is taken, allow viable fallback domain for global/eu naming exploration.
            if tld == 'com' and not require_base_com and avail != 'yes' and c.com_fallback_available == 'yes':
                continue
            if fail_on_unknown and avail == 'unknown':
                mark_fail(c, f'required_domain_{tld}_unknown')
                break
            if avail != 'yes':
                mark_fail(c, f'required_domain_{tld}_not_available')
                break

        if require_base_com and c.com_available != 'yes':
            mark_fail(c, 'base_com_not_available')

        if fail_on_unknown and c.store_unknown_countries:
            mark_fail(c, 'app_store_check_unknown')

        if fail_on_unknown and web_check and not web_ok:
            mark_fail(c, 'web_check_unknown')

        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)


def recommendation(c: Candidate, gate: str) -> str:
    if c.hard_fail:
        return 'reject'
    if gate == 'strict':
        if c.challenge_risk <= 24 and c.total_score >= 66:
            return 'strong'
        if c.challenge_risk <= 34 and c.total_score >= 56:
            return 'consider'
        return 'weak'
    if c.challenge_risk <= 32 and c.total_score >= 62:
        return 'strong'
    if c.challenge_risk <= 45 and c.total_score >= 52:
        return 'consider'
    return 'weak'


def write_csv(path: Path, scope: str, candidates: list[Candidate], gate: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(
            [
                'name',
                'scope',
                'gate',
                'quality_score',
                'challenge_risk',
                'total_score',
                'descriptive_risk',
                'similarity_risk',
                'closest_mark',
                'scope_penalty',
                'itunes_de_count',
                'itunes_de_exact',
                'itunes_ch_count',
                'itunes_ch_exact',
                'itunes_us_count',
                'itunes_us_exact',
                'itunes_exact_countries',
                'itunes_unknown_countries',
                'domain_com_available',
                'domain_com_fallback_available',
                'domain_com_fallback_domain',
                'domain_de_available',
                'domain_ch_available',
                'web_result_count',
                'web_exact_hits',
                'web_near_hits',
                'web_sample_domains',
                'web_source',
                'external_penalty',
                'hard_fail',
                'fail_reason',
                'recommendation',
            ]
        )
        for c in candidates:
            w.writerow(
                [
                    c.name,
                    scope,
                    gate,
                    c.quality_score,
                    c.challenge_risk,
                    c.total_score,
                    c.descriptive_risk,
                    c.similarity_risk,
                    c.closest_mark,
                    c.scope_penalty,
                    c.store_de_count,
                    c.store_de_exact,
                    c.store_ch_count,
                    c.store_ch_exact,
                    c.store_us_count,
                    c.store_us_exact,
                    c.store_exact_countries,
                    c.store_unknown_countries,
                    c.com_available,
                    c.com_fallback_available,
                    c.com_fallback_domain,
                    c.de_available,
                    c.ch_available,
                    c.web_result_count,
                    c.web_exact_hits,
                    c.web_near_hits,
                    c.web_sample_domains,
                    c.web_source,
                    c.external_penalty,
                    c.hard_fail,
                    c.fail_reason,
                    recommendation(c, gate),
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate and screen app name candidates.')
    parser.add_argument('--scope', choices=['dach', 'eu', 'global'], default='eu')
    parser.add_argument('--gate', choices=['balanced', 'strict'], default='strict')
    parser.add_argument('--seeds', default='', help='Comma-separated seed names/roots.')
    parser.add_argument(
        '--candidates',
        default='',
        help='Comma-separated explicit candidate names to screen (always included).',
    )
    parser.add_argument(
        '--only-candidates',
        action='store_true',
        help='Do not generate names; only screen --candidates.',
    )
    parser.add_argument('--pool-size', type=int, default=300, help='Internal pool size before external checks.')
    parser.add_argument('--check-limit', type=int, default=80, help='Top N to run external checks on.')
    parser.add_argument('--min-len', type=int, default=6)
    parser.add_argument('--max-len', type=int, default=11)
    parser.add_argument(
        '--store-countries',
        default='de,ch,us,gb,fr,it',
        help='Comma-separated country codes for App Store exact-match checks.',
    )
    parser.add_argument('--web-top', type=int, default=8, help='How many web search results to inspect.')
    parser.add_argument('--web-check', dest='web_check', action='store_true', default=True)
    parser.add_argument('--no-web-check', dest='web_check', action='store_false')
    parser.add_argument('--require-base-com', action='store_true', help='Require base <name>.com availability.')
    parser.add_argument(
        '--fail-on-unknown',
        action='store_true',
        help='Treat unknown external-check states as hard-fail.',
    )
    parser.add_argument('--throttle-ms', type=int, default=0, help='Sleep between candidate checks (ms).')
    parser.add_argument('--output', default='', help='Output CSV path.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    seeds = [s.strip() for s in args.seeds.split(',') if s.strip()]
    explicit_candidates = [normalize_alpha(s.strip()) for s in args.candidates.split(',') if s.strip()]
    explicit_candidates = [c for c in explicit_candidates if c]
    store_countries = [s.strip().lower() for s in args.store_countries.split(',') if re.fullmatch(r'[a-z]{2}', s.strip().lower())]
    if not store_countries:
        store_countries = ['de', 'ch', 'us']

    similarity_fail_threshold = 80 if args.gate == 'strict' else 88
    require_base_com = args.require_base_com or args.gate == 'strict'
    fail_on_unknown = args.fail_on_unknown or args.gate == 'strict'

    if args.only_candidates:
        generated = explicit_candidates
    else:
        generated = generate_candidates(args.scope, seeds, args.min_len, args.max_len)
        generated.extend(explicit_candidates)
        generated = sorted(set(generated))

    if not generated:
        print('No candidates to evaluate. Provide --candidates and/or generation inputs.')
        return 1

    evaluated = evaluate_candidates(args.scope, generated, similarity_fail_threshold)

    ranked = sorted(
        evaluated,
        key=lambda c: (c.hard_fail, -c.total_score, c.challenge_risk, -c.quality_score, c.name),
    )

    pool = ranked[: max(1, args.pool_size)]
    to_check = pool[: max(1, args.check_limit)]

    if explicit_candidates:
        by_name = {c.name: c for c in evaluated}
        in_check = {c.name for c in to_check}
        for name in explicit_candidates:
            c = by_name.get(name)
            if c and c.name not in in_check:
                to_check.append(c)
                in_check.add(c.name)
    run_external_checks(
        to_check,
        args.scope,
        args.throttle_ms,
        args.gate,
        store_countries,
        args.web_check,
        args.web_top,
        require_base_com,
        fail_on_unknown,
    )
    final_ranked = sorted(
        to_check,
        key=lambda c: (
            c.hard_fail,
            {'strong': 0, 'consider': 1, 'weak': 2, 'reject': 3}[recommendation(c, args.gate)],
            c.challenge_risk,
            -c.total_score,
            c.name,
        ),
    )

    out_path = args.output
    if not out_path:
        ts = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        out_path = f'docs/branding/generated_name_candidates_{args.scope}_{args.gate}_{ts}.csv'
    output_file = Path(out_path)
    write_csv(output_file, args.scope, final_ranked, args.gate)

    print(f'Wrote {len(final_ranked)} screened candidates: {output_file}')
    print('Top candidates:')
    shown = 0
    for c in final_ranked:
        rec = recommendation(c, args.gate)
        if rec in {'strong', 'consider'} and not c.hard_fail:
            print(
                f'- {c.name:12s} | rec={rec:8s} | total={c.total_score:3d} | '
                f'risk={c.challenge_risk:3d} | domains(com/de/ch)='
                f'{c.com_available}/{c.de_available}/{c.ch_available} | '
                f'fallback={c.com_fallback_domain or "-"} | '
                f'store(de/ch/us)={c.store_de_count}/{c.store_ch_count}/{c.store_us_count} | '
                f'web(exact/near)={c.web_exact_hits}/{c.web_near_hits}'
            )
            shown += 1
        if shown >= 15:
            break

    if shown == 0:
        print('No strong/consider candidates found with current constraints.')

    print('\nNext: manually verify top 5 in DPMA/IGE/EUIPO and run user trust tests.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
