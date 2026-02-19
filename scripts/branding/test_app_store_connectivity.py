#!/usr/bin/env python3
"""Standalone App Store connectivity probe for naming pipeline diagnostics.

Purpose:
- verify iTunes Search API behavior for given names/countries
- capture exact failure class (HTTP status vs timeout/DNS/etc.)
- confirm whether unknown results are collisions or transport/API issues
"""

from __future__ import annotations

import argparse
import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

import name_generator as ng


@dataclass
class ProbeResult:
    name: str
    country: str
    url: str
    ok: bool
    exact: bool
    result_count: int
    status: int | None
    error_kind: str
    error_message: str
    attempts: int
    elapsed_ms: int


def parse_csv(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(',') if part.strip()]


def build_search_url(name: str, country: str, limit: int) -> str:
    query = urllib.parse.urlencode({'term': name, 'entity': 'software', 'country': country, 'limit': limit})
    return f'https://itunes.apple.com/search?{query}'


def probe_search(
    *,
    name: str,
    country: str,
    limit: int,
    timeout_s: float,
    retries: int,
    pause_ms: int,
    user_agent: str,
) -> ProbeResult:
    url = build_search_url(name, country, limit)
    req = urllib.request.Request(url, headers={'User-Agent': user_agent})
    started = time.monotonic()
    attempts = 0
    last_status: int | None = None
    last_error_kind = ''
    last_error_message = ''

    for attempt in range(retries + 1):
        attempts = attempt + 1
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                status = int(resp.status)
                raw = resp.read().decode('utf-8', errors='replace')
            payload = json.loads(raw)
            count = int(payload.get('resultCount', 0))
            normalized = ng.normalize_alpha(name)
            exact = any(
                ng.normalize_alpha(str(item.get('trackName', ''))) == normalized for item in payload.get('results', [])
            )
            return ProbeResult(
                name=name,
                country=country,
                url=url,
                ok=True,
                exact=exact,
                result_count=count,
                status=status,
                error_kind='',
                error_message='',
                attempts=attempts,
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        except urllib.error.HTTPError as err:
            last_status = int(err.code)
            last_error_kind = 'http_error'
            last_error_message = str(err)
            retryable = err.code in {408, 425, 429, 500, 502, 503, 504}
            if retryable and attempt < retries:
                time.sleep(max(0, pause_ms) / 1000.0)
                continue
            break
        except urllib.error.URLError as err:
            last_error_kind = 'url_error'
            last_error_message = str(err.reason)
            if attempt < retries:
                time.sleep(max(0, pause_ms) / 1000.0)
                continue
            break
        except TimeoutError as err:
            last_error_kind = 'timeout_error'
            last_error_message = str(err)
            if attempt < retries:
                time.sleep(max(0, pause_ms) / 1000.0)
                continue
            break
        except socket.timeout as err:
            last_error_kind = 'socket_timeout'
            last_error_message = str(err)
            if attempt < retries:
                time.sleep(max(0, pause_ms) / 1000.0)
                continue
            break
        except json.JSONDecodeError as err:
            last_error_kind = 'json_decode_error'
            last_error_message = str(err)
            break
        except Exception as err:  # pragma: no cover - defensive branch for runtime diagnostics
            last_error_kind = type(err).__name__
            last_error_message = str(err)
            if attempt < retries:
                time.sleep(max(0, pause_ms) / 1000.0)
                continue
            break

    return ProbeResult(
        name=name,
        country=country,
        url=url,
        ok=False,
        exact=False,
        result_count=-1,
        status=last_status,
        error_kind=last_error_kind or 'unknown_error',
        error_message=last_error_message,
        attempts=attempts,
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Probe iTunes Search API connectivity for naming pipeline.')
    parser.add_argument(
        '--names',
        default='fairshava,trueledla,trueledra,verolid',
        help='Comma-separated candidate names to probe.',
    )
    parser.add_argument(
        '--countries',
        default='de,ch,us,gb,fr,it',
        help='Comma-separated ISO country codes for iTunes search.',
    )
    parser.add_argument('--limit', type=int, default=8, help='Result limit sent to iTunes search API.')
    parser.add_argument('--timeout-s', type=float, default=8.0, help='Per-request timeout in seconds.')
    parser.add_argument('--retries', type=int, default=2, help='Retry attempts for transient failures.')
    parser.add_argument('--pause-ms', type=int, default=350, help='Pause between retries in milliseconds.')
    parser.add_argument('--user-agent', default=ng.USER_AGENT, help='User-Agent header.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    names = [ng.normalize_alpha(name) for name in parse_csv(args.names)]
    countries = [country.lower() for country in parse_csv(args.countries)]
    if not names or not countries:
        print('Provide at least one name and one country.')
        return 1

    print('info: iTunes Search API does not require an app ID; lookup API is the one that uses IDs.')
    print(
        'probe_config '
        f'names={names} countries={countries} timeout_s={args.timeout_s} '
        f'retries={args.retries} user_agent={args.user_agent}'
    )

    ok_count = 0
    fail_count = 0
    for name in names:
        for country in countries:
            result = probe_search(
                name=name,
                country=country,
                limit=max(1, int(args.limit)),
                timeout_s=max(0.5, float(args.timeout_s)),
                retries=max(0, int(args.retries)),
                pause_ms=max(0, int(args.pause_ms)),
                user_agent=args.user_agent,
            )
            if result.ok:
                ok_count += 1
                print(
                    f'PASS name={result.name} country={result.country} status={result.status} '
                    f'result_count={result.result_count} exact={int(result.exact)} '
                    f'attempts={result.attempts} elapsed_ms={result.elapsed_ms}'
                )
            else:
                fail_count += 1
                print(
                    f'FAIL name={result.name} country={result.country} status={result.status} '
                    f'error_kind={result.error_kind} attempts={result.attempts} '
                    f'elapsed_ms={result.elapsed_ms} error={result.error_message}'
                )

    print(f'probe_summary total={ok_count + fail_count} ok={ok_count} fail={fail_count}')
    return 0 if fail_count == 0 else 2


if __name__ == '__main__':
    raise SystemExit(main())
