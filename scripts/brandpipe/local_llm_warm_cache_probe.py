#!/usr/bin/env python3
"""Probe local LLM cold/warm latency and model residency behavior.

Supports:
- openai_compat runtimes (LM Studio, Ollama OpenAI compatibility, etc.)
- Ollama native API with keep_alive controls
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Any
from urllib import error, request


@dataclass
class ProbeRecord:
    phase: str
    index: int
    ok: bool
    status: int | None
    elapsed_ms: int
    error_kind: str
    error_message: str
    load_ms: int | None
    prompt_eval_ms: int | None
    eval_ms: int | None


def _coerce_keep_alive(raw: str) -> str | int:
    value = str(raw or '').strip()
    if not value:
        return ''
    if value.lstrip('-').isdigit():
        return int(value)
    return value


def _ns_to_ms(raw: Any) -> int | None:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    return int(round(value / 1_000_000.0))


def _request_json(*, url: str, payload: dict[str, Any] | None, headers: dict[str, str], timeout_s: float) -> tuple[
    dict[str, Any] | None, int | None, str, str
]:
    data: bytes | None = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req = request.Request(url, data=data, headers=headers, method='POST' if payload is not None else 'GET')
    try:
        with request.urlopen(req, timeout=max(0.2, float(timeout_s))) as resp:
            status = int(getattr(resp, 'status', 200))
            raw = resp.read().decode('utf-8', errors='replace')
        parsed = json.loads(raw) if raw else {}
        if not isinstance(parsed, dict):
            return None, status, 'invalid_json_root', 'response root is not a JSON object'
        return parsed, status, '', ''
    except error.HTTPError as exc:
        return None, int(exc.code), 'http_error', str(exc)
    except error.URLError as exc:
        return None, None, 'url_error', str(exc.reason)
    except TimeoutError as exc:
        return None, None, 'timeout', str(exc)
    except json.JSONDecodeError as exc:
        return None, None, 'json_decode_error', str(exc)


def probe_openai_compat(
    *,
    base_url: str,
    model: str,
    prompt: str,
    api_key: str,
    timeout_s: float,
    ttl_s: int,
    keep_alive: str | int,
    max_tokens: int,
) -> ProbeRecord:
    endpoint = f'{str(base_url).rstrip("/")}/chat/completions'
    body: dict[str, Any] = {
        'model': model,
        'messages': [{'role': 'user', 'content': prompt}],
        'temperature': 0.2,
        'max_tokens': max(1, int(max_tokens)),
        'stream': False,
    }
    if int(ttl_s) > 0:
        body['ttl'] = int(ttl_s)
    if keep_alive != '':
        body['keep_alive'] = keep_alive

    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    token = str(api_key or '').strip()
    if token:
        headers['Authorization'] = f'Bearer {token}'

    started = time.monotonic()
    payload, status, err_kind, err_msg = _request_json(url=endpoint, payload=body, headers=headers, timeout_s=timeout_s)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    ok = bool(payload is not None and not err_kind)
    return ProbeRecord(
        phase='main',
        index=0,
        ok=ok,
        status=status,
        elapsed_ms=elapsed_ms,
        error_kind=err_kind,
        error_message=err_msg,
        load_ms=None,
        prompt_eval_ms=None,
        eval_ms=None,
    )


def probe_ollama_native(
    *,
    base_url: str,
    model: str,
    prompt: str,
    timeout_s: float,
    keep_alive: str | int,
) -> ProbeRecord:
    endpoint = f'{str(base_url).rstrip("/")}/api/generate'
    body: dict[str, Any] = {
        'model': model,
        'prompt': prompt,
        'stream': False,
    }
    if keep_alive != '':
        body['keep_alive'] = keep_alive

    started = time.monotonic()
    payload, status, err_kind, err_msg = _request_json(
        url=endpoint,
        payload=body,
        headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
        timeout_s=timeout_s,
    )
    elapsed_ms = int((time.monotonic() - started) * 1000)
    ok = bool(payload is not None and not err_kind)
    return ProbeRecord(
        phase='main',
        index=0,
        ok=ok,
        status=status,
        elapsed_ms=elapsed_ms,
        error_kind=err_kind,
        error_message=err_msg,
        load_ms=_ns_to_ms((payload or {}).get('load_duration')),
        prompt_eval_ms=_ns_to_ms((payload or {}).get('prompt_eval_duration')),
        eval_ms=_ns_to_ms((payload or {}).get('eval_duration')),
    )


def check_catalog(*, provider: str, base_url: str, api_key: str, timeout_s: float) -> tuple[bool, str]:
    if provider == 'openai_compat':
        url = f'{str(base_url).rstrip("/")}/models'
        headers = {'Accept': 'application/json'}
        token = str(api_key or '').strip()
        if token:
            headers['Authorization'] = f'Bearer {token}'
        payload, status, err_kind, err_msg = _request_json(url=url, payload=None, headers=headers, timeout_s=timeout_s)
        if err_kind:
            return False, f'catalog status={status} err={err_kind} msg={err_msg}'
        count = 0
        data = (payload or {}).get('data')
        if isinstance(data, list):
            count = len(data)
        return True, f'catalog status={status} models={count}'

    url = f'{str(base_url).rstrip("/")}/api/tags'
    payload, status, err_kind, err_msg = _request_json(
        url=url,
        payload=None,
        headers={'Accept': 'application/json'},
        timeout_s=timeout_s,
    )
    if err_kind:
        return False, f'catalog status={status} err={err_kind} msg={err_msg}'
    count = 0
    models = (payload or {}).get('models')
    if isinstance(models, list):
        count = len(models)
    return True, f'catalog status={status} models={count}'


def summarize(records: list[ProbeRecord]) -> dict[str, Any]:
    main_rows = [row for row in records if row.phase == 'main']
    main_ok = [row for row in main_rows if row.ok]
    cold_ms = main_ok[0].elapsed_ms if main_ok else None
    warm_ms = [row.elapsed_ms for row in main_ok[1:]]
    post_idle = next((row for row in records if row.phase == 'post_idle'), None)
    return {
        'total_main_runs': len(main_rows),
        'ok_main_runs': len(main_ok),
        'failed_main_runs': len(main_rows) - len(main_ok),
        'cold_elapsed_ms': cold_ms,
        'warm_min_ms': min(warm_ms) if warm_ms else None,
        'warm_median_ms': int(statistics.median(warm_ms)) if warm_ms else None,
        'warm_max_ms': max(warm_ms) if warm_ms else None,
        'post_idle_elapsed_ms': post_idle.elapsed_ms if post_idle else None,
        'post_idle_ok': post_idle.ok if post_idle else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Probe local LLM cold/warm latency and residency behavior.')
    parser.add_argument(
        '--provider',
        choices=['openai_compat', 'ollama_native'],
        default='openai_compat',
        help='Runtime API style to probe.',
    )
    parser.add_argument('--base-url', default='', help='Base URL (defaults by provider).')
    parser.add_argument('--model', required=True, help='Model identifier to call.')
    parser.add_argument('--api-key', default='ollama', help='Bearer token for openai_compat probes.')
    parser.add_argument(
        '--prompt',
        default='Return exactly: {"ok":true}',
        help='Prompt used for each probe request.',
    )
    parser.add_argument('--runs', type=int, default=5, help='Number of sequential probe calls.')
    parser.add_argument('--gap-s', type=float, default=1.0, help='Sleep between main probe calls.')
    parser.add_argument(
        '--eviction-gap-s',
        type=float,
        default=0.0,
        help='Optional idle gap before one extra post-idle probe call.',
    )
    parser.add_argument(
        '--ttl-s',
        type=int,
        default=0,
        help='Optional openai_compat ttl field in seconds (LM Studio supports this).',
    )
    parser.add_argument(
        '--keep-alive',
        default='',
        help='Optional keep_alive field (Ollama native supports this; some openai_compat runtimes may also accept it).',
    )
    parser.add_argument('--max-tokens', type=int, default=48, help='max_tokens used in openai_compat requests.')
    parser.add_argument('--timeout-s', type=float, default=30.0, help='Per-request timeout seconds.')
    parser.add_argument('--output-json', default='', help='Optional path to write JSON report.')
    parser.add_argument('--skip-catalog-check', action='store_true', help='Skip /models or /api/tags check.')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_url = str(args.base_url or '').strip()
    if not base_url:
        base_url = 'http://localhost:1234/v1' if args.provider == 'openai_compat' else 'http://localhost:11434'

    keep_alive = _coerce_keep_alive(args.keep_alive)
    print(
        'probe_config '
        f'provider={args.provider} base_url={base_url} model={args.model} runs={max(1, int(args.runs))} '
        f'gap_s={max(0.0, float(args.gap_s)):.3f} eviction_gap_s={max(0.0, float(args.eviction_gap_s)):.3f} '
        f'ttl_s={max(0, int(args.ttl_s))} keep_alive={keep_alive if keep_alive != "" else "unset"}'
    )

    if not args.skip_catalog_check:
        ok, line = check_catalog(
            provider=args.provider,
            base_url=base_url,
            api_key=args.api_key,
            timeout_s=max(0.2, float(args.timeout_s)),
        )
        prefix = 'catalog_ok' if ok else 'catalog_fail'
        print(f'{prefix} {line}')
        if not ok:
            return 2

    records: list[ProbeRecord] = []
    runs = max(1, int(args.runs))
    gap_s = max(0.0, float(args.gap_s))
    timeout_s = max(0.2, float(args.timeout_s))
    prompt = str(args.prompt)

    for idx in range(1, runs + 1):
        if idx > 1 and gap_s > 0.0:
            time.sleep(gap_s)
        if args.provider == 'openai_compat':
            row = probe_openai_compat(
                base_url=base_url,
                model=args.model,
                prompt=prompt,
                api_key=args.api_key,
                timeout_s=timeout_s,
                ttl_s=max(0, int(args.ttl_s)),
                keep_alive=keep_alive,
                max_tokens=max(1, int(args.max_tokens)),
            )
        else:
            row = probe_ollama_native(
                base_url=base_url,
                model=args.model,
                prompt=prompt,
                timeout_s=timeout_s,
                keep_alive=keep_alive,
            )
        row.phase = 'main'
        row.index = idx
        records.append(row)
        print(
            f'run phase={row.phase} idx={row.index} ok={int(row.ok)} status={row.status} '
            f'elapsed_ms={row.elapsed_ms} load_ms={row.load_ms} prompt_eval_ms={row.prompt_eval_ms} '
            f'eval_ms={row.eval_ms} err={row.error_kind or "-"}'
        )

    eviction_gap_s = max(0.0, float(args.eviction_gap_s))
    if eviction_gap_s > 0.0:
        print(f'idle_wait seconds={eviction_gap_s:.3f}')
        time.sleep(eviction_gap_s)
        if args.provider == 'openai_compat':
            row = probe_openai_compat(
                base_url=base_url,
                model=args.model,
                prompt=prompt,
                api_key=args.api_key,
                timeout_s=timeout_s,
                ttl_s=max(0, int(args.ttl_s)),
                keep_alive=keep_alive,
                max_tokens=max(1, int(args.max_tokens)),
            )
        else:
            row = probe_ollama_native(
                base_url=base_url,
                model=args.model,
                prompt=prompt,
                timeout_s=timeout_s,
                keep_alive=keep_alive,
            )
        row.phase = 'post_idle'
        row.index = 1
        records.append(row)
        print(
            f'run phase={row.phase} idx={row.index} ok={int(row.ok)} status={row.status} '
            f'elapsed_ms={row.elapsed_ms} load_ms={row.load_ms} prompt_eval_ms={row.prompt_eval_ms} '
            f'eval_ms={row.eval_ms} err={row.error_kind or "-"}'
        )

    summary = summarize(records)
    print(
        'probe_summary '
        f'total_main={summary["total_main_runs"]} ok_main={summary["ok_main_runs"]} '
        f'failed_main={summary["failed_main_runs"]} cold_ms={summary["cold_elapsed_ms"]} '
        f'warm_median_ms={summary["warm_median_ms"]} post_idle_ms={summary["post_idle_elapsed_ms"]}'
    )

    if str(args.output_json or '').strip():
        output_path = str(args.output_json).strip()
        payload = {
            'config': {
                'provider': args.provider,
                'base_url': base_url,
                'model': args.model,
                'runs': runs,
                'gap_s': gap_s,
                'eviction_gap_s': eviction_gap_s,
                'ttl_s': max(0, int(args.ttl_s)),
                'keep_alive': keep_alive,
                'timeout_s': timeout_s,
            },
            'records': [asdict(row) for row in records],
            'summary': summary,
        }
        with open(output_path, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write('\n')
        print(f'json_report path={output_path}')

    return 0 if int(summary['failed_main_runs']) == 0 else 2


if __name__ == '__main__':
    raise SystemExit(main())
