#!/usr/bin/env python3
"""Helpers for active LLM ideation stage in naming campaigns.

This module is intentionally stdlib-only so campaign runs do not require extra
Python dependencies.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import re
from pathlib import Path
from typing import Any
from urllib import error, request


MODE_TRIPLETS: tuple[tuple[str, str, str], ...] = (
    ('smooth', 'blend', 'trust'),
    ('crisp', 'coined', 'precision'),
    ('balanced', 'hybrid', 'clarity'),
    ('smooth', 'coined', 'neutral'),
    ('crisp', 'hybrid', 'trust'),
    ('balanced', 'blend', 'precision'),
)


def normalize_alpha_name(raw: str) -> str:
    return re.sub(r'[^a-z]', '', str(raw or '').strip().lower())


def _sanitize_text(raw: Any, *, max_chars: int) -> str:
    text = ' '.join(str(raw or '').strip().split())
    if not text:
        return ''
    if len(text) > max_chars:
        return text[:max_chars].rstrip()
    return text


def _sanitize_phrase_list(raw: Any, *, max_items: int, max_chars: int) -> list[str]:
    if isinstance(raw, str):
        source = [part.strip() for part in raw.split(',') if part.strip()]
    elif isinstance(raw, list):
        source = [item for item in raw]
    else:
        source = []
    out: list[str] = []
    seen: set[str] = set()
    for item in source:
        phrase = _sanitize_text(item, max_chars=max_chars)
        key = phrase.lower()
        if not phrase or key in seen:
            continue
        seen.add(key)
        out.append(phrase)
        if len(out) >= max(1, max_items):
            break
    return out


def _sanitize_name_list(raw: Any, *, max_items: int) -> list[str]:
    if isinstance(raw, str):
        source = [part.strip() for part in raw.split(',') if part.strip()]
    elif isinstance(raw, list):
        source = [item for item in raw]
    else:
        source = []
    out: list[str] = []
    seen: set[str] = set()
    for item in source:
        name = normalize_alpha_name(str(item or ''))
        if not (4 <= len(name) <= 16):
            continue
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
        if len(out) >= max(1, max_items):
            break
    return out


def _sanitize_tone_mix(raw: Any, *, max_items: int) -> dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for key in sorted(raw.keys()):
        label = re.sub(r'[^a-z0-9_-]', '', str(key or '').strip().lower())
        if not label:
            continue
        try:
            value = float(raw[key])
        except (TypeError, ValueError):
            continue
        out[label[:24]] = round(max(0.0, min(1.0, value)), 2)
        if len(out) >= max(1, max_items):
            break
    return out


def load_context_packet(path: str) -> dict[str, Any]:
    file_path = Path(path).expanduser()
    if not str(path or '').strip():
        return {}
    if not file_path.exists():
        raise ValueError(f'context_file_not_found:{file_path}')
    try:
        payload = json.loads(file_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f'context_file_invalid_json:{file_path}:{exc}') from exc
    if not isinstance(payload, dict):
        raise ValueError(f'context_file_invalid_root:{file_path}')

    source = payload.get('llm_context') if isinstance(payload.get('llm_context'), dict) else payload
    if not isinstance(source, dict):
        raise ValueError(f'context_file_invalid_packet:{file_path}')

    context: dict[str, Any] = {}
    product_core = _sanitize_text(source.get('product_core'), max_chars=180)
    if product_core:
        context['product_core'] = product_core

    target_users = _sanitize_phrase_list(source.get('target_users'), max_items=8, max_chars=56)
    if target_users:
        context['target_users'] = target_users

    trust_signals = _sanitize_phrase_list(source.get('trust_signals'), max_items=10, max_chars=40)
    if trust_signals:
        context['trust_signals'] = trust_signals

    forbidden_directions = _sanitize_phrase_list(source.get('forbidden_directions'), max_items=10, max_chars=48)
    if forbidden_directions:
        context['forbidden_directions'] = forbidden_directions

    language_market = _sanitize_text(source.get('language_market'), max_chars=80)
    if language_market:
        context['language_market'] = language_market

    tone_mix = _sanitize_tone_mix(source.get('tone_mix'), max_items=8)
    if tone_mix:
        context['tone_mix'] = tone_mix

    good_examples = _sanitize_name_list(source.get('good_examples'), max_items=10)
    if good_examples:
        context['good_examples'] = good_examples

    bad_examples = _sanitize_name_list(source.get('bad_examples'), max_items=10)
    if bad_examples:
        context['bad_examples'] = bad_examples

    seed_roots = _sanitize_name_list(source.get('seed_roots'), max_items=20)
    if seed_roots:
        context['seed_roots'] = seed_roots

    notes = _sanitize_text(source.get('notes'), max_chars=240)
    if notes:
        context['notes'] = notes

    return context


def render_context_lines(context_packet: dict[str, Any]) -> list[str]:
    if not isinstance(context_packet, dict) or not context_packet:
        return []
    lines: list[str] = []
    if context_packet.get('product_core'):
        lines.append(f"product_core: {context_packet['product_core']}")
    if context_packet.get('target_users'):
        lines.append(f"target_users: {', '.join(context_packet['target_users'])}")
    if context_packet.get('trust_signals'):
        lines.append(f"trust_signals: {', '.join(context_packet['trust_signals'])}")
    if context_packet.get('forbidden_directions'):
        lines.append(f"forbidden_directions: {', '.join(context_packet['forbidden_directions'])}")
    if context_packet.get('language_market'):
        lines.append(f"language_market: {context_packet['language_market']}")
    if context_packet.get('tone_mix'):
        tone_pairs = [f'{key}={value:.2f}' for key, value in context_packet['tone_mix'].items()]
        lines.append(f"tone_mix: {', '.join(tone_pairs)}")
    if context_packet.get('good_examples'):
        lines.append(f"good_examples: {', '.join(context_packet['good_examples'])}")
    if context_packet.get('bad_examples'):
        lines.append(f"bad_examples: {', '.join(context_packet['bad_examples'])}")
    if context_packet.get('seed_roots'):
        lines.append(f"seed_roots: {', '.join(context_packet['seed_roots'])}")
    if context_packet.get('notes'):
        lines.append(f"notes: {context_packet['notes']}")
    return lines


def is_truthy(raw: str) -> bool:
    return str(raw or '').strip().lower() in {'1', 'true', 'yes', 'y'}


def extract_json_object(raw: str) -> str | None:
    start = raw.find('{')
    if start < 0:
        return None
    depth = 0
    for idx in range(start, len(raw)):
        ch = raw[idx]
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return raw[start : idx + 1]
    return None


def parse_candidate_payload(raw_text: str) -> list[str]:
    text = raw_text.strip()
    if not text:
        return []
    payload: Any
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        extracted = extract_json_object(text)
        if not extracted:
            return []
        try:
            payload = json.loads(extracted)
        except json.JSONDecodeError:
            return []

    source: list[Any] = []
    if isinstance(payload, dict):
        if isinstance(payload.get('candidates'), list):
            source = list(payload['candidates'])
        elif isinstance(payload.get('names'), list):
            source = list(payload['names'])
    elif isinstance(payload, list):
        source = list(payload)

    out: list[str] = []
    for item in source:
        if isinstance(item, str):
            name = normalize_alpha_name(item)
            if 5 <= len(name) <= 12:
                out.append(name)
            continue
        if isinstance(item, dict):
            raw_name = item.get('name') or item.get('candidate')
            if isinstance(raw_name, str):
                name = normalize_alpha_name(raw_name)
                if 5 <= len(name) <= 12:
                    out.append(name)
    return sorted(set(out))


def load_fixture_candidates(path: str) -> list[str]:
    p = Path(path)
    if not path or not p.exists():
        return []
    raw = p.read_text(encoding='utf-8')
    names = parse_candidate_payload(raw)
    if names:
        return names
    fallback: list[str] = []
    for line in raw.splitlines():
        name = normalize_alpha_name(line.strip().strip('-*').strip())
        if 5 <= len(name) <= 12:
            fallback.append(name)
    return sorted(set(fallback))


def canonical_constraints(constraints: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in sorted((constraints or {}).items()):
        if isinstance(value, list):
            out[key] = sorted({str(v).strip().lower() for v in value if str(v).strip()})
        elif isinstance(value, dict):
            out[key] = {k: value[k] for k in sorted(value.keys())}
        else:
            out[key] = value
    return out


def constraints_hash(constraints: dict[str, Any]) -> str:
    canonical = canonical_constraints(constraints)
    blob = json.dumps(canonical, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


def _iter_recent_run_csvs(*, runs_dir: Path, window_runs: int) -> list[Path]:
    csvs = sorted(runs_dir.glob('run_*.csv'))
    if window_runs <= 0:
        return []
    return csvs[-window_runs:]


def _name_fragments(name: str, size: int = 4) -> list[str]:
    if len(name) < size:
        return []
    return [name[idx : idx + size] for idx in range(0, len(name) - size + 1)]


def _shannon_entropy(counts: dict[str, int]) -> float:
    total = sum(counts.values())
    if total <= 0:
        return 0.0
    entropy = 0.0
    for count in counts.values():
        p = count / total
        if p > 0.0:
            entropy -= p * math.log2(p)
    return entropy


def compute_dynamic_constraints(
    *,
    runs_dir: Path,
    seen_shortlist: set[str],
    window_runs: int = 5,
    fail_threshold: float = 0.20,
    entropy_threshold: float = 2.5,
    max_token_ban: int = 50,
    max_prefix_ban: int = 30,
    carry_prev_from_latest: bool = True,
) -> dict[str, Any]:
    fail_rows: list[tuple[str, str]] = []
    shortlist_rows: list[str] = list(seen_shortlist)
    reason_counts: dict[str, int] = {}
    fragment_counts: dict[str, int] = {}
    prefix_counts: dict[str, int] = {}
    total_fails = 0

    for csv_path in _iter_recent_run_csvs(runs_dir=runs_dir, window_runs=window_runs):
        with csv_path.open('r', encoding='utf-8', newline='') as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                name = normalize_alpha_name(str(row.get('name') or ''))
                if not name:
                    continue
                if is_truthy(row.get('shortlist_selected') or ''):
                    shortlist_rows.append(name)
                if not is_truthy(row.get('hard_fail') or ''):
                    continue
                reason = str(row.get('fail_reason') or 'unknown').strip() or 'unknown'
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
                total_fails += 1
                fail_rows.append((name, reason))

    selected_reasons: set[str] = set()
    if total_fails > 0:
        for reason, count in reason_counts.items():
            if (count / total_fails) >= max(0.0, min(1.0, fail_threshold)):
                selected_reasons.add(reason)

    banned_prefixes: list[str] = []
    for name, reason in fail_rows:
        if selected_reasons and reason not in selected_reasons:
            continue
        if len(name) >= 4:
            banned_prefixes.append(name[:4])
        for frag in _name_fragments(name, size=4):
            fragment_counts[frag] = fragment_counts.get(frag, 0) + 1

    banned_tokens = [frag for frag, cnt in sorted(fragment_counts.items(), key=lambda kv: (-kv[1], kv[0])) if cnt >= 2]

    for name in shortlist_rows:
        if len(name) >= 3:
            prefix = name[:3]
            prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
    entropy = _shannon_entropy(prefix_counts)
    max_share = 0.0
    if prefix_counts:
        denom = sum(prefix_counts.values())
        max_share = max(prefix_counts.values()) / max(1, denom)

    if prefix_counts and (entropy < max(0.0, entropy_threshold) or max_share > 0.30):
        over = sorted(prefix_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        for pref, _ in over:
            banned_prefixes.append(pref)

    carry_tokens: list[str] = []
    carry_prefixes: list[str] = []
    if carry_prev_from_latest:
        latest = sorted(runs_dir.glob('run_*_dynamic_constraints.json'))
        if latest:
            try:
                prev = json.loads(latest[-1].read_text(encoding='utf-8'))
                if isinstance(prev, dict):
                    if isinstance(prev.get('banned_tokens'), list):
                        carry_tokens = [str(v) for v in prev['banned_tokens']]
                    if isinstance(prev.get('banned_prefixes'), list):
                        carry_prefixes = [str(v) for v in prev['banned_prefixes']]
            except (OSError, json.JSONDecodeError):
                pass

    merged_tokens = [t for t in carry_tokens if t] + [t for t in banned_tokens if t]
    merged_prefixes = [p for p in carry_prefixes if p] + [p for p in banned_prefixes if p]

    def _dedupe_keep_order(values: list[str], cap: int) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            token = normalize_alpha_name(value)
            if not token:
                continue
            if token in seen:
                continue
            seen.add(token)
            out.append(token)
        if cap > 0 and len(out) > cap:
            return out[-cap:]
        return out

    final_tokens = _dedupe_keep_order(merged_tokens, max(1, max_token_ban))
    final_prefixes = _dedupe_keep_order(merged_prefixes, max(1, max_prefix_ban))

    return {
        'window_runs': int(max(1, window_runs)),
        'reason_counts': reason_counts,
        'selected_reasons': sorted(selected_reasons),
        'fail_total': int(total_fails),
        'shortlist_prefix_entropy': round(entropy, 4),
        'shortlist_prefix_max_share': round(max_share, 4),
        'banned_tokens': final_tokens,
        'banned_prefixes': final_prefixes,
        'max_pattern_counts': {'prefix4': 1},
        'min_new_names_target': max(8, min(40, len(final_prefixes) + 8)),
    }


def build_prompt(
    *,
    scope: str,
    round_index: int,
    target_count: int,
    constraints: dict[str, Any],
    context_packet: dict[str, Any] | None = None,
) -> tuple[str, tuple[str, str, str]]:
    mode = MODE_TRIPLETS[round_index % len(MODE_TRIPLETS)]
    phonetic, morphology, semantic = mode
    banned_tokens = ','.join((constraints or {}).get('banned_tokens', [])[:30]) or 'none'
    banned_prefixes = ','.join((constraints or {}).get('banned_prefixes', [])[:20]) or 'none'
    context_lines = render_context_lines(context_packet or {})
    context_block = ''
    if context_lines:
        context_block = 'Context packet:\n' + '\n'.join(f'- {line}' for line in context_lines) + '\n'
    prompt = (
        'Generate app brand names for utility-cost settlement software.\n'
        f'Scope: {scope}\n'
        f'Round: {round_index + 1}\n'
        f'Target candidates: {max(1, int(target_count))}\n'
        f'Phonetic mode: {phonetic}\n'
        f'Morphology mode: {morphology}\n'
        f'Semantic mode: {semantic}\n'
        f'Banned tokens: {banned_tokens}\n'
        f'Banned prefixes: {banned_prefixes}\n'
        f'{context_block}'
        'Rules:\n'
        '- lowercase latin letters only, 5-12 chars\n'
        '- no spaces, punctuation, digits\n'
        '- align with context packet priorities when provided\n'
        '- no availability claims (domain/store/trademark/social)\n'
        '- no duplicate names\n'
        '- no two names with same first 4 letters in this output\n'
        'Return JSON only with schema: {"candidates":[{"name":"string"}]}.\n'
        'No markdown, no prose, no additional keys.'
    )
    return prompt, mode


def list_openrouter_models(*, api_key: str, timeout_ms: int) -> set[str] | None:
    req = request.Request(
        'https://openrouter.ai/api/v1/models',
        headers={
            'Authorization': f'Bearer {api_key}',
            'Accept': 'application/json',
        },
        method='GET',
    )
    try:
        with request.urlopen(req, timeout=max(1.0, timeout_ms / 1000.0)) as resp:
            raw = resp.read().decode('utf-8')
        payload = json.loads(raw)
    except (error.URLError, error.HTTPError, TimeoutError, json.JSONDecodeError):
        return None
    data = payload.get('data') if isinstance(payload, dict) else None
    if not isinstance(data, list):
        return None
    ids: set[str] = set()
    for item in data:
        if isinstance(item, dict) and isinstance(item.get('id'), str):
            ids.add(item['id'])
    return ids


def call_openrouter_candidates(
    *,
    api_key: str,
    model: str,
    prompt: str,
    timeout_ms: int,
    strict_json: bool,
) -> tuple[list[str], dict[str, Any], str]:
    schema = {
        'name': 'name_candidates',
        'strict': bool(strict_json),
        'schema': {
            'type': 'object',
            'properties': {
                'candidates': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {'name': {'type': 'string'}},
                        'required': ['name'],
                        'additionalProperties': False,
                    },
                }
            },
            'required': ['candidates'],
            'additionalProperties': False,
        },
    }
    base_body = {
        'model': model,
        'messages': [{'role': 'user', 'content': prompt}],
        'temperature': 0.8,
    }
    attempts: list[dict[str, Any]] = [
        {
            **base_body,
            'response_format': {'type': 'json_schema', 'json_schema': schema},
            'provider': {'require_parameters': True},
        },
        {
            **base_body,
            'response_format': {'type': 'json_object'},
        },
        base_body,
    ]

    def request_once(payload_body: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
        data = json.dumps(payload_body, ensure_ascii=False).encode('utf-8')
        req = request.Request(
            'https://openrouter.ai/api/v1/chat/completions',
            data=data,
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            method='POST',
        )
        try:
            with request.urlopen(req, timeout=max(1.0, timeout_ms / 1000.0)) as resp:
                raw = resp.read().decode('utf-8')
        except error.HTTPError as exc:
            return '', {}, f'http_{exc.code}'
        except error.URLError:
            return '', {}, 'network_error'
        except TimeoutError:
            return '', {}, 'timeout'

        try:
            response = json.loads(raw)
        except json.JSONDecodeError:
            return '', {}, 'response_json_decode_error'
        usage = response.get('usage') if isinstance(response, dict) and isinstance(response.get('usage'), dict) else {}
        choices = response.get('choices') if isinstance(response, dict) else None
        if not isinstance(choices, list) or not choices:
            return '', usage, 'missing_choices'
        msg = choices[0].get('message') if isinstance(choices[0], dict) else None
        content = msg.get('content') if isinstance(msg, dict) else ''
        if isinstance(content, list):
            text_parts = []
            for part in content:
                if isinstance(part, dict) and isinstance(part.get('text'), str):
                    text_parts.append(part['text'])
            content = '\n'.join(text_parts)
        if not isinstance(content, str):
            content = str(content or '')
        return content, usage, ''

    fallback_http_errors = {'http_400', 'http_404', 'http_422'}
    last_usage: dict[str, Any] = {}
    last_err = 'unknown'
    for idx, payload_body in enumerate(attempts):
        content, usage, err = request_once(payload_body)
        if usage:
            last_usage = usage
        if err:
            last_err = err
            if idx + 1 < len(attempts) and err in fallback_http_errors:
                continue
            return [], last_usage, err

        names = parse_candidate_payload(content)
        if names:
            return names, usage, ''
        last_err = 'candidate_parse_failed'
        if idx + 1 < len(attempts):
            continue
        return [], last_usage, last_err

    return [], last_usage, last_err


def estimate_usage_cost_usd(
    *,
    usage: dict[str, Any],
    in_price_per_1k: float,
    out_price_per_1k: float,
) -> float:
    prompt_tokens = float(usage.get('prompt_tokens') or usage.get('input_tokens') or 0.0)
    completion_tokens = float(usage.get('completion_tokens') or usage.get('output_tokens') or 0.0)
    in_cost = (prompt_tokens / 1000.0) * max(0.0, in_price_per_1k)
    out_cost = (completion_tokens / 1000.0) * max(0.0, out_price_per_1k)
    return round(in_cost + out_cost, 8)


def parse_family_quotas(raw: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for part in str(raw or '').split(','):
        chunk = part.strip()
        if not chunk or ':' not in chunk:
            continue
        key, value = chunk.split(':', 1)
        family = key.strip()
        try:
            quota = int(value.strip())
        except ValueError:
            continue
        if family and quota > 0:
            out[family] = quota
    return out


def render_family_quotas(*, quotas: dict[str, int], family_order: list[str]) -> str:
    chunks = []
    for family in family_order:
        if family in quotas:
            chunks.append(f'{family}:{max(1, int(quotas[family]))}')
    return ','.join(chunks)


def adapt_family_quotas(
    *,
    runs_dir: Path,
    base_quota_profile: str,
    active_families: list[str],
    window_runs: int = 5,
) -> tuple[str, dict[str, Any]]:
    quotas = parse_family_quotas(base_quota_profile)
    if not quotas:
        return base_quota_profile, {'adjusted': False, 'reason': 'invalid_base_profile'}
    total_before = sum(quotas.values())

    family_total: dict[str, int] = {}
    family_fail: dict[str, int] = {}
    family_shortlist: dict[str, int] = {}
    for csv_path in _iter_recent_run_csvs(runs_dir=runs_dir, window_runs=window_runs):
        with csv_path.open('r', encoding='utf-8', newline='') as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                family = str(row.get('generator_family') or '').strip()
                if not family:
                    continue
                family_total[family] = family_total.get(family, 0) + 1
                if is_truthy(row.get('hard_fail') or ''):
                    family_fail[family] = family_fail.get(family, 0) + 1
                if is_truthy(row.get('shortlist_selected') or ''):
                    family_shortlist[family] = family_shortlist.get(family, 0) + 1

    adjustments: dict[str, int] = {}
    for family in active_families:
        if family not in quotas:
            continue
        total = family_total.get(family, 0)
        fail_rate = (family_fail.get(family, 0) / total) if total > 0 else 0.0
        shortlist_rate = (family_shortlist.get(family, 0) / total) if total > 0 else 0.0
        delta = 0
        if total >= 10 and fail_rate >= 0.45:
            delta = -20
        elif total >= 10 and fail_rate <= 0.20 and shortlist_rate >= 0.10:
            delta = 20
        if delta:
            quotas[family] = max(20, quotas[family] + delta)
            adjustments[family] = delta

    if not adjustments:
        return base_quota_profile, {'adjusted': False, 'reason': 'no_adjustments'}

    total_after = sum(quotas.values())
    if total_after > 0 and total_before > 0:
        scale = total_before / total_after
        for family in list(quotas.keys()):
            quotas[family] = max(20, int(round(quotas[family] * scale)))

    rendered = render_family_quotas(quotas=quotas, family_order=active_families)
    return rendered or base_quota_profile, {
        'adjusted': True,
        'changes': adjustments,
        'total_before': total_before,
        'total_after': sum(parse_family_quotas(rendered).values()),
    }


def compute_hard_fail_ratio(csv_path: Path) -> float:
    if not csv_path.exists():
        return 0.0
    total = 0
    hard = 0
    with csv_path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            name = str(row.get('name') or '').strip()
            if not name:
                continue
            total += 1
            if is_truthy(row.get('hard_fail') or ''):
                hard += 1
    if total <= 0:
        return 0.0
    return hard / total


def build_ab_arms(*, max_runs: int, seed: int, block_size: int = 4) -> list[str]:
    if max_runs <= 0:
        return []
    rng = random.Random(seed)
    arms: list[str] = []
    while len(arms) < max_runs:
        half = max(1, block_size // 2)
        block = ['A'] * half + ['B'] * half
        if len(block) < block_size:
            block.append('A')
        rng.shuffle(block)
        arms.extend(block)
    return arms[:max_runs]
