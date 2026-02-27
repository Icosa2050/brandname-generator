#!/usr/bin/env python3
"""Async validator orchestration framework for naming pipeline.

V1 scope:
- create/run validation jobs per candidate + check type
- persist job lifecycle states: pending -> running -> success/fail
- retry with backoff
- persist validation results and run summary

This phase focuses on framework + deterministic checks. External adapters can
be plugged in later.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import sqlite3
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Protocol

import name_generator as ng
import naming_db as ndb
import path_config as bpaths


@dataclass
class CandidateRow:
    candidate_id: int
    name_display: str
    state: str
    current_score: float
    current_recommendation: str


@dataclass
class ValidationJobSpec:
    job_id: int
    run_id: int
    candidate_id: int
    candidate_name: str
    candidate_prev_state: str
    check_type: str


@dataclass
class ProgressState:
    total_jobs: int
    started_at_monotonic: float
    completed_jobs: int = 0
    success_jobs: int = 0
    failed_jobs: int = 0
    last_report_monotonic: float = 0.0


class InstrumentedLock:
    """Async lock with lightweight wait-time instrumentation."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.acquisition_count = 0
        self.total_wait_s = 0.0
        self.max_wait_s = 0.0
        self.contended_count = 0

    async def __aenter__(self) -> 'InstrumentedLock':
        started = time.monotonic()
        await self._lock.acquire()
        wait_s = max(0.0, time.monotonic() - started)
        self.acquisition_count += 1
        self.total_wait_s += wait_s
        if wait_s > self.max_wait_s:
            self.max_wait_s = wait_s
        if wait_s > 0.001:
            self.contended_count += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        self._lock.release()
        return False

    def snapshot(self) -> dict[str, int]:
        return {
            'lock_acquisitions': int(self.acquisition_count),
            'lock_total_wait_ms': int(round(self.total_wait_s * 1000.0)),
            'lock_max_wait_ms': int(round(self.max_wait_s * 1000.0)),
            'lock_contended_count': int(self.contended_count),
        }


class AdaptiveSemaphore:
    """Concurrency gate with runtime-adjustable limit."""

    def __init__(self, *, initial_concurrency: int, min_concurrency: int, max_concurrency: int) -> None:
        self._min = max(1, int(min_concurrency))
        self._max = max(self._min, int(max_concurrency))
        self._limit = max(self._min, min(self._max, int(initial_concurrency)))
        self._in_flight = 0
        self._condition = asyncio.Condition()

    async def __aenter__(self) -> 'AdaptiveSemaphore':
        async with self._condition:
            while self._in_flight >= self._limit:
                await self._condition.wait()
            self._in_flight += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        async with self._condition:
            self._in_flight = max(0, self._in_flight - 1)
            self._condition.notify_all()
        return False

    async def adjust(self, target_concurrency: int) -> int:
        target = max(self._min, min(self._max, int(target_concurrency)))
        async with self._condition:
            self._limit = target
            self._condition.notify_all()
        return target

    @property
    def current_limit(self) -> int:
        return int(self._limit)

    @property
    def bounds(self) -> tuple[int, int]:
        return (self._min, self._max)


def calculate_adaptive_concurrency_target(
    *,
    outcomes: list[str],
    current_concurrency: int,
    min_concurrency: int,
    max_concurrency: int,
) -> tuple[int, float]:
    current = max(1, int(current_concurrency))
    lower = max(1, int(min_concurrency))
    upper = max(lower, int(max_concurrency))
    if not outcomes:
        return max(lower, min(upper, current)), 0.0
    errors = sum(1 for outcome in outcomes if outcome != 'success')
    error_rate = errors / max(1, len(outcomes))
    target = current
    if len(outcomes) >= 50:
        if error_rate > 0.20:
            target = max(lower, max(1, current // 2))
        elif error_rate < 0.05:
            grown = max(current + 1, (current * 5 + 3) // 4)
            target = min(upper, grown)
    target = max(lower, min(upper, target))
    return target, error_rate


@dataclass(frozen=True)
class ValidationFeatureFlags:
    pipeline_version: str
    v3_enabled: bool
    validation_tier: str


class ValidationRunner(Protocol):
    def __call__(self, name: str, args: argparse.Namespace) -> dict:
        """Run a single validation check and return structured result payload."""


@dataclass(frozen=True)
class ValidationCheckSpec:
    check_type: str
    tier: str
    runner: ValidationRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Async validator orchestration for naming pipeline.')
    parser.add_argument('--db', default=str(bpaths.NAMING_PIPELINE_DB), help='SQLite DB path.')
    parser.add_argument(
        '--pipeline-version',
        choices=['v2', 'v3'],
        default='v2',
        help='Validation contract version toggle. Default v2 preserves existing flow.',
    )
    parser.add_argument(
        '--enable-v3',
        action='store_true',
        help='Feature flag enabling v3 validator behavior and tier filtering.',
    )
    parser.add_argument(
        '--validation-tier',
        choices=['all', 'cheap', 'expensive'],
        default='all',
        help='When v3 is enabled, select check tier set to run.',
    )
    parser.add_argument('--candidate-limit', type=int, default=100, help='Max candidates to validate in this run.')
    parser.add_argument(
        '--expensive-finalist-limit',
        type=int,
        default=30,
        help='When tiered (v3+all), expensive checks run only on top finalists.',
    )
    parser.add_argument(
        '--finalist-recommendations',
        default='strong,consider',
        help='Comma-separated recommendations prioritized for expensive tier in v3.',
    )
    parser.add_argument('--concurrency', type=int, default=12, help='Max concurrent jobs.')
    parser.add_argument(
        '--min-concurrency',
        type=int,
        default=2,
        help='Lower bound for adaptive concurrency scaling.',
    )
    parser.add_argument(
        '--max-concurrency',
        type=int,
        default=24,
        help='Upper bound for adaptive concurrency scaling.',
    )
    parser.add_argument('--max-retries', type=int, default=2, help='Retry attempts per job.')
    parser.add_argument('--retry-backoff-ms', type=int, default=300, help='Base retry backoff (ms).')
    parser.add_argument('--timeout-s', type=float, default=8.0, help='Per-check timeout seconds.')
    parser.add_argument(
        '--checks',
        default='adversarial,psych,descriptive',
        help='Comma-separated check types to execute.',
    )
    parser.add_argument(
        '--state-filter',
        default='new',
        help='Comma-separated candidate states eligible for validation.',
    )
    parser.add_argument('--scope', choices=['dach', 'eu', 'global'], default='global')
    parser.add_argument('--gate', choices=['strict', 'balanced'], default='balanced')

    parser.add_argument('--adversarial-fail-threshold', type=int, default=82)
    parser.add_argument('--adversarial-warn-threshold', type=int, default=68)
    parser.add_argument(
        '--cheap-trademark-screen',
        dest='cheap_trademark_screen',
        action='store_true',
        default=True,
        help='Enable static trademark pre-screen in cheap tier.',
    )
    parser.add_argument(
        '--no-cheap-trademark-screen',
        dest='cheap_trademark_screen',
        action='store_false',
    )
    parser.add_argument(
        '--cheap-trademark-fail-threshold',
        type=int,
        default=90,
        help='Hard-fail threshold for cheap trademark pre-screen similarity score (0-100).',
    )
    parser.add_argument(
        '--cheap-trademark-warn-threshold',
        type=int,
        default=78,
        help='Warn threshold for cheap trademark pre-screen similarity score (0-100).',
    )
    parser.add_argument('--min-trust-proxy', type=int, default=50)
    parser.add_argument('--warn-trust-proxy', type=int, default=62)
    parser.add_argument('--max-spelling-risk', type=int, default=28)
    parser.add_argument('--warn-spelling-risk', type=int, default=16)
    parser.add_argument('--descriptive-fail-threshold', type=int, default=72)
    parser.add_argument('--descriptive-warn-threshold', type=int, default=52)
    parser.add_argument('--web-top', type=int, default=8)
    parser.add_argument(
        '--web-exact-domain-fail-threshold',
        type=int,
        default=2,
        help='Hard-fail only when exact web collisions appear on this many distinct non-social domains.',
    )
    parser.add_argument('--web-near-fail-threshold', type=int, default=2)
    parser.add_argument('--store-countries', default='de,ch,us')
    parser.add_argument('--social-unavailable-fail-threshold', type=int, default=3)
    parser.add_argument('--strict-required-domains', action='store_true')
    parser.add_argument('--progress', dest='progress', action='store_true', default=True)
    parser.add_argument('--no-progress', dest='progress', action='store_false')
    parser.add_argument(
        '--cheap-cache',
        dest='cheap_cache',
        action='store_true',
        default=True,
        help='Reuse recent cheap-tier validation results from DB cache when signatures match.',
    )
    parser.add_argument(
        '--no-cheap-cache',
        dest='cheap_cache',
        action='store_false',
    )
    parser.add_argument(
        '--cheap-cache-ttl-s',
        type=int,
        default=3600,
        help='TTL for cheap-tier result reuse cache in seconds.',
    )
    parser.add_argument(
        '--memory-db',
        default='',
        help='Optional SQLite DB path for persistent hard-fail exclusion memory across campaigns.',
    )
    parser.add_argument(
        '--memory-ttl-days',
        type=int,
        default=180,
        help='Days to keep hard-fail exclusions active in memory DB.',
    )
    parser.add_argument(
        '--sqlite-busy-timeout-ms',
        type=int,
        default=ndb.DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
        help='SQLite busy timeout in milliseconds for primary and memory DB connections.',
    )
    parser.add_argument(
        '--track-job-lifecycle',
        dest='track_job_lifecycle',
        action='store_true',
        default=True,
        help='Persist per-attempt running/pending lifecycle updates in validation_jobs.',
    )
    parser.add_argument(
        '--no-track-job-lifecycle',
        dest='track_job_lifecycle',
        action='store_false',
    )
    parser.add_argument(
        '--progress-every',
        type=int,
        default=20,
        help='Emit progress after every N completed jobs.',
    )
    parser.add_argument(
        '--progress-interval-s',
        type=float,
        default=10.0,
        help='Emit progress when this many seconds elapsed since last report.',
    )
    parser.add_argument(
        '--stage-events',
        dest='stage_events',
        action='store_true',
        default=True,
        help='Emit structured JSON stage events for monitoring/triage.',
    )
    parser.add_argument(
        '--no-stage-events',
        dest='stage_events',
        action='store_false',
    )
    return parser.parse_args()


def parse_csv_set(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(',') if part.strip()]


def resolve_feature_flags(args: argparse.Namespace) -> ValidationFeatureFlags:
    pipeline_version = str(getattr(args, 'pipeline_version', 'v2') or 'v2').strip().lower()
    if pipeline_version not in {'v2', 'v3'}:
        pipeline_version = 'v2'
    v3_enabled = bool(getattr(args, 'enable_v3', False) or pipeline_version == 'v3')
    validation_tier = str(getattr(args, 'validation_tier', 'all') or 'all').strip().lower()
    if validation_tier not in {'all', 'cheap', 'expensive'}:
        validation_tier = 'all'
    if not v3_enabled:
        validation_tier = 'all'
    return ValidationFeatureFlags(
        pipeline_version=pipeline_version,
        v3_enabled=v3_enabled,
        validation_tier=validation_tier,
    )


MEMORY_POLICY_FIELDS: tuple[str, ...] = (
    'adversarial_fail_threshold',
    'adversarial_warn_threshold',
    'cheap_trademark_screen',
    'cheap_trademark_fail_threshold',
    'cheap_trademark_warn_threshold',
    'min_trust_proxy',
    'warn_trust_proxy',
    'max_spelling_risk',
    'warn_spelling_risk',
    'descriptive_fail_threshold',
    'descriptive_warn_threshold',
    'web_top',
    'web_near_fail_threshold',
    'social_unavailable_fail_threshold',
    'strict_required_domains',
)


def exclusion_memory_policy_signature(
    *,
    args: argparse.Namespace,
    checks: list[str],
    flags: ValidationFeatureFlags,
) -> str:
    payload: dict[str, object] = {
        'checks': sorted(checks),
        'pipeline_version': flags.pipeline_version,
        'v3_enabled': bool(flags.v3_enabled),
        'validation_tier': flags.validation_tier,
        'scope': str(getattr(args, 'scope', '') or ''),
        'gate': str(getattr(args, 'gate', '') or ''),
        'blocklist_size': len(CHEAP_TRADEMARK_BLOCKLIST),
    }
    for field in MEMORY_POLICY_FIELDS:
        payload[field] = getattr(args, field, None)
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()[:16]


def ensure_exclusion_memory_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS excluded_candidates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name_normalized TEXT NOT NULL,
          scope TEXT NOT NULL,
          gate TEXT NOT NULL,
          policy_signature TEXT NOT NULL,
          reasons_json TEXT NOT NULL DEFAULT '[]',
          fail_count INTEGER NOT NULL DEFAULT 1,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(name_normalized, scope, gate, policy_signature)
        );
        CREATE INDEX IF NOT EXISTS idx_excluded_candidates_lookup
          ON excluded_candidates(name_normalized, scope, gate, policy_signature, expires_at);
        """
    )
    conn.commit()


def _expires_at_iso(*, ttl_days: int) -> str:
    if ttl_days <= 0:
        ttl_days = 36500
    return (dt.datetime.now() + dt.timedelta(days=int(ttl_days))).isoformat(timespec='seconds')


def load_memory_excluded_names(
    conn: sqlite3.Connection,
    *,
    names: list[str],
    scope: str,
    gate: str,
    policy_signature: str,
) -> set[str]:
    normalized = sorted({ndb.normalize_name(name) for name in names if ndb.normalize_name(name)})
    if not normalized:
        return set()
    placeholders = ','.join('?' for _ in normalized)
    rows = conn.execute(
        f"""
        SELECT name_normalized
        FROM excluded_candidates
        WHERE scope = ? AND gate = ? AND policy_signature = ? AND expires_at >= ?
          AND name_normalized IN ({placeholders})
        """,
        (scope, gate, policy_signature, ndb.now_iso(), *normalized),
    ).fetchall()
    return {str(row[0]) for row in rows}


def mark_candidates_memory_excluded(
    conn: sqlite3.Connection,
    rows: list[CandidateRow],
    *,
    actor: str,
    note: str,
) -> None:
    ts = ndb.now_iso()
    for row in rows:
        conn.execute(
            """
            UPDATE candidates
            SET state = ?, status = ?, rejection_reason = ?, state_updated_at = ?
            WHERE id = ?
            """,
            ('memory_excluded', 'rejected_memory', 'memory_excluded', ts, row.candidate_id),
        )
        conn.execute(
            """
            INSERT INTO state_transitions(candidate_id, from_state, to_state, actor, note, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (row.candidate_id, row.state, 'memory_excluded', actor, note, ts),
        )


def collect_hard_fail_reasons_by_name(conn: sqlite3.Connection, *, run_id: int) -> dict[str, list[str]]:
    rows = conn.execute(
        """
        SELECT c.name_normalized, COALESCE(vr.reason, '')
        FROM validation_results vr
        JOIN candidates c ON c.id = vr.candidate_id
        WHERE vr.run_id = ? AND vr.hard_fail = 1
        """,
        (run_id,),
    ).fetchall()
    grouped: dict[str, set[str]] = {}
    for name_normalized, reason in rows:
        name = str(name_normalized or '').strip().lower()
        if not name:
            continue
        if name not in grouped:
            grouped[name] = set()
        reason_text = str(reason or '').strip()
        grouped[name].add(reason_text or 'hard_fail')
    return {
        name: sorted(values)[:8]
        for name, values in grouped.items()
    }


def upsert_exclusion_memory(
    conn: sqlite3.Connection,
    *,
    exclusions: dict[str, list[str]],
    scope: str,
    gate: str,
    policy_signature: str,
    ttl_days: int,
) -> int:
    if not exclusions:
        return 0
    ts = ndb.now_iso()
    expires_at = _expires_at_iso(ttl_days=ttl_days)
    for name, reasons in exclusions.items():
        reasons_json = json.dumps(list(reasons)[:8], ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO excluded_candidates(
              name_normalized, scope, gate, policy_signature, reasons_json, fail_count,
              first_seen_at, last_seen_at, expires_at, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name_normalized, scope, gate, policy_signature)
            DO UPDATE SET
              reasons_json = excluded.reasons_json,
              fail_count = excluded_candidates.fail_count + 1,
              last_seen_at = excluded.last_seen_at,
              expires_at = excluded.expires_at,
              updated_at = excluded.updated_at
            """,
            (
                name,
                scope,
                gate,
                policy_signature,
                reasons_json,
                1,
                ts,
                ts,
                expires_at,
                ts,
                ts,
            ),
        )
    conn.commit()
    return len(exclusions)


def emit_stage_event(enabled: bool, stage: str, **fields: object) -> None:
    if not enabled:
        return
    payload = {
        'event': 'naming_validation_stage',
        'timestamp': ndb.now_iso(),
        'stage': stage,
        **fields,
    }
    print(f'stage_event={json.dumps(payload, ensure_ascii=False)}', flush=True)


def select_expensive_finalists(
    rows: list[CandidateRow],
    *,
    recommendations: list[str],
    limit: int,
) -> list[CandidateRow]:
    if not rows:
        return []
    rec_set = {item.strip().lower() for item in recommendations if item.strip()}
    prioritized = [row for row in rows if row.current_recommendation.strip().lower() in rec_set]
    fallback = [row for row in rows if row.current_recommendation.strip().lower() not in rec_set]

    ranked = sorted(
        prioritized,
        key=lambda row: (-row.current_score, row.candidate_id),
    )
    if len(ranked) < limit:
        ranked.extend(sorted(fallback, key=lambda row: (-row.current_score, row.candidate_id)))
    return ranked[: max(1, limit)]


def load_candidates(conn: sqlite3.Connection, states: list[str], limit: int) -> list[CandidateRow]:
    if not states:
        return []
    placeholders = ','.join('?' for _ in states)
    rows = conn.execute(
        f"""
        SELECT id, name_display, state, COALESCE(current_score, 0), COALESCE(current_recommendation, '')
        FROM candidates
        WHERE state IN ({placeholders})
        ORDER BY id ASC
        LIMIT ?
        """,
        (*states, limit),
    ).fetchall()
    return [
        CandidateRow(
            candidate_id=int(row[0]),
            name_display=str(row[1]),
            state=str(row[2]),
            current_score=float(row[3] or 0.0),
            current_recommendation=str(row[4] or ''),
        )
        for row in rows
    ]


CHEAP_TRADEMARK_BLOCKLIST = sorted(
    set(
        list(ng.PROTECTED_MARKS)
        + list(ng.ADVERSARIAL_MARKS)
        + [
            'airbnb',
            'booking',
            'immobilienscout',
            'immoscout24',
            'immonet',
            'immowelt',
            'microsoft',
            'salesforce',
            'sap',
        ]
    )
)


def cheap_trademark_similarity_signal(name: str) -> tuple[int, str]:
    best_score = 0
    best_mark = ''
    for mark in CHEAP_TRADEMARK_BLOCKLIST:
        ratio = ng.similarity_with_prefix_boost(name, mark)
        if mark in name and len(mark) >= 5:
            ratio = max(ratio, 0.94)
        score = int(round(ratio * 100))
        if score > best_score:
            best_score = score
            best_mark = mark
    return best_score, best_mark


def check_adversarial(name: str, args: argparse.Namespace) -> dict:
    normalized = ng.normalize_alpha(name)
    risk, hits = ng.adversarial_similarity_signal(normalized)
    if risk >= args.adversarial_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -20.0,
            'reason': 'adversarial_confusion_risk',
            'evidence': {'risk': risk, 'hits': hits},
        }
    if risk >= args.adversarial_warn_threshold:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -8.0,
            'reason': 'adversarial_similarity_warning',
            'evidence': {'risk': risk, 'hits': hits},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'risk': risk, 'hits': hits},
    }


def check_psych(name: str, args: argparse.Namespace) -> dict:
    normalized = ng.normalize_alpha(name)
    spelling_risk = ng.psych_spelling_risk(normalized)
    trust_proxy = ng.psych_trust_proxy_score(normalized)

    if trust_proxy < args.min_trust_proxy or spelling_risk > args.max_spelling_risk:
        return {
            'status': 'fail',
            'hard_fail': False,
            'score_delta': -10.0,
            'reason': 'psych_quality_fail',
            'evidence': {'spelling_risk': spelling_risk, 'trust_proxy': trust_proxy},
        }
    if trust_proxy < args.warn_trust_proxy or spelling_risk > args.warn_spelling_risk:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -4.0,
            'reason': 'psych_quality_warn',
            'evidence': {'spelling_risk': spelling_risk, 'trust_proxy': trust_proxy},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'spelling_risk': spelling_risk, 'trust_proxy': trust_proxy},
    }


def check_descriptive(name: str, args: argparse.Namespace) -> dict:
    normalized = ng.normalize_alpha(name)
    risk = ng.descriptive_risk(normalized)
    if risk >= args.descriptive_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -15.0,
            'reason': 'descriptive_risk_fail',
            'evidence': {'descriptive_risk': risk},
        }
    if risk >= args.descriptive_warn_threshold:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -6.0,
            'reason': 'descriptive_risk_warn',
            'evidence': {'descriptive_risk': risk},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'descriptive_risk': risk},
    }


def check_trademark_cheap(name: str, args: argparse.Namespace) -> dict:
    normalized = ng.normalize_alpha(name)
    if not getattr(args, 'cheap_trademark_screen', True):
        return {
            'status': 'pass',
            'hard_fail': False,
            'score_delta': 0.0,
            'reason': 'cheap_trademark_screen_disabled',
            'evidence': {'screen_enabled': False},
        }

    similarity_score, closest_mark = cheap_trademark_similarity_signal(normalized)
    if similarity_score >= max(0, min(100, args.cheap_trademark_fail_threshold)):
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -16.0,
            'reason': 'cheap_trademark_collision_risk',
            'evidence': {
                'screen_enabled': True,
                'similarity_score': similarity_score,
                'closest_mark': closest_mark,
                'blocklist_size': len(CHEAP_TRADEMARK_BLOCKLIST),
            },
        }
    if similarity_score >= max(0, min(100, args.cheap_trademark_warn_threshold)):
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -6.0,
            'reason': 'cheap_trademark_similarity_warning',
            'evidence': {
                'screen_enabled': True,
                'similarity_score': similarity_score,
                'closest_mark': closest_mark,
                'blocklist_size': len(CHEAP_TRADEMARK_BLOCKLIST),
            },
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {
            'screen_enabled': True,
            'similarity_score': similarity_score,
            'closest_mark': closest_mark,
            'blocklist_size': len(CHEAP_TRADEMARK_BLOCKLIST),
        },
    }


def normalized_or_fail(name: str) -> str:
    normalized = ng.normalize_alpha(name)
    if not normalized:
        raise ValueError(f'Invalid candidate name for check: {name!r}')
    return normalized


def check_domain(name: str, args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    com = ng.rdap_available(normalized, 'com')
    de = ng.rdap_available(normalized, 'de')
    ch = ng.rdap_available(normalized, 'ch')
    required = ng.required_tlds(args.scope)
    availability = {'com': com, 'de': de, 'ch': ch}

    missing = [tld for tld in required if availability.get(tld) == 'no']
    unknown = [tld for tld in required if availability.get(tld) == 'unknown']
    if missing:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -18.0,
            'reason': f'domain_unavailable_{"-".join(missing)}',
            'evidence': {'required': required, 'availability': availability},
        }
    if unknown:
        if args.strict_required_domains:
            return {
                'status': 'fail',
                'hard_fail': True,
                'score_delta': -10.0,
                'reason': f'domain_unknown_{"-".join(unknown)}',
                'evidence': {'required': required, 'availability': availability},
            }
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -4.0,
            'reason': f'domain_unknown_{"-".join(unknown)}',
            'evidence': {'required': required, 'availability': availability},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'required': required, 'availability': availability},
    }


def check_web(name: str, args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    exact_hits, near_hits, result_count, sample_domains, ok, source = ng.web_collision_signal(normalized, args.web_top)
    if not ok or result_count < 0:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -2.0,
            'reason': 'web_check_unknown',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    exact_fail_threshold = max(1, int(getattr(args, 'web_exact_domain_fail_threshold', 2)))
    if exact_hits >= exact_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -20.0,
            'reason': 'web_exact_collision',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    if exact_hits > 0:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -6.0,
            'reason': 'web_exact_warning',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    if near_hits >= args.web_near_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': False,
            'score_delta': -10.0,
            'reason': 'web_near_collision',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    if near_hits > 0:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -4.0,
            'reason': 'web_near_warning',
            'evidence': {
                'exact_hits': exact_hits,
                'near_hits': near_hits,
                'result_count': result_count,
                'sample_domains': sample_domains,
                'source': source,
            },
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {
            'exact_hits': exact_hits,
            'near_hits': near_hits,
            'result_count': result_count,
            'sample_domains': sample_domains,
            'source': source,
        },
    }


def check_app_store(name: str, args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    countries = [c.strip().lower() for c in args.store_countries.split(',') if c.strip()]
    exact: list[str] = []
    unknown: list[str] = []
    counts: dict[str, int] = {}
    for country in countries:
        count, is_exact, ok = ng.app_store_signal(normalized, country)
        counts[country] = count
        if is_exact:
            exact.append(country)
        if not ok:
            unknown.append(country)
    if exact:
        return {
            'status': 'fail',
            'hard_fail': True,
            'score_delta': -18.0,
            'reason': f'app_store_exact_collision_{"-".join(exact)}',
            'evidence': {'countries': countries, 'counts': counts, 'exact': exact, 'unknown': unknown},
        }
    if unknown:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -3.0,
            'reason': f'app_store_unknown_{"-".join(unknown)}',
            'evidence': {'countries': countries, 'counts': counts, 'exact': exact, 'unknown': unknown},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'countries': countries, 'counts': counts, 'exact': exact, 'unknown': unknown},
    }


def check_package(name: str, _args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    pypi = ng.package_exists_on_pypi(normalized)
    npm = ng.package_exists_on_npm(normalized)
    collisions = [label for label, value in (('pypi', pypi), ('npm', npm)) if value == 'yes']
    unknown = [label for label, value in (('pypi', pypi), ('npm', npm)) if value == 'unknown']

    if collisions:
        return {
            'status': 'fail',
            'hard_fail': False,
            'score_delta': -10.0,
            'reason': f'package_collision_{"-".join(collisions)}',
            'evidence': {'pypi': pypi, 'npm': npm},
        }
    if unknown:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -2.0,
            'reason': f'package_unknown_{"-".join(unknown)}',
            'evidence': {'pypi': pypi, 'npm': npm},
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {'pypi': pypi, 'npm': npm},
    }


def check_social(name: str, args: argparse.Namespace) -> dict:
    normalized = normalized_or_fail(name)
    github, linkedin, x_handle, instagram, unavailable_count, unknown_count = ng.social_handle_signal(normalized)
    if unavailable_count >= args.social_unavailable_fail_threshold:
        return {
            'status': 'fail',
            'hard_fail': False,
            'score_delta': -8.0,
            'reason': 'social_handle_crowded',
            'evidence': {
                'github': github,
                'linkedin': linkedin,
                'x': x_handle,
                'instagram': instagram,
                'unavailable_count': unavailable_count,
                'unknown_count': unknown_count,
            },
        }
    if unknown_count > 0:
        return {
            'status': 'warn',
            'hard_fail': False,
            'score_delta': -2.0,
            'reason': 'social_check_unknown',
            'evidence': {
                'github': github,
                'linkedin': linkedin,
                'x': x_handle,
                'instagram': instagram,
                'unavailable_count': unavailable_count,
                'unknown_count': unknown_count,
            },
        }
    return {
        'status': 'pass',
        'hard_fail': False,
        'score_delta': 0.0,
        'reason': '',
        'evidence': {
            'github': github,
            'linkedin': linkedin,
            'x': x_handle,
            'instagram': instagram,
            'unavailable_count': unavailable_count,
            'unknown_count': unknown_count,
        },
    }


CHECK_SPECS: dict[str, ValidationCheckSpec] = {
    'adversarial': ValidationCheckSpec('adversarial', 'cheap', check_adversarial),
    'psych': ValidationCheckSpec('psych', 'cheap', check_psych),
    'descriptive': ValidationCheckSpec('descriptive', 'cheap', check_descriptive),
    'tm_cheap': ValidationCheckSpec('tm_cheap', 'cheap', check_trademark_cheap),
    'domain': ValidationCheckSpec('domain', 'expensive', check_domain),
    'web': ValidationCheckSpec('web', 'expensive', check_web),
    'app_store': ValidationCheckSpec('app_store', 'expensive', check_app_store),
    'package': ValidationCheckSpec('package', 'expensive', check_package),
    'social': ValidationCheckSpec('social', 'expensive', check_social),
}

CHECK_RUNNERS: dict[str, ValidationRunner] = {check_type: spec.runner for check_type, spec in CHECK_SPECS.items()}


def select_checks(args: argparse.Namespace) -> tuple[list[str], ValidationFeatureFlags]:
    checks = parse_csv_set(args.checks)
    flags = resolve_feature_flags(args)
    unknown = [check for check in checks if check not in CHECK_SPECS]
    if unknown:
        raise ValueError(f'Unknown checks: {", ".join(unknown)}')

    if flags.validation_tier == 'all':
        return checks, flags

    filtered: list[str] = []
    for check in checks:
        spec = CHECK_SPECS[check]
        if spec.tier == flags.validation_tier:
            filtered.append(check)
    return filtered, flags


CACHE_SIGNATURE_FIELDS: dict[str, tuple[str, ...]] = {
    'adversarial': ('adversarial_fail_threshold', 'adversarial_warn_threshold'),
    'psych': ('min_trust_proxy', 'warn_trust_proxy', 'max_spelling_risk', 'warn_spelling_risk'),
    'descriptive': ('descriptive_fail_threshold', 'descriptive_warn_threshold'),
    'tm_cheap': ('cheap_trademark_screen', 'cheap_trademark_fail_threshold', 'cheap_trademark_warn_threshold'),
}


def cheap_check_cache_signature(check_type: str, args: argparse.Namespace) -> str:
    fields = CACHE_SIGNATURE_FIELDS.get(check_type, ())
    payload: dict[str, object] = {'check_type': check_type}
    for field in fields:
        payload[field] = getattr(args, field, None)
    if check_type == 'tm_cheap':
        payload['blocklist_size'] = len(CHEAP_TRADEMARK_BLOCKLIST)
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()[:12]


def load_cached_validation_result(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    check_type: str,
    ttl_s: int,
    cache_signature: str,
) -> dict | None:
    row = conn.execute(
        """
        SELECT status, score_delta, hard_fail, reason, evidence_json, checked_at
        FROM validation_results
        WHERE candidate_id = ? AND check_type = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (candidate_id, check_type),
    ).fetchone()
    if not row:
        return None
    status = str(row[0])
    score_delta = float(row[1] or 0.0)
    hard_fail = bool(int(row[2] or 0))
    reason = str(row[3] or '')
    evidence_json = str(row[4] or '')
    checked_at_raw = str(row[5] or '')
    if not checked_at_raw:
        return None
    try:
        checked_at = dt.datetime.fromisoformat(checked_at_raw)
    except ValueError:
        return None
    age_s = max(0.0, (dt.datetime.now() - checked_at).total_seconds())
    if age_s > max(1, ttl_s):
        return None
    try:
        evidence = json.loads(evidence_json) if evidence_json else {}
    except json.JSONDecodeError:
        return None
    if not isinstance(evidence, dict):
        return None
    if evidence.get('_cache_signature') != cache_signature:
        return None
    return {
        'status': status,
        'score_delta': score_delta,
        'hard_fail': hard_fail,
        'reason': reason,
        'evidence': evidence,
    }


async def run_single_job(
    *,
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    spec: ValidationJobSpec,
    runner: Callable[[str, argparse.Namespace], dict],
    db_lock: InstrumentedLock | asyncio.Lock,
    semaphore: AdaptiveSemaphore | asyncio.Semaphore,
    on_complete: Callable[[str], Awaitable[None]] | None = None,
) -> None:
    async with semaphore:
        attempt = 0
        started_at = ndb.now_iso()
        spec_meta = CHECK_SPECS.get(spec.check_type)
        cache_eligible = bool(
            getattr(args, 'cheap_cache', True)
            and spec_meta is not None
            and spec_meta.tier == 'cheap'
        )
        cache_signature = (
            cheap_check_cache_signature(spec.check_type, args)
            if cache_eligible
            else ''
        )
        while True:
            attempt += 1
            cached_result: dict | None = None
            async with db_lock:
                if bool(getattr(args, 'track_job_lifecycle', True)):
                    ndb.update_validation_job(
                        conn,
                        job_id=spec.job_id,
                        status='running',
                        attempt_count=attempt,
                        started_at=started_at,
                        finished_at=None,
                        last_error='',
                    )
                if cache_eligible:
                    cached_result = load_cached_validation_result(
                        conn,
                        candidate_id=spec.candidate_id,
                        check_type=spec.check_type,
                        ttl_s=max(1, int(getattr(args, 'cheap_cache_ttl_s', 3600))),
                        cache_signature=cache_signature,
                    )
                if bool(getattr(args, 'track_job_lifecycle', True)):
                    conn.commit()

            try:
                if cached_result is not None:
                    status = str(cached_result['status'])
                    hard_fail = bool(cached_result['hard_fail'])
                    score_delta = float(cached_result['score_delta'])
                    reason = str(cached_result['reason'])
                    evidence = dict(cached_result.get('evidence') or {})
                    evidence['_cache_source'] = 'reused'
                else:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(runner, spec.candidate_name, args),
                        timeout=args.timeout_s,
                    )
                    status = str(result['status'])
                    hard_fail = bool(result['hard_fail'])
                    score_delta = float(result['score_delta'])
                    reason = str(result['reason'])
                    evidence = dict(result['evidence'])
                    if cache_eligible:
                        evidence['_cache_signature'] = cache_signature
                        evidence['_cache_source'] = 'live'

                async with db_lock:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=spec.candidate_id,
                        run_id=spec.run_id,
                        check_type=spec.check_type,
                        status=status,
                        score_delta=score_delta,
                        hard_fail=hard_fail,
                        reason=reason,
                        evidence=evidence,
                    )
                    ndb.update_validation_job(
                        conn,
                        job_id=spec.job_id,
                        status='success',
                        attempt_count=attempt,
                        started_at=started_at,
                        finished_at=ndb.now_iso(),
                        last_error='',
                    )
                    conn.commit()
                if on_complete is not None:
                    await on_complete('success')
                return
            except Exception as exc:  # noqa: BLE001
                err = f'{type(exc).__name__}: {exc}'
                should_retry = attempt <= args.max_retries
                if should_retry:
                    if bool(getattr(args, 'track_job_lifecycle', True)):
                        async with db_lock:
                            ndb.update_validation_job(
                                conn,
                                job_id=spec.job_id,
                                status='pending',
                                attempt_count=attempt,
                                started_at=started_at,
                                finished_at=None,
                                last_error=err,
                            )
                            conn.commit()
                    await asyncio.sleep((args.retry_backoff_ms / 1000.0) * attempt)
                    continue

                async with db_lock:
                    ndb.add_validation_result(
                        conn,
                        candidate_id=spec.candidate_id,
                        run_id=spec.run_id,
                        check_type=spec.check_type,
                        status='error',
                        score_delta=-5.0,
                        hard_fail=False,
                        reason='validator_execution_error',
                        evidence={'error': err},
                    )
                    ndb.update_validation_job(
                        conn,
                        job_id=spec.job_id,
                        status='fail',
                        attempt_count=attempt,
                        started_at=started_at,
                        finished_at=ndb.now_iso(),
                        last_error=err,
                    )
                    conn.commit()
                if on_complete is not None:
                    await on_complete('fail')
                return


def summarize_run(conn: sqlite3.Connection, run_id: int) -> dict:
    rows = conn.execute(
        """
        SELECT status, COUNT(*)
        FROM validation_jobs
        WHERE run_id = ?
        GROUP BY status
        ORDER BY status
        """,
        (run_id,),
    ).fetchall()
    summary = {'total_jobs': 0, 'status_counts': {}}
    for status, count in rows:
        summary['status_counts'][str(status)] = int(count)
        summary['total_jobs'] += int(count)
    return summary


def summarize_results_by_tier(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    cheap_checks: list[str],
    expensive_checks: list[str],
) -> dict[str, dict[str, int]]:
    rows = conn.execute(
        """
        SELECT check_type, status, COUNT(*)
        FROM validation_results
        WHERE run_id = ?
        GROUP BY check_type, status
        """,
        (run_id,),
    ).fetchall()

    cheap_set = set(cheap_checks)
    expensive_set = set(expensive_checks)
    summary = {
        'cheap': {'pass': 0, 'warn': 0, 'fail': 0, 'error': 0},
        'expensive': {'pass': 0, 'warn': 0, 'fail': 0, 'error': 0},
    }
    for check_type, status, count in rows:
        status_key = str(status).strip().lower()
        if status_key not in {'pass', 'warn', 'fail', 'error'}:
            status_key = 'error'
        check = str(check_type)
        if check in cheap_set:
            summary['cheap'][status_key] += int(count)
        elif check in expensive_set:
            summary['expensive'][status_key] += int(count)
    return summary


def summarize_cache_usage(conn: sqlite3.Connection, run_id: int) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT evidence_json
        FROM validation_results
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchall()
    summary = {'reused': 0, 'live': 0, 'none': 0}
    for row in rows:
        evidence_json = str(row[0] or '')
        if not evidence_json:
            summary['none'] += 1
            continue
        try:
            evidence = json.loads(evidence_json)
        except json.JSONDecodeError:
            summary['none'] += 1
            continue
        if not isinstance(evidence, dict):
            summary['none'] += 1
            continue
        source = str(evidence.get('_cache_source') or '').strip().lower()
        if source == 'reused':
            summary['reused'] += 1
        elif source == 'live':
            summary['live'] += 1
        else:
            summary['none'] += 1
    return summary


def mark_candidates_checked(conn: sqlite3.Connection, rows: list[CandidateRow], actor: str) -> None:
    ts = ndb.now_iso()
    for row in rows:
        if row.state == 'checked':
            continue
        conn.execute(
            """
            UPDATE candidates
            SET state = ?, status = ?, state_updated_at = ?
            WHERE id = ?
            """,
            ('checked', 'checked', ts, row.candidate_id),
        )
        conn.execute(
            """
            INSERT INTO state_transitions(candidate_id, from_state, to_state, actor, note, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (row.candidate_id, row.state, 'checked', actor, 'async validation completed', ts),
        )


def demote_checked_candidates_with_validation_failures(
    conn: sqlite3.Connection,
    *,
    actor: str,
) -> int:
    ts = ndb.now_iso()
    rows = conn.execute(
        """
        SELECT c.id, c.state
        FROM candidates c
        WHERE c.state = 'checked'
          AND (
            EXISTS (
                SELECT 1
                FROM validation_results vr
                WHERE vr.candidate_id = c.id
                  AND COALESCE(vr.hard_fail, 0) = 1
            )
            OR EXISTS (
                SELECT 1
                FROM validation_results vr
                WHERE vr.candidate_id = c.id
                  AND vr.check_type IN ('domain', 'web', 'app_store', 'package', 'social')
                  AND vr.status IN ('fail', 'error')
            )
          )
        """
    ).fetchall()
    demoted = 0
    for row in rows:
        candidate_id = int(row[0])
        from_state = str(row[1] or '')
        conn.execute(
            """
            UPDATE candidates
            SET state = ?, status = ?, rejection_reason = ?, state_updated_at = ?
            WHERE id = ?
            """,
            ('rejected_validation', 'rejected', 'validation_failed', ts, candidate_id),
        )
        conn.execute(
            """
            INSERT INTO state_transitions(candidate_id, from_state, to_state, actor, note, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                candidate_id,
                from_state,
                'rejected_validation',
                actor,
                'demoted due to validation hard-fail or expensive check fail/error',
                ts,
            ),
        )
        demoted += 1
    return demoted


async def orchestrate(args: argparse.Namespace) -> int:
    started_monotonic = time.monotonic()
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        checks, flags = select_checks(args)
    except ValueError as exc:
        print(str(exc))
        return 1
    if not checks:
        print(
            f'No checks selected after tier filter '
            f'(pipeline={flags.pipeline_version} tier={flags.validation_tier}).'
        )
        return 0

    states = parse_csv_set(args.state_filter)
    memory_db_path = Path(args.memory_db).expanduser() if str(args.memory_db or '').strip() else None
    memory_policy_signature = exclusion_memory_policy_signature(args=args, checks=checks, flags=flags)
    min_concurrency = max(1, int(getattr(args, 'min_concurrency', 2)))
    max_concurrency = max(min_concurrency, int(getattr(args, 'max_concurrency', 24)))
    initial_concurrency = max(min_concurrency, min(max_concurrency, max(1, int(args.concurrency))))

    sqlite_busy_timeout_ms = max(0, int(args.sqlite_busy_timeout_ms))
    with ndb.open_connection(db_path, busy_timeout_ms=sqlite_busy_timeout_ms, wal=True) as conn:
        ndb.ensure_schema(conn, busy_timeout_ms=sqlite_busy_timeout_ms, wal=True)
        memory_conn: sqlite3.Connection | None = None
        memory_prefilter_count = 0
        if memory_db_path is not None:
            memory_db_path.parent.mkdir(parents=True, exist_ok=True)
            memory_conn = ndb.open_connection(memory_db_path, busy_timeout_ms=sqlite_busy_timeout_ms, wal=True)
            ensure_exclusion_memory_schema(memory_conn)
        while True:
            candidates = load_candidates(conn, states, args.candidate_limit)
            if not candidates:
                if memory_conn is not None and memory_prefilter_count > 0:
                    emit_stage_event(
                        args.stage_events,
                        'memory_prefilter',
                        memory_db=str(memory_db_path),
                        policy_signature=memory_policy_signature,
                        excluded_count=memory_prefilter_count,
                        candidate_count_after_prefilter=0,
                    )
                print('No candidates found for selected state filter.')
                if memory_conn is not None:
                    memory_conn.close()
                return 0
            if memory_conn is None:
                break
            excluded_names = load_memory_excluded_names(
                memory_conn,
                names=[row.name_display for row in candidates],
                scope=args.scope,
                gate=args.gate,
                policy_signature=memory_policy_signature,
            )
            if not excluded_names:
                break
            memory_rows = [
                row for row in candidates if ndb.normalize_name(row.name_display) in excluded_names
            ]
            if not memory_rows:
                break
            mark_candidates_memory_excluded(
                conn,
                memory_rows,
                actor='naming_validate_memory',
                note=f'memory exclusion match signature={memory_policy_signature}',
            )
            conn.commit()
            memory_prefilter_count += len(memory_rows)

        cheap_checks = [check for check in checks if CHECK_SPECS[check].tier == 'cheap']
        expensive_checks = [check for check in checks if CHECK_SPECS[check].tier == 'expensive']
        tiered_split = bool(
            flags.v3_enabled
            and flags.validation_tier == 'all'
            and cheap_checks
            and expensive_checks
        )
        finalist_recommendations = parse_csv_set(args.finalist_recommendations)
        expensive_finalists = (
            select_expensive_finalists(
                candidates,
                recommendations=finalist_recommendations,
                limit=max(1, args.expensive_finalist_limit),
            )
            if tiered_split
            else candidates
        )
        expensive_ids = {row.candidate_id for row in expensive_finalists}
        job_plan: list[tuple[CandidateRow, str]] = []
        if tiered_split:
            for row in candidates:
                for check_type in cheap_checks:
                    job_plan.append((row, check_type))
            for row in expensive_finalists:
                for check_type in expensive_checks:
                    job_plan.append((row, check_type))
        else:
            for row in candidates:
                for check_type in checks:
                    job_plan.append((row, check_type))

        if memory_conn is not None:
            emit_stage_event(
                args.stage_events,
                'memory_prefilter',
                memory_db=str(memory_db_path),
                policy_signature=memory_policy_signature,
                excluded_count=memory_prefilter_count,
                candidate_count_after_prefilter=len(candidates),
            )

        emit_stage_event(
            args.stage_events,
            'candidate_load',
            candidate_count=len(candidates),
            checks=checks,
            cheap_checks=cheap_checks,
            expensive_checks=expensive_checks,
            tiered_split=tiered_split,
            expensive_finalist_count=len(expensive_finalists),
            expensive_finalist_ids=sorted(expensive_ids)[:20],
            validation_tier=flags.validation_tier,
            cheap_tm_enabled=bool(args.cheap_trademark_screen),
            cheap_tm_blocklist_size=len(CHEAP_TRADEMARK_BLOCKLIST),
        )

        run_id = ndb.create_run(
            conn,
            source_path=str(db_path),
            scope=args.scope,
            gate_mode=args.gate,
            variation_profile='validator_async',
            status='running',
            config={
                'checks': checks,
                'candidate_limit': args.candidate_limit,
                'concurrency': initial_concurrency,
                'min_concurrency': min_concurrency,
                'max_concurrency': max_concurrency,
                'max_retries': args.max_retries,
                'state_filter': states,
                'pipeline_version': flags.pipeline_version,
                'v3_enabled': flags.v3_enabled,
                'validation_tier': flags.validation_tier,
                'cheap_checks': cheap_checks,
                'expensive_checks': expensive_checks,
                'tiered_split': tiered_split,
                'expensive_finalist_limit': int(args.expensive_finalist_limit),
                'finalist_recommendations': finalist_recommendations,
                'planned_job_count': len(job_plan),
                'cheap_trademark_screen': bool(args.cheap_trademark_screen),
                'cheap_trademark_fail_threshold': int(args.cheap_trademark_fail_threshold),
                'cheap_trademark_warn_threshold': int(args.cheap_trademark_warn_threshold),
                'cheap_trademark_blocklist_size': len(CHEAP_TRADEMARK_BLOCKLIST),
                'cheap_cache': bool(args.cheap_cache),
                'cheap_cache_ttl_s': int(args.cheap_cache_ttl_s),
                'memory_db': str(memory_db_path) if memory_db_path is not None else '',
                'memory_ttl_days': int(args.memory_ttl_days),
                'sqlite_busy_timeout_ms': int(sqlite_busy_timeout_ms),
                'track_job_lifecycle': bool(args.track_job_lifecycle),
                'memory_policy_signature': memory_policy_signature if memory_conn is not None else '',
                'memory_prefilter_count': int(memory_prefilter_count),
            },
            summary={},
        )
        conn.commit()

        jobs: list[ValidationJobSpec] = []
        for row, check_type in job_plan:
            job_id = ndb.create_validation_job(
                conn,
                run_id=run_id,
                candidate_id=row.candidate_id,
                check_type=check_type,
                status='pending',
            )
            jobs.append(
                ValidationJobSpec(
                    job_id=job_id,
                    run_id=run_id,
                    candidate_id=row.candidate_id,
                    candidate_name=row.name_display,
                    candidate_prev_state=row.state,
                    check_type=check_type,
                )
            )
        conn.commit()

        db_lock = InstrumentedLock()
        semaphore = AdaptiveSemaphore(
            initial_concurrency=initial_concurrency,
            min_concurrency=min_concurrency,
            max_concurrency=max_concurrency,
        )
        recent_outcomes: deque[str] = deque(maxlen=50)
        concurrency_adjustments: list[dict[str, object]] = []
        progress_lock = asyncio.Lock()
        progress_state = ProgressState(
            total_jobs=len(jobs),
            started_at_monotonic=time.monotonic(),
            last_report_monotonic=time.monotonic(),
        )

        def format_progress_line(*, force: bool = False) -> str:
            elapsed = max(0.001, time.monotonic() - progress_state.started_at_monotonic)
            rate = progress_state.completed_jobs / elapsed
            remaining = max(0, progress_state.total_jobs - progress_state.completed_jobs)
            eta_seconds = remaining / max(rate, 0.001)
            percent = (progress_state.completed_jobs / max(1, progress_state.total_jobs)) * 100.0
            label = 'progress_final' if force else 'progress'
            return (
                f'async_validation_{label} completed={progress_state.completed_jobs}/{progress_state.total_jobs} '
                f'({percent:.1f}%) success={progress_state.success_jobs} fail={progress_state.failed_jobs} '
                f'rate={rate:.2f}jobs/s eta={eta_seconds:.1f}s'
            )

        async def on_job_complete(outcome: str) -> None:
            async with progress_lock:
                progress_state.completed_jobs += 1
                if outcome == 'success':
                    progress_state.success_jobs += 1
                else:
                    progress_state.failed_jobs += 1
                recent_outcomes.append(outcome)

                if len(recent_outcomes) >= 50 and progress_state.completed_jobs % 50 == 0:
                    target_concurrency, error_rate = calculate_adaptive_concurrency_target(
                        outcomes=list(recent_outcomes),
                        current_concurrency=semaphore.current_limit,
                        min_concurrency=min_concurrency,
                        max_concurrency=max_concurrency,
                    )
                    if target_concurrency != semaphore.current_limit:
                        previous = semaphore.current_limit
                        updated = await semaphore.adjust(target_concurrency)
                        adjustment = {
                            'completed_jobs': int(progress_state.completed_jobs),
                            'error_rate': round(error_rate, 4),
                            'from': int(previous),
                            'to': int(updated),
                        }
                        concurrency_adjustments.append(adjustment)
                        emit_stage_event(
                            args.stage_events,
                            'concurrency_adjust',
                            **adjustment,
                            window_size=50,
                        )

                if not args.progress:
                    return
                now = time.monotonic()
                due_by_count = progress_state.completed_jobs % max(1, args.progress_every) == 0
                due_by_time = (now - progress_state.last_report_monotonic) >= max(0.1, args.progress_interval_s)
                first_job = progress_state.completed_jobs == 1
                last_job = progress_state.completed_jobs >= progress_state.total_jobs
                if first_job or last_job or due_by_count or due_by_time:
                    print(format_progress_line(force=last_job), flush=True)
                    progress_state.last_report_monotonic = now

        if args.progress:
            print(
                f'async_validation_start run_id={run_id} candidates={len(candidates)} '
                f'jobs={len(jobs)} checks={",".join(checks)} '
                f'pipeline={flags.pipeline_version} v3_enabled={flags.v3_enabled} '
                f'tier={flags.validation_tier} tiered_split={tiered_split} '
                f'expensive_finalists={len(expensive_finalists)} '
                f'concurrency_initial={initial_concurrency} '
                f'concurrency_range={min_concurrency}-{max_concurrency} '
                f'cheap_cache={args.cheap_cache} ttl={args.cheap_cache_ttl_s}s '
                f'memory_db={str(memory_db_path) if memory_db_path is not None else "disabled"} '
                f'memory_prefilter={memory_prefilter_count}',
                flush=True,
            )

        tasks = []
        for job in jobs:
            runner = CHECK_RUNNERS[job.check_type]
            tasks.append(
                asyncio.create_task(
                    run_single_job(
                        conn=conn,
                        args=args,
                        spec=job,
                        runner=runner,
                        db_lock=db_lock,
                        semaphore=semaphore,
                        on_complete=on_job_complete,
                    )
                )
            )
        await asyncio.gather(*tasks)

        mark_candidates_checked(conn, candidates, actor='naming_validate_async')
        demoted_validation_count = demote_checked_candidates_with_validation_failures(
            conn,
            actor='naming_validate_async',
        )
        summary = summarize_run(conn, run_id)
        tier_summary = summarize_results_by_tier(
            conn,
            run_id=run_id,
            cheap_checks=cheap_checks,
            expensive_checks=expensive_checks,
        )
        cache_summary = summarize_cache_usage(conn, run_id)
        memory_exclusions_upserted = 0
        memory_hard_fail_count = 0
        if memory_conn is not None:
            hard_fail_reasons = collect_hard_fail_reasons_by_name(conn, run_id=run_id)
            memory_hard_fail_count = len(hard_fail_reasons)
            memory_exclusions_upserted = upsert_exclusion_memory(
                memory_conn,
                exclusions=hard_fail_reasons,
                scope=args.scope,
                gate=args.gate,
                policy_signature=memory_policy_signature,
                ttl_days=int(args.memory_ttl_days),
            )
            emit_stage_event(
                args.stage_events,
                'memory_update',
                memory_db=str(memory_db_path),
                policy_signature=memory_policy_signature,
                hard_fail_name_count=memory_hard_fail_count,
                exclusions_upserted=memory_exclusions_upserted,
                ttl_days=int(args.memory_ttl_days),
            )
        emit_stage_event(
            args.stage_events,
            'cheap_gate',
            result_counts=tier_summary['cheap'],
            dropoff_count=tier_summary['cheap'].get('fail', 0) + tier_summary['cheap'].get('error', 0),
            checks=cheap_checks,
            cache=cache_summary,
        )
        emit_stage_event(
            args.stage_events,
            'expensive_gate',
            result_counts=tier_summary['expensive'],
            dropoff_count=tier_summary['expensive'].get('fail', 0) + tier_summary['expensive'].get('error', 0),
            checks=expensive_checks,
            finalist_count=len(expensive_finalists),
        )
        lock_metrics = db_lock.snapshot()
        adaptive_summary = {
            'initial': int(initial_concurrency),
            'final': int(semaphore.current_limit),
            'min': int(min_concurrency),
            'max': int(max_concurrency),
            'adjustment_count': int(len(concurrency_adjustments)),
            'adjustments': concurrency_adjustments[:20],
        }
        latency_ms = int((time.monotonic() - started_monotonic) * 1000)
        emit_stage_event(
            args.stage_events,
            'complete',
            run_id=run_id,
            candidate_count=len(candidates),
            planned_job_count=len(job_plan),
            executed_job_count=len(jobs),
            status_counts=summary.get('status_counts', {}),
            tier_result_counts=tier_summary,
            cache_summary=cache_summary,
            memory_prefilter_count=memory_prefilter_count,
            memory_hard_fail_count=memory_hard_fail_count,
            memory_exclusions_upserted=memory_exclusions_upserted,
            latency_ms=latency_ms,
            concurrency_initial=adaptive_summary['initial'],
            concurrency_final=adaptive_summary['final'],
            concurrency_adjustment_count=adaptive_summary['adjustment_count'],
            demoted_validation_count=demoted_validation_count,
            **lock_metrics,
        )
        conn.execute(
            """
            UPDATE naming_runs
            SET status = ?, summary_json = ?
            WHERE id = ?
            """,
            (
                'completed',
                json.dumps(
                    {
                        **summary,
                        'tier_result_counts': tier_summary,
                        'cache_summary': cache_summary,
                        'tiered_split': tiered_split,
                        'expensive_finalist_count': len(expensive_finalists),
                        'memory_prefilter_count': memory_prefilter_count,
                        'memory_hard_fail_count': memory_hard_fail_count,
                        'memory_exclusions_upserted': memory_exclusions_upserted,
                        'adaptive_concurrency': adaptive_summary,
                        'demoted_validation_count': demoted_validation_count,
                        **lock_metrics,
                    },
                    ensure_ascii=False,
                ),
                run_id,
            ),
        )
        conn.commit()
        if memory_conn is not None:
            memory_conn.close()

    print(
        f'async_validation_complete run_id={run_id} candidates={len(candidates)} '
        f'jobs={len(jobs)} db={db_path}'
    )
    print(
        'run_summary='
        + json.dumps(
            {
                **summary,
                'tier_result_counts': tier_summary,
                'cache_summary': cache_summary,
                'tiered_split': tiered_split,
                'expensive_finalist_count': len(expensive_finalists),
                'memory_prefilter_count': memory_prefilter_count,
                'memory_hard_fail_count': memory_hard_fail_count,
                'memory_exclusions_upserted': memory_exclusions_upserted,
                'adaptive_concurrency': adaptive_summary,
                'demoted_validation_count': demoted_validation_count,
                **lock_metrics,
            },
            ensure_ascii=False,
        )
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(orchestrate(args))


if __name__ == '__main__':
    raise SystemExit(main())
