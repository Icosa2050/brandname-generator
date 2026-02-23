# OpenRouter Workflow Analysis: Validation Parallelization + Observability

Date: 2026-02-21
Scope: `scripts/branding/naming_campaign_runner.py`, `scripts/branding/naming_validate_async.py`, `scripts/branding/name_generator.py`, OpenRouter ideation loop.

## Executive Summary
The repository already has strong foundations:
- Campaign-level sharding (`--shard-id/--shard-count`) exists for sweep combos.
- Validator is asynchronous and concurrent (`asyncio` + semaphore + thread offload).
- Structured stage events are emitted by generator and validator.

Primary gap: progress visibility is mostly hidden because child process output is redirected to log files only. This matches the current pain point: you need `tail -f` to see real progress.

Secondary opportunity: validation parallelism can scale further, but the current SQLite + shared-state model requires explicit claim/shard strategy to avoid duplicate work and lock contention.

## Current Pipeline Notes

### What is already good
1. Active LLM ideation stage with retries, model catalog check, cache, budget/time caps.
2. Validator supports cheap/expensive tiers, result caching, and structured run summaries.
3. Campaign progress CSV captures per-run aggregates (cost, shortlist deltas, validator summary).

### What is limiting throughput/visibility
1. Campaign runner suppresses child stdout/stderr into per-stage log files (`run_cmd` writes only to file).
2. Validator progress lines and stage events exist, but are only visible in validator logs unless tailed.
3. SQLite is used with default `journal_mode=delete` in observed run, which is weaker for multi-process write concurrency.
4. Validator candidate selection is state-based (`new`) without a lease/claim mechanism, so naive multi-process validator runs can overlap on same candidates.

## Weighted Options
Scale used:
- Impact: 1-5
- Effort: 1-5 (higher = more effort)
- Risk: 1-5 (higher = higher regression/operational risk)
- Priority Score = `(Impact*2) - Effort - Risk`

### Option A: Live Progress Streaming from Campaign Runner
- Change: add a streaming mode in `run_cmd` that tees child output to log file and stdout (at least for progress/stage_event lines).
- Impact: 5
- Effort: 2
- Risk: 1
- Priority Score: 7
- Why: immediate UX fix; no algorithmic risk; fastest path to remove `tail -f` dependency.

### Option B: Unified Event Bus File (`campaign_events.jsonl`)
- Change: normalize and append campaign/generator/validator events into one timeline file + optional stdout summaries every N seconds.
- Impact: 5
- Effort: 3
- Risk: 1
- Priority Score: 6
- Why: best observability foundation for local runs + CI artifacts + dashboards.

### Option C: Validator Worker Sharding by Candidate Partition
- Change: add validator shard args (e.g. `--candidate-shard-id/--candidate-shard-count`) with deterministic SQL partition (`id % shard_count = shard_id`) or claim lease.
- Impact: 4
- Effort: 4
- Risk: 3
- Priority Score: 1
- Why: materially increases throughput on expensive checks, but needs careful dedupe/coordination.

### Option D: In-process Validator Write-path Optimization
- Change: reduce DB lock/commit frequency in `run_single_job` via buffered writer queue or batched commits.
- Impact: 3
- Effort: 3
- Risk: 3
- Priority Score: 0
- Why: helps CPU-heavy/cheap checks; less benefit when bottleneck is network I/O.

### Option E: Remove Duplicate Expensive Checking Between Generator and Validator
- Change: run generator in cheap/degraded mode and keep validator as authoritative expensive checker, or invert that ownership.
- Impact: 4
- Effort: 2
- Risk: 2
- Priority Score: 4
- Why: reduces redundant network calls and runtime cost; largely config-level.

### Option F: Multi-DB Per-shard then Merge
- Change: each shard validates in its own DB then merges results.
- Impact: 4
- Effort: 5
- Risk: 4
- Priority Score: -1
- Why: highest throughput headroom, but highest complexity and data-integrity burden.

## Recommended Sequence
1. Option A first (fastest win for visibility).
2. Option E next (reduce waste before scaling complexity).
3. Option B to formalize observability and make CI/operator consumption easy.
4. Option C for parallel validator scaling (with explicit anti-overlap design).
5. Option D only if profiling still shows DB write-path as material bottleneck.
6. Option F only if you outgrow single-DB architecture.

## Concrete Sharding Design Recommendation
Preferred design: deterministic candidate partition in validator.
- Add args: `--candidate-shard-id`, `--candidate-shard-count`.
- Apply partition in candidate loader query (same `state_filter`, each shard sees disjoint candidate subset).
- Keep each shard writing results to same DB only after enabling WAL + appropriate busy timeout.
- Add shard metadata into run config and `run_summary` for traceability.

Why this over lease-based claiming first:
- Lower coordination complexity.
- No new transient states needed in candidate table.
- Easier to reason about reproducibility and replay.

## Logging/Progress Recommendation
Minimum viable improvement:
- Add `--live-progress` to campaign runner.
- During generator/validator subprocess execution, stream selected lines containing:
  - `stage_event=`
  - `async_validation_progress`
  - `run_summary=`
- Keep full logs on disk unchanged.

This preserves existing artifacts and unlocks real-time visibility in terminal/CI logs.

## Task Manager Initialization Status
- Local Task Manager backlog initialized at:
  - `.taskmaster/tasks/tasks.json`
- Important tool behavior note:
  - `task-master-ai parse_prd` and `expand_task` timed out repeatedly in this environment.
  - Local tasks are usable via file-targeted operations (e.g. `next_task` with `file=`).

## Evidence Run (local smoke)
A constrained run was executed successfully with fixture ideation and no external checks:
- Output root: `/tmp/brandname_analysis_run`
- Campaign summary: `/tmp/brandname_analysis_run/campaign_summary.json`
- Progress CSV: `/tmp/brandname_analysis_run/campaign_progress.csv`
- Validator logs include rich progress and stage events, but those are not surfaced live by campaign runner.

