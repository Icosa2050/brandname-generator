---
owner: product
status: draft
last_validated: 2026-02-17
---

# Naming Pipeline V1 Execution Plan

## Goal
Build a repeatable funnel that can evaluate 50-500+ candidates, retain evidence, and produce legally defensible finalists for DE/CH first with optional global expansion.

## Current Baseline
- Candidate lake DB + dedupe/state transitions.
- Generator + external collision checks.
- AI ingest with provenance.
- Async validation jobs with persisted lifecycle and progress reporting.

## External Review Snapshot (PAL Gemini)
- Reviewed with `google/gemini-3-pro-preview` via PAL on 2026-02-17.
- Confirmed fan-out/fan-in architecture is correct for 50-500+ candidate processing.
- Recommended strict evidence retention for legal handover (timestamped source snapshots per check).
- Recommended explicit `open`/`needs_human_review` states to avoid false confidence when external data is missing.
- Recommended prioritizing trademark/confusion risk weighting over pure availability.

## Gate Model
- `pass`: clear automated evidence; no hard blockers.
- `fail`: hard blocker (for example exact collision or invalid base quality).
- `open`: incomplete/unknown external evidence due network/rate limits.
- `needs_human_review`: non-blocking but risky pattern (near similarity, cultural ambiguity, weak trust signals).

## Phase 1 (Next 1-2 days)
- Stabilize current scripts and schemas.
- Keep runs reproducible via fixture smoke tests.
- Require provenance on all ingested names.
- Enforce evidence retention in DB for every automated check.

## Phase 2 (Next 1-2 weeks)
- Add stronger risk model combining:
  - legal risk
  - collision risk
  - psych/comprehension risk
  - uncertainty penalty for `open` checks.
- Add registry/trademark pre-screen adapter layer (DPMA/IGE/EUIPO/TMview connectors + cache + rate limits).
- Add candidate funnel dashboards/queries: by risk tier, by unresolved checks, by source batch.

## Phase 3 (Later)
- Add semi-automated user testing package generation (DE/CH comprehension and trust tests).
- Add counsel handover export bundle (finalist + backup + evidence snapshots).
- Add active monitoring for near-name collisions after launch.

## Key Pitfalls to Avoid
- Treating exact-match-only as legal clearance.
- Overfitting to `.com` while ignoring operational alternatives.
- Ignoring unresolved external checks (`open`) in final ranking.
- Advancing candidates without archived evidence trails.

## Mapping to TaskMaster
- `713.1`: naming brief and constraints.
- `713.2`: automated screening pipeline and deterministic quality gates.
- `713.3+`: batch generation, legal pre-screen, adversarial review, psych testing, legal handover, final memo.
