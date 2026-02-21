# OpenRouter Ideation Loop Plan (2026-02-21)

## Objective
Upgrade naming generation from fallback-only AI ingestion to an active ideation stage that improves shortlist diversity and quality, while keeping deterministic legal/confusion gates unchanged.

## Status Refresh (2026-02-21)
Implemented in current branch:
1. Active LLM ideation stage exists in campaign runner with `openrouter_http|fixture|pal` modes.
2. Generator receives ideation artifact through `--llm-input` in campaign runs.
3. Seed family is active in campaign defaults (`--generator-seeds` default non-empty).
4. `quality-first` is default-on in campaign and in v3 smoke/full runner.
5. Family/quota parity assertion is enforced in campaign runner and in `test_naming_pipeline_v3.sh`.
6. Dynamic constraints feedback loop is active (`*_dynamic_constraints.json` emitted and reused).
7. A/B report generation exists (`ab_report.json`, `ab_report.md`).
8. OpenRouter compatibility fallback exists (`json_schema+require_parameters` -> `json_object` -> plain chat).
9. Optional context packet support exists (`--llm-context-file`, example file in `docs/branding/llm_context.example.json`).

Remaining or follow-up items:
1. Increase automated coverage for `name_generator.py` hot paths (quality-first thresholds, seed/quota behavior, fallback ingestion retries).
2. Keep model/provider behavior validated with live canary due provider-side schema/routing changes.

## Baseline Findings (Historical, pre-implementation)
1. LLM is fallback-only, not active generation.
   - `scripts/branding/name_generator.py` (`--llm-input` fallback path, parser load/merge)
   - `scripts/branding/name_ideation_ingest.py` (prompt/render + ingest, no model call)
   - `scripts/branding/naming_campaign_runner.py` (no LLM ideation call or `--llm-input`)
2. Seed family is mostly inert in campaign flow.
   - `scripts/branding/name_generator.py` (`--seeds` default empty; seed family depends on seeds)
   - `scripts/branding/naming_campaign_runner.py` (includes `seed` family but no seeds passed)
3. `quality-first` is opt-in and not enabled in campaign/v3 runner defaults.
4. Novelty is used for early stop only, not for adaptive generation controls.
5. V3 runner config mismatch exists (`stem` quota present while family list omits `stem`).
6. Source corpus is static/small per run (`docs/branding/source_inputs_v2.csv`).

## Non-Negotiable Constraints
1. Deterministic risk gates stay authoritative.
2. LLM output never asserts availability; all availability checks remain pipeline-owned.
3. LLM stage must fail open to deterministic generation.
4. Every LLM candidate needs provenance (`model`, `provider`, `prompt_id`, `prompt_hash`, `run_id`).

## Execution Architecture
## 1) Active LLM Stage in Campaign Runner
Add a pre-generation stage in `scripts/branding/naming_campaign_runner.py`:
1. Build context from previous run artifacts (`seen_shortlist_names`, fail reasons, saturation stats).
2. Request candidate batch from provider.
3. Write structured artifact: `runs/<run_id>_llm_candidates.json`.
4. Pass artifact to `name_generator.py --llm-input=<file>`.
5. Continue existing deterministic generation/screening.

Proposed runner flags:
1. `--llm-ideation-enabled` (default false in phase 0)
2. `--llm-provider` (`pal` or `openrouter_http`)
3. `--llm-model` (phase 0 single model; must be validated against provider catalog)
4. `--llm-rounds` (default 2)
5. `--llm-candidates-per-round` (default 20)
6. `--llm-max-call-latency-ms` (default 8000, per call)
7. `--llm-stage-timeout-ms` (default 30000, whole stage)
8. `--llm-max-usd-per-run`
9. `--llm-cache-dir`
10. `--llm-strict-json` (default true)

## 2) OpenRouter Integration Spec
Provider mode `openrouter_http`:
1. Auth: `Authorization: Bearer $OPENROUTER_API_KEY`; fail-fast if missing.
2. Endpoint: `POST https://openrouter.ai/api/v1/chat/completions`.
3. Request format: OpenAI-compatible payload with:
   - `model`
   - `messages`
   - `response_format` JSON schema (strict mode)
   - provider routing option enforcing strict parameter adherence.
4. Error handling:
   - `401/403`: disable LLM stage for run, continue deterministic.
   - `429`: retry with exponential backoff + jitter (max 3).
   - `5xx`: retry once then skip stage.
   - timeout: skip call and continue.
5. Availability probe:
   - required model check at run start via `GET https://openrouter.ai/api/v1/models`; if unavailable, fallback immediately.
   - no hardcoded model is trusted without catalog verification.

Provider mode `pal`:
1. Phase 0: fallback path only when `openrouter_http` is unavailable.
2. PAL response still passes the same local schema validation and fallback parser pipeline.
3. Timeout/retry accounting is identical to `openrouter_http`.

## 3) Model Strategy (Phased)
Phase 0:
1. Single creative model:
   - preferred: `mistralai/mistral-small-creative` when present in provider catalog.
   - fallback: first validated small creative-capable model from the live catalog.
2. Local JSON validation/repair pipeline (no extra model in hot path).

Phase 1 (conditional):
1. Add normalizer model only if local parse/repair failure >10% of LLM calls.
2. Keep critic model (`anthropic/claude-opus-4.6`) as offline plan-review tool only.

## 4) Prompt Program and Diversity Control
Prompt modes rotate deterministically by run index:
1. Phonetic mode: `smooth|crisp|balanced`.
2. Morphology mode: `blend|coined|hybrid`.
3. Semantic mode: `trust|precision|clarity|neutral`.

Selection rule:
1. `mode_triplet = mode_grid[run_index % len(mode_grid)]`.
2. Each round uses different triplet; no random drift in phase 0.

Prompt contract (hard):
1. JSON only, schema-conformant.
2. lowercase latin letters only, length 5-12.
3. dynamic `banned_tokens` and `banned_prefixes`.
4. uniqueness constraint: no duplicate names; no same first-4 prefix in a round.
5. explicit ban on availability claims.

## 5) Adaptive Feedback Algorithm (Concrete)
Window:
1. trailing `N=5` runs (configurable).

Steps:
1. Read fail reasons from run logs and screened artifacts.
2. If a fail reason exceeds 20% in window:
   - extract token/prefix patterns from failed names.
   - add to `dynamic_constraints` (`banned_tokens`, `banned_prefixes`).
3. Compute shortlist prefix entropy and phonetic bucket concentration.
4. If entropy <2.5 bits or any bucket share >30%:
   - add top overrepresented prefixes to `banned_prefixes`.
5. Constraint ceiling:
   - max `banned_tokens=50`, max `banned_prefixes=30`; prune oldest first.
6. Persist constraints to `runs/<run_id>_dynamic_constraints.json`.
7. Constraints reset at campaign start by default (optional carry-over flag later).

## 6) Cost, Latency, and Caching Guardrails
1. Hard caps:
   - max calls/run
   - max estimated USD/run
   - max stage wall-clock
2. Cost accounting:
   - accumulate usage fields from each response (tokens in/out/total).
   - compute running USD estimate from model pricing config.
   - stop when next call is projected to exceed `--llm-max-usd-per-run`.
3. Cache key:
   - hash of `{model, prompt_text, schema_version, dynamic_constraints_hash}`.
4. `dynamic_constraints_hash` canonicalization:
   - serialize sorted constraint data with stable JSON encoding before hashing.
5. Cache TTL:
   - default 7 days, manual invalidate flag.
6. Telemetry events:
   - `llm_ideation_start`
   - `llm_ideation_call_ok`
   - `llm_ideation_call_retry`
   - `llm_ideation_budget_stop`
   - `llm_ideation_complete`

## 7) Quality Defaults and Config Hygiene
1. Enable `--quality-first` by default in campaign runner.
2. Fix v3 test runner family/quota mismatch:
   - either include `stem` in families or remove `stem` quota.
3. Add startup assertion:
   - quota keys must equal active families.

## A/B Test Plan
## Arms
1. A: baseline campaign (no active LLM stage).
2. B: LLM-active campaign (same deterministic gates).

## Randomization
1. Randomized block assignment:
   - block size 4, each block has `A,A,B,B` in random order.
2. Keep scope/gate/quota profile schedule identical across arms.

## Metrics
Primary:
1. `new_shortlist_count` per run.
2. cumulative `unique_shortlist_names` slope.
3. hard-fail ratio before expensive checks.

Secondary:
1. shortlist family entropy.
2. parser fallback rate.
3. llm timeout/retry rate.
4. cost per additional unique shortlist name.

## Sample Size and Decision Rule
1. Pilot: 10 runs/arm for smoke signal.
2. Decision set: minimum 30 runs/arm.
3. Statistical check: Mann-Whitney U on `new_shortlist_count`; report median difference + 95% bootstrap CI.
4. Go criteria:
   - median `new_shortlist_count` +25% or better,
   - hard-fail ratio increase <=10% relative,
   - timeout/failure <5%,
   - budget cap respected.

## File-Scoped Implementation Plan
1. `scripts/branding/naming_campaign_runner.py`
   - add LLM stage orchestration and flags.
   - wire `--llm-input`.
   - enable `--quality-first` default.
2. `scripts/branding/name_generator.py`
   - keep parser fallback, add llm-stage provenance events.
   - enforce strict event emission for llm source counts.
3. `scripts/branding/name_ideation_ingest.py`
   - reuse schema logic for active-stage validation.
   - expose parse-quality metrics.
4. `scripts/branding/test_naming_pipeline_v3.sh`
   - fix family/quota mismatch.
   - add optional llm fixture mode.
5. `docs/branding/name_generator_guide.md`
   - document new flags, A/B command recipes, rollback path.

## Definition of Done (Per Work Item)
1. LLM stage hook:
   - with flag off, no behavioral regression in existing run outputs.
   - with fixture input, llm candidates are ingested and traceable.
2. Parser/fallback tests:
   - fixtures: valid JSON, wrapped JSON, malformed JSON, empty payload, timeout simulation.
   - no unhandled exception path.
3. Quality-first default:
   - runner default on; configurable override remains.
4. Family/quota assertion:
   - startup failure with clear message on mismatch.
5. A/B report:
   - generated CSV + summary markdown with metrics and decision.
6. Adaptive feedback tests:
   - synthetic 5-run fixtures verify fail-threshold triggers, entropy triggers, and constraint pruning.

## Rollout
Phase 0:
1. Implement + fixture tests in offline/degraded mode.

Phase 1:
1. Canary with `--llm-ideation-enabled` for a subset of campaign runs.

Phase 2:
1. Expand after go criteria are met.

Phase 3:
1. Default-on with one-flag rollback.
