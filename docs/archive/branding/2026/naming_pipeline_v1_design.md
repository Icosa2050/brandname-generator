---
owner: product
status: draft
last_validated: 2026-02-16
task: 715.1
---

# Naming Pipeline V1 Design

## Objective
Build a long-running naming pipeline that can generate, validate, and track large candidate pools (5k-50k over time), then produce evidence-backed top-50 shortlists for psych testing and legal review.

## Scope
- DACH-first brand search with optional global naming expansion.
- Deterministic validation checks as source of truth.
- AI-assisted ideation as optional candidate source channel.
- Persistent run history and candidate lifecycle tracking.

## Non-Goals (V1)
- Fully automated trademark clearance decisions.
- Real-time distributed worker infrastructure.
- Public-facing UI.

## Core Principles
- Deterministic checks decide pass/fail for availability/conflict gates.
- AI is used only for candidate ideation and optional linguistic suggestion.
- Every candidate must be traceable to its generation source.
- Unknown external check states are explicit and configurable (strict vs degraded mode).
- Final legal decision remains external counsel responsibility.

## System Components
1. Candidate Lake (SQLite)
- Canonical storage for candidates, provenance, check results, scores, and status transitions.

2. Generator Layer
- Deterministic generator (existing rules + multilingual variation packs).
- Optional AI ideation ingestion channel.
- Manual import channel for curated lists.

3. Validation Orchestrator
- Async worker loop that executes enabled validators in parallel.
- Rate limiting, retry/backoff, and cached result reuse.

4. Scoring & Gate Engine
- Applies hard-fail rules and soft penalties.
- Produces ranked top-N outputs with evidence links and reasons.

5. Reporting & Promotion Layer
- Exports top-50 bundle (CSV/JSON/Markdown).
- Tracks candidate progression to psych testing and legal review.

## Data Model (SQLite)

### Table: `naming_runs`
- `id` INTEGER PK
- `created_at` TEXT (ISO timestamp)
- `scope` TEXT (`dach|eu|global`)
- `gate_mode` TEXT (`strict|balanced`)
- `variation_profile` TEXT (`standard|expanded`)
- `config_json` TEXT (full run settings)
- `status` TEXT (`running|completed|failed|cancelled`)
- `summary_json` TEXT

### Table: `candidates`
- `id` INTEGER PK
- `name_display` TEXT
- `name_normalized` TEXT UNIQUE
- `first_seen_at` TEXT
- `last_seen_at` TEXT
- `current_score` REAL
- `current_risk` REAL
- `current_recommendation` TEXT
- `state` TEXT (`new|checked|shortlisted|psych_test|legal_review|approved|rejected`)
- `state_updated_at` TEXT

### Table: `candidate_sources`
- `id` INTEGER PK
- `candidate_id` INTEGER FK -> candidates.id
- `run_id` INTEGER FK -> naming_runs.id
- `source_type` TEXT (`rule|ai|manual|import`)
- `source_label` TEXT (e.g. model/provider name)
- `prompt_or_seed` TEXT
- `metadata_json` TEXT
- `created_at` TEXT

### Table: `validation_results`
- `id` INTEGER PK
- `candidate_id` INTEGER FK -> candidates.id
- `run_id` INTEGER FK -> naming_runs.id
- `check_type` TEXT
- `status` TEXT (`pass|fail|warn|unknown|skipped|error`)
- `score_delta` REAL
- `hard_fail` INTEGER (0/1)
- `reason` TEXT
- `evidence_json` TEXT
- `checked_at` TEXT
- `cache_expires_at` TEXT

### Table: `candidate_scores`
- `id` INTEGER PK
- `candidate_id` INTEGER FK -> candidates.id
- `run_id` INTEGER FK -> naming_runs.id
- `quality_score` REAL
- `risk_score` REAL
- `external_penalty` REAL
- `total_score` REAL
- `recommendation` TEXT
- `hard_fail` INTEGER (0/1)
- `reason` TEXT
- `created_at` TEXT

### Table: `state_transitions`
- `id` INTEGER PK
- `candidate_id` INTEGER FK -> candidates.id
- `from_state` TEXT
- `to_state` TEXT
- `actor` TEXT
- `note` TEXT
- `created_at` TEXT

## Candidate Lifecycle State Machine
- `new` -> generated/imported, no completed validation set.
- `checked` -> validators and scoring completed.
- `shortlisted` -> selected into top-N bundle.
- `psych_test` -> in active user testing cycle.
- `legal_review` -> sent for trademark/legal review.
- `approved` -> chosen/ready.
- `rejected` -> excluded with explicit reason.

Transition rules:
- `new -> checked` only after scoring snapshot exists.
- `checked -> shortlisted` requires not hard-failed and above configured threshold.
- `shortlisted -> psych_test -> legal_review -> approved` is preferred path.
- Any state can move to `rejected` with mandatory note.

## Validation Architecture

### Validator Modules
- Web collision check.
- Domain availability check.
- App Store namespace check.
- Package namespace check (PyPI/npm).
- Social handle signal check.
- Adversarial similarity check.

### Worker Execution Model
- Batch candidates in `pending` state.
- Run checks concurrently per candidate with per-source semaphores.
- Persist each validator result independently.
- Re-score candidate after all enabled validators complete.

### Rate-Limit Safety
- Per-check concurrency caps.
- Exponential backoff for transient failures.
- Cooldown-based cache reuse to avoid repeated external calls.

## Mode Semantics

### Strict Mode
- Unknown external states can hard-fail (configurable).
- Base domain and key checks must pass.

### Balanced Mode
- Unknown states can remain warnings.
- Hard-fail only on explicit conflicts.

### Degraded Network Mode
- Unknown external states are soft warnings by default.
- Preserve forward progress for exploratory runs.
- Clearly annotate uncertainty in output bundle.

## Scoring and Gates

### Hard-Fail Examples
- Exact app-store collision in target markets.
- Confirmed blocked domain for required TLDs.
- Adversarial similarity above threshold.

### Soft Penalty Examples
- Near web collisions.
- Namespace conflicts in package ecosystems.
- Social handle crowding.
- Pronunciation/spelling risk heuristics.

### Promotion Thresholds (Default V1)
- `shortlisted` candidate should satisfy:
- no hard fail
- recommendation in `strong|consider`
- minimum total score configurable (default 70)

## Run History and Longitudinal Tracking
- Persist one run summary record per execution.
- Track:
- candidate totals
- recommendation distribution
- hard-fail category counts
- unknown/warn rates
- overlap with previous top-50

## Outputs
- Candidate detail CSV (full check matrix).
- Candidate detail JSON (machine-readable evidence).
- Top-50 Markdown summary with rationale and links.
- Legal pre-screen bundle including DPMA/Swissreg/TMview search links.

## AI Boundary
- AI allowed:
- idea generation
- multilingual variation suggestion
- semantic clustering support (optional)

- AI not authoritative for:
- domain availability
- trademark clearance
- app-store/storefront collision truth

## Risks and Mitigations
- API throttling and blockers.
Mitigation: async rate limits + caching + degraded mode.

- Candidate drift toward repetitive low-quality clusters.
Mitigation: provenance tracking + run analytics + diversity constraints.

- Overreliance on AI output quality.
Mitigation: deterministic gates and source weighting.

- False confidence in legal safety.
Mitigation: explicit legal disclaimer in all exported reports.

## Mapping to Task 715 Subtasks
- 715.1: this design document.
- 715.2-715.3: DB + generator refactor.
- 715.4: AI ingestion channel.
- 715.5-715.7: async validators + reliability controls.
- 715.8: scoring + gates.
- 715.9: run analytics.
- 715.10-715.11: shortlist export + promotion states.
- 715.12: pilot run and operational guide.

## Acceptance Criteria for 715.1
- Data model and lifecycle states defined.
- Deterministic vs AI boundary documented.
- Validator orchestration and mode semantics specified.
- Quality gates and output artifacts specified.
- Subtask mapping clearly defined.
