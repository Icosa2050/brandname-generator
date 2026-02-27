---
owner: product
status: draft
last_validated: 2026-02-27
---

# Corpus Strategy Execution Plan

## Objective
Increase forward progress in naming campaigns by improving the quality and diversity of source atoms while keeping legal/collision risk controlled.

Primary outcome:
- increase `strict_strong` yield per run window without increasing expensive-check failure churn.

## Decision Summary
- Do not use direct modern-fantasy IP lexemes (for example LOTR-specific terms) as generation atoms.
- Use a layered corpus design:
  1. Core atoms (high confidence): WordNet + Wiktionary-derived stems.
  2. Expansion atoms (medium confidence): ConceptNet-derived relations + curated long-tail public-domain mythic/classical stems.
  3. Exclusion/filter layer: company/software/entity blacklist + frequency gates + validation-fail memory.
- Keep decision gating on `strict_*` metrics. Treat `checked_*` as throughput telemetry only.

## Why This Plan
- Current pipeline now demotes downstream validation failures from `checked` state, reducing stale positives.
- Stagnation is primarily a candidate quality/diversity problem; corpus quality is a high-leverage input.
- Corpus improvements must be paired with explicit exclusion/filtering to avoid collision-heavy drift.

## Scope
In scope:
- source-atom ingestion and weighting strategy;
- pre-generator filtering design;
- benchmark protocol and promotion criteria.

Out of scope:
- legal counsel replacement;
- broad architecture rewrite of generator/validator engines.

## Phased Plan

### Phase 0: Baseline and Diagnostics (1 day)
Tasks:
1. Snapshot baseline metrics over recent runs:
   - `strict_good`, `strict_strong`, expensive-check fail/error rate, shortlist novelty.
2. Build fail-category breakdown from validation output:
   - domain/web/app_store/package/social fail/error mix;
   - hard-fail reason distribution.
3. Confirm atom lineage observability in outputs (`source_label`, family, seed lineage).

Deliverables:
- `baseline_metrics_<date>.md`
- `fail_mix_<date>.csv`

Go/No-Go:
- Go if at least one dominant failure class is addressable via corpus/filter strategy.

### Phase 1: Core Corpus Build (1-2 days)
Tasks:
1. Create `resources/branding/inputs/source_inputs_core_v3.csv` from:
   - WordNet domain-adjacent roots (`trust`, `clarity`, `stability`, `governance`, `finance`, `property`);
   - Wiktionary etymological stems (Latin/Greek/Old English/Old French).
2. Ingest with morphology derivation:
   - `scripts/branding/name_input_ingest.py --derive-morphology`.
3. Validate distribution with source stats and quick sanity checks.

Deliverables:
- core CSV file;
- source stats snapshot.

Go/No-Go:
- Go if core set is clean (no obvious entity collisions) and category/language distribution is balanced.

### Phase 2: Expansion + Exclusion Layer (2 days)
Tasks:
1. Create `resources/branding/inputs/source_inputs_expansion_v3.csv` from:
   - ConceptNet related roots;
   - long-tail public-domain mythology/classical dictionaries.
2. Build exclusion list input:
   - company/software/entity titles from Wikipedia/Wikidata exports;
   - local validation fail memory rollup.
3. Apply frequency gates in preprocessing:
   - suppress overly common atoms and extremely rare/noisy atoms.

Deliverables:
- expansion CSV file;
- exclusion/filter spec and seed lists.

Go/No-Go:
- Go if expansion adds diversity without materially raising pre-validation collision indicators.

### Phase 3: Controlled A/B Benchmark (2 days)
Protocol:
1. Run baseline corpus vs candidate corpus in fresh output dirs.
2. Keep all runtime settings identical except corpus stack.
3. Use sufficient run count for stable comparison.

Measure:
- strict yield: `strict_good`, `strict_strong` per fixed run window;
- expensive-check fail/error rate among checked survivors;
- shortlist novelty (unique additions, overlap ratio);
- cost/throughput guardrails (runtime, spend cap adherence).

Go/No-Go:
- Promote if all are true:
  - strict yield meaningfully improves;
  - expensive-check fail/error does not regress;
  - novelty improves without quality collapse.

### Phase 4: Promote and Operationalize (1 day)
Tasks:
1. Promote winning corpus files to active defaults.
2. Document default run profile and rollback command.
3. Schedule periodic corpus refresh + blacklist refresh.

Deliverables:
- updated default corpus references;
- runbook note in docs.

## Operating Rules
- Keep per-run spend cap active.
- Default accumulation mode: `--validator-state-filter=new`.
- Run periodic refresh mode: `--validator-state-filter=new,checked`.
- Keep demotion logic active so `checked` reflects current survivorship.

## Initial Data Sources (Low-Risk Start)
1. Princeton WordNet (core semantic roots).
2. Open Multilingual WordNet (language extension).
3. Wiktionary dump extracts (etymology/stem harvesting).
4. ConceptNet (expansion relations).
5. Public-domain literary/classical corpora (long-tail roots only, curated).

## Risks and Mitigations
- Risk: creative corpus increases legal/collision drift.
  - Mitigation: strict exclusion layer + expensive-check gating.
- Risk: novelty rises but strict quality drops.
  - Mitigation: promote only on strict-yield + fail-rate combined criteria.
- Risk: too many near-duplicates.
  - Mitigation: enforce diversity constraints and lineage-aware dedup checks.

## PAL (Opus) Review Notes
This plan was reviewed with PAL using `anthropic/claude-opus-4.6`.
Main incorporated recommendations:
- strengthen go/no-go checkpoints per phase;
- separate generation corpus from exclusion/filter corpus;
- require benchmark promotion criteria to include both yield and fail-rate behavior;
- avoid direct modern-fantasy IP lexemes for generation despite creative upside.

## Implementation Status (Current)
Implemented in this repository:
- New corpus inputs:
  - `resources/branding/inputs/source_inputs_core_v3.csv`
  - `resources/branding/inputs/source_inputs_expansion_v3.csv`
  - `resources/branding/inputs/source_exclusions_seed_v1.txt`
- New baseline diagnostics script:
  - `scripts/branding/build_corpus_strategy_baseline.py`
- Source-ingest filtering enhancements in `scripts/branding/name_input_ingest.py`:
  - `--exclude-inputs`
  - `--zipf-min`, `--zipf-max`, `--zipf-language`
  - optional strict dependency guard: `--zipf-require-package`
  - run summary now records filtered counts (`excluded_count`, `zipf_low_count`, `zipf_high_count`)
- Campaign runner integration in `scripts/branding/naming_campaign_runner.py`:
  - `--source-input-files`
  - `--source-exclusion-files`
  - `--source-zipf-min`, `--source-zipf-max`, `--source-zipf-language`

Validation completed:
- unit tests for new ingest helpers and runner arg parsing
- smoke run of `build_corpus_strategy_baseline.py`
- smoke run of source ingest with new corpus inputs and exclusion file
