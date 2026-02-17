---
owner: product
status: draft
last_validated: 2026-02-17
related_task: 719
---

# Naming Input Pipeline V2 Plan

## Objective
Increase candidate quality by improving upstream inputs and generation constraints so produced names are human-sounding, distinctive, and legally screenable.

## Core Problem
Current batches overuse synthetic syllable recombination, causing low semantic coherence and weak brand feel.

## Strategy
1. Build a high-signal source corpus.
2. Generate from multiple controlled strategies, not one pattern family.
3. Enforce anti-gibberish and diversity gates before expensive legal checks.
4. Track lineage and provenance so every candidate can be explained and audited.

## Phase 1: Source Quality Foundation
- Define corpus rules and blocked patterns.
- Add deterministic source ingestion with provenance tags.
- Extend DB to store source atoms and candidate lineage.

Deliverables:
- /Users/bernhard/Development/kostula/docs/branding/naming_input_corpus_spec_v2.md
- /Users/bernhard/Development/kostula/scripts/branding/name_input_ingest.py
- /Users/bernhard/Development/kostula/scripts/branding/naming_db.py (schema extension)

## Phase 2: Generator Quality Upgrade
- Add generator-mix architecture in `name_generator.py`.
- Enforce family quotas and configurable weights.
- Add anti-gibberish and diversity controls.
- Add multilingual false-friend and negative-association filters.

Deliverables:
- /Users/bernhard/Development/kostula/scripts/branding/name_generator.py
- /Users/bernhard/Development/kostula/docs/branding/naming_false_friend_lexicon_v1.md

## Phase 3: Validation Batch and Handoff
- Produce 100-candidate batch with improved inputs.
- Reduce to <=20 via deterministic thresholds.
- Publish quality report and handoff to remaining naming/legal tasks.

Deliverables:
- /Users/bernhard/Development/kostula/docs/branding/candidate_batch_v2.csv
- /Users/bernhard/Development/kostula/docs/branding/candidate_batch_screened_v2.csv
- /Users/bernhard/Development/kostula/docs/branding/naming_input_quality_report_v2.md

## Acceptance Criteria
- Candidate batch shows higher lexical diversity and lower synthetic repetition than v1.
- Every candidate includes source lineage metadata.
- Screened shortlist is <=20 and every exclusion is traceable to an objective gate.
- Output is ready for trademark pre-screen in tasks 713.4-713.9.

## Risks
- External registry/search availability may still be degraded; preserve provisional flagging.
- Over-filtering can kill creativity; keep thresholds configurable and evidence-driven.
