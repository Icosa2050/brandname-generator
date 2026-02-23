---
owner: product
status: draft
last_validated: 2026-02-17
related_task: 719.8
upstream_artifacts:
  - /Users/bernhard/Development/kostula/docs/branding/candidate_batch_v1.csv
  - /Users/bernhard/Development/kostula/docs/branding/candidate_batch_v2.csv
  - /Users/bernhard/Development/kostula/docs/branding/candidate_batch_screened_v2.csv
  - /Users/bernhard/Development/kostula/docs/branding/candidate_batch_screened_v2_summary.json
---

# Naming Input Quality Report V2

## Executive Summary
Task 719 goal was to improve upstream naming quality before legal and psych evaluation. V2 delivers a reproducible source-ingestion + generator-mix pipeline and a deterministic shortlist gate.

Outcome:
- Generated batch increased to 120 candidates with lineage and generator-family provenance.
- Deterministic screening reduced to 20 shortlist candidates with explicit exclusion reasons.
- Quality gates now include anti-gibberish and false-friend controls.

## Before/After Metrics

| Metric | V1 Batch | V2 Batch | Screened V2 |
|---|---:|---:|---:|
| Candidate rows | 80 | 120 | 20 |
| Unique names | 80 | 120 | 20 |
| Avg challenge risk | 34.61 | 30.39 | 24.55 |
| Challenge risk range | 29-40 | 21-44 | 22-28 |
| Generator-family coverage | 1 (`unknown`) | 5 | 5 |
| Gibberish penalty available | No | Yes | Yes |
| False-friend risk available | No | Yes | Yes |
| Hard-fail rows | 0 | 0 | 0 |

## Deterministic Gate (V2)
Screen gate used for `candidate_batch_screened_v2.csv`:
- `recommendation in {strong,consider}`
- `hard_fail = false`
- `challenge_risk <= 30`
- `external_penalty <= 16`
- `gibberish_penalty <= 20`
- `false_friend_risk <= 20`
- `psych_spelling_risk <= 20`
- Diversity stage 1: `prefix4<=3`, `suffix3<=2`, `root5<=2`, family caps (`source_pool<=8`, `blend<=4`, `expression<=3`, `suggestive<=3`, `coined<=2`)
- Stage 2 deterministic backfill by score if shortlist count is below target.

Exclusion counts (`candidate_batch_screened_v2_summary.json`):
- `challenge_risk`: 68
- `diversity_family_cap`: 22
- `diversity_root5_cap_stage1`: 8
- `diversity_suffix3_cap`: 3
- `backfill_used`: 1

## Key Wins
1. Reproducible provenance: source atoms + lineage now tie each candidate to inputs.
2. Better controllability: generator families and quotas allow directional tuning.
3. Lower shortlist risk band: average challenge risk dropped from 34.61 (v1) to 24.55 (screened v2).
4. Stronger pre-legal hygiene: gibberish and false-friend gates are now first-class checks.

## Remaining Gaps
1. External checks were intentionally disabled during v2 generation (degraded mode) and must be completed in legal phase.
2. Some morphological clusters remain concentrated (`cert*`, `lumen*`, `vero*`, `terra*`) despite diversity caps.
3. Psychological tests are still downstream and not yet measured on real users.
4. Trademark registers still require manual/counsel review for final go/no-go.

## Handoff to Task 713.4-713.9
- `713.4`: use the 20-name screened list for DPMA/Swissreg/TMview/Zefix pre-screen.
- `713.5`: run adversarial confusion matrix against known incumbents in real-estate/proptech/accounting adjacency.
- `713.6`: score shortlist with weighted rubric and enforce `>=75/100` threshold.
- `713.7`: run DE/CH comprehension/psych protocol; reject names failing comprehension or spelling gates.
- `713.8`: prepare counsel-ready packet with primary + backup, classes, jurisdictions, and evidence links.
- `713.9`: publish final memo and explicitly unblock tasks 701, 702, 707.

## Legal Notice
This report is internal product screening output. It is not legal advice and does not guarantee freedom to operate.
