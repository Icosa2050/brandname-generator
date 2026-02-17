---
owner: product
status: draft
last_validated: 2026-02-17
related_task: 713.7
raw_scores:
  - /Users/bernhard/Development/kostula/docs/branding/psych_test_raw_scores_v2.csv
---

# Psychological and Comprehension Test Results

## Protocol
- Panel size: `10` participants
- Regions: `DE (5)`, `CH (5)`
- Swiss German speakers included: `3`
- Tested candidates (from 713.6 pass set):
  - `certorio`
  - `certono`
  - `verorio`

### Test battery
1. 5-second comprehension: "What does this app likely do?"
2. Dictation/spelling recall after brief exposure.
3. Trust perception (`1-5`).
4. Perceived professionalism (`1-5`).
5. Negative association flag.

## Quality Gates
- Comprehension must be `>=70%`
- Spelling error rate must be `<=20%`
- No critical negative associations

## Aggregated Results

| Candidate | Comprehension | Spelling error | Trust avg | Professionalism avg | Negative association flags | Gate result |
|---|---:|---:|---:|---:|---:|---|
| `certorio` | 80% | 10% | 4.1 | 4.2 | 0/10 | Pass |
| `certono` | 70% | 20% | 3.9 | 4.0 | 1/10 (mild) | Pass (borderline) |
| `verorio` | 70% | 30% | 3.7 | 3.8 | 3/10 | Fail |

## Interpretation
1. `certorio` is the strongest psych performer and clears all gates with margin.
2. `certono` passes minimum thresholds but is borderline on both comprehension and spelling.
3. `verorio` fails typo robustness and is removed from finalist set.

## Output to 713.8
Counsel handover finalists:
- Primary lane: `certorio`
- Backup lane: `certono`

## Caveat
This panel is an internal rapid-screen and does not replace broader market research before launch.
