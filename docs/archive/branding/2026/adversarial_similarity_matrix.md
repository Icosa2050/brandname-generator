---
owner: product
status: draft
last_validated: 2026-02-17
related_task: 713.5
input_report:
  - /Users/bernhard/Development/kostula/docs/branding/trademark_prescreen_report.md
---

# Adversarial Similarity Matrix

## Objective
Stress-test pre-screen pass candidates against foreseeable challenge arguments from incumbents in DACH real-estate, accounting, and billing tooling.

Scale:
- `1` = low confusion risk
- `3` = medium
- `5` = high

Overall confusion score is the average of four axes:
- Phonetic
- Visual
- Semantic
- Market proximity

## Incumbent Set Used
- `ImmoScout24`
- `Immonet`
- `Objego`
- `Saldeo`
- `Scalara`
- `Kostal`
- `Domuso`

## Candidate Matrix

| Candidate | Phonetic | Visual | Semantic | Market proximity | Avg | Attack question outcome |
|---|---:|---:|---:|---:|---:|---|
| `certorio` | 2 | 2 | 2 | 2 | 2.00 | Plausible challenge exists but weak; no direct incumbent echo. |
| `certono` | 2 | 2 | 2 | 2 | 2.00 | Similar to `certorio` family but still low incumbent confusion. |
| `verorio` | 2 | 2 | 2 | 2 | 2.00 | No strong direct overlap with known incumbent name shapes. |
| `verobil` | 2 | 2 | 2 | 2 | 2.00 | Mild similarity to synthetic SaaS naming style; still low confusion. |
| `fidemen` | 2 | 2 | 2 | 2 | 2.00 | Distinct enough visually/phonetically vs top incumbents. |
| `trueledva` | 3 | 3 | 4 | 3 | 3.25 | Strongest attack vector: semantic overlap with ledger/accounting language. |

## Gate Decision
Pass (low-confusion average <= 2.5):
- `certorio`
- `certono`
- `verorio`
- `verobil`
- `fidemen`

Watchlist / conditional:
- `trueledva` (fails low-confusion threshold due semantic+visual overlap)

## Recommended Countermeasures for Passed Names
1. Avoid visual identity treatments that imitate incumbent palettes or typography.
2. Pair launch messaging with explicit category descriptor to improve comprehension without increasing legal similarity.
3. Keep backup candidate ready in case counsel flags hidden class-specific collisions.
