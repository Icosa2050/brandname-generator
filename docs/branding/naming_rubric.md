---
owner: product
status: draft
last_validated: 2026-02-17
related_task: 713.6
---

# Naming Rubric V1

## Purpose
Provide a deterministic, auditable scoring model for shortlist progression from legal/adversarial screening to psych validation and final counsel handover.

## Weighted Dimensions (0-100)

| Dimension | Weight | Scoring guidance |
|---|---:|---|
| Legal defensibility | 30 | Lower collision/opposition likelihood in DE/CH/EU target classes. |
| Distinctiveness | 20 | Not generic/descriptive; clear ownable brand shape. |
| Trust signal | 15 | Conveys reliability and professionalism for landlords/property managers. |
| Pronunciation + typo robustness | 15 | Easy to say/spell in DE/CH contexts; low dictation error risk. |
| Portability | 10 | Works across DE/CH and can scale beyond one local jargon term. |
| SEO uniqueness | 10 | Searchability without heavy incumbent noise. |

## Hard Gates
A candidate is rejected regardless of numeric score if any hard gate fails:
1. Clear blocking legal conflict in pre-screen.
2. Adversarial confusion average > 2.5 (phonetic + visual + semantic + market axes).
3. Psych comprehension < 70%.
4. Psych spelling error rate > 20%.

## Pass Threshold
- Numeric threshold: `>= 75/100`.
- Hard gates must all pass.

## Tie-Break Rule
If two candidates are within 2 points:
1. Choose higher legal-defensibility score.
2. If still tied, choose lower observed psych spelling error rate.
