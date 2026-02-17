---
owner: product
status: draft
last_validated: 2026-02-17
related_task: 719.6
---

# Naming False-Friend Lexicon V1

Purpose: define tokens that should penalize or hard-fail naming candidates due
negative associations, false-friend risks, or regulatory/reputational concerns.

Format below is parsed by `scripts/branding/name_generator.py`.

| token | weight | reason |
| --- | --- | --- |
| mist | 18 | negative_meaning_de |
| gift | 20 | false_friend_de |
| assi | 30 | negative_association_de |
| nazi | 100 | prohibited_association |
| dumm | 24 | negative_association_de |
| schlecht | 24 | negative_association_de |
| faux | 12 | negative_association_fr |
| foul | 16 | negative_association_en |
| toxic | 24 | negative_association_en |
| poop | 18 | negative_association_en |
| crud | 18 | negative_association_en |
| fail | 14 | failure_association_en |
| pain | 16 | negative_association_en |
| debt | 14 | negative_association_en |

## Notes
- Weights are additive and capped at 100.
- `--false-friend-fail-threshold` controls hard-fail behavior.
- Lexicon is intentionally conservative; manual review still required.
