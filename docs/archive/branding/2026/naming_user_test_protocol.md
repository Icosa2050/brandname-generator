---
owner: product
status: ready-for-execution
last_validated: 2026-02-15
---

# Naming User Test Protocol (Phase 4 / 712.4)

## Goal
Validate comprehension and trust of surviving name candidates with a 5-second test.

## Candidate Set (Round 1)
1. ImmoSaldo
2. NebenSaldo
3. NebenKlar
4. ObjektSaldo
5. UmlageKlar

Optional Round 2 if needed:
- UmlagePilot
- UmlagePlaner
- HausUmlage
- MietUmlage
- WohnUmlage

## Audience
- Target: small landlords and micro property managers in DE/CH.
- Sample size: 5 to 10 participants.
- Recommended split: at least 2 participants from CH and 3 from DE.

## Test Format
- Show each name with one descriptor line for exactly 5 seconds.
- Descriptor line (fixed across candidates):
  - `Software für rechtssichere Heiz- und Nebenkostenabrechnungen`
- After 5 seconds, hide stimulus and ask 3 questions:
  1. What do you think this product does?
  2. Who do you think this is for?
  3. Would you trust this for legal/financial property administration? (1-5)

## Pass Criteria
- Comprehension rate >= 70%.
- Average trust >= 4.0 / 5.

## Scoring Rules
- Q1 correct if user mentions utility/service-charge settlement or billing (`Nebenkosten`, `Betriebskosten`, `Abrechnung`, equivalent).
- Q2 correct if user identifies landlords, property managers, or similar.
- Comprehension per response = correct Q1 AND correct Q2.

## Output
Record results in:
- `docs/branding/naming_user_test_results.csv`

Then compute:
- comprehension_rate per candidate
- average_trust per candidate

Decision rule:
- Any candidate failing either threshold is removed.
- Top 3 by trust, then comprehension, then lowest legal/domain risk go to final recommendation.
