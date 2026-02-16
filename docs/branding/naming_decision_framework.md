---
owner: product
status: draft
last_validated: 2026-02-15
task: 712.1
---

# Naming Decision Framework (Phase 1)

## One-Sentence Naming Brief
Software for small landlords and property managers in Germany and Switzerland that creates legally robust Heiz- und Nebenkostenabrechnungen with transparent, dispute-ready allocation logic users can trust.

## DE/CH Language Constraints For Name Generation
- Prioritize trust-forward wording over startup-style novelty.
- Ensure names work with DE terms: `Nebenkosten`, `Betriebskosten`, `Umlage`, `Abrechnung`.
- Ensure names do not break CH comprehension in Heiz- und Nebenkosten (HNK) context.
- Avoid names with `C/K` spelling ambiguity in German pronunciation.
- Avoid names confusingly similar to `KOSTAL` or `Costal`.

## Candidate Pool (45 Total)
Notes:
- This is generation output only for `712.1`; legal/domain/store elimination happens in `712.3`.
- Final keep/rename decision is not made in this phase.

### 1) Descriptive + Modifier (15)
1. NebenkostenPilot
2. NebenkostenPlaner
3. NebenkostenManager
4. NebenkostenWerk
5. NebenkostenCheck
6. BetriebskostenPilot
7. BetriebskostenPlaner
8. BetriebskostenManager
9. UmlagePilot
10. UmlagePlaner
11. UmlageManager
12. AbrechnungsWerk
13. AbrechnungsPlaner
14. HeizkostenPlaner
15. HNKPlaner

### 2) Hybrid Brand + Descriptor (15)
1. ImmoSaldo - Nebenkostenabrechnung
2. ImmoExakt - Nebenkostenabrechnung
3. WohnBilanz - Nebenkostenabrechnung
4. ObjektSaldo - Nebenkostenabrechnung
5. SaldoWerk - Nebenkostenabrechnung
6. MietBilanz - Nebenkostenabrechnung
7. Verwalto - Nebenkostenabrechnung
8. Domivio - Nebenkostenabrechnung
9. Imovara - Nebenkostenabrechnung
10. Umlavio - Nebenkostenabrechnung
11. Abrevia - Nebenkostenabrechnung
12. Abrechnio - Nebenkostenabrechnung
13. Klarvia - Nebenkostenabrechnung
14. Hausiva - Nebenkostenabrechnung
15. Mietora - Nebenkostenabrechnung

### 3) Compact Compounds (15)
1. ImmoKlar
2. NebenKlar
3. MietKlar
4. UmlageKlar
5. HausBilanz
6. ObjektKlar
7. VerwalterKlar
8. AbrechnungsPlus
9. Nebenkosten24
10. HausUmlage
11. MietUmlage
12. NebenSaldo
13. AbrechnungsKompass
14. HNKKompass
15. WohnUmlage

## Exit Criteria For Subtask 712.1
- One-sentence brief defined with DE/CH context.
- Candidate list contains at least 40 names.
- All three strategies represented.

## Handoff To 712.2
- Apply weighted scoring (100): clarity 30, statutory trust 25, pronunciation/spelling 20, visual economy 15, SEO uniqueness 10.
- Remove candidates below threshold and prepare shortlist for elimination gates.

# Phase 2 - Weighted Scoring (712.2)

## Scoring Model
- Semantic clarity: 30
- Statutory trust: 25
- DACH pronunciation/spelling: 20
- Visual economy: 15
- SEO uniqueness: 10
- Total: 100

## Decision Thresholds
- Operational pass threshold for further screening: `>= 84`.
- Shortlist threshold for deep elimination gates (`712.3`): `>= 88`.

## Scoring Output
- Full matrix (45 candidates): `docs/branding/naming_scores_phase2.csv`

## Shortlist For 712.3 (>= 88)
1. ImmoKlar (92)
2. ImmoSaldo - Nebenkostenabrechnung (91)
3. NebenKlar (91)
4. MietKlar (91)
5. HausBilanz (91)
6. NebenSaldo (91)
7. UmlagePilot (90)
8. ImmoExakt - Nebenkostenabrechnung (90)
9. UmlagePlaner (89)
10. UmlageKlar (89)
11. ObjektKlar (89)
12. HausUmlage (89)
13. MietUmlage (89)
14. WohnUmlage (89)
15. NebenkostenPilot (88)
16. WohnBilanz - Nebenkostenabrechnung (88)
17. ObjektSaldo - Nebenkostenabrechnung (88)

## Phase 2 Notes
- Descriptive names scored high on clarity/trust but were often penalized on visual economy (long compounds).
- Hybrid names with explicit descriptor provided the best trust/clarity balance.
- Compact compounds provided strong memorability and pronunciation while keeping a professional tone.
- Acronym-first variants (`HNK*`) underperformed on clarity outside expert contexts.

## Handoff To 712.3
- Run exact + close-spelling App Store checks (DE/CH focus) for the 17 shortlisted names.
- Run `.de/.ch/.com` domain availability checks for the same set.
- Run trademark pre-screen (DPMA, IGE, EUIPO) and apply kill criteria.

# Phase 3 - Elimination Gates (712.3)

## Inputs
- Phase-2 shortlist (`>= 88`): 17 names.
- Technical checks executed:
  - App Store search signal (DE + CH) for exact terms.
  - Domain availability check for `.de`, `.ch`, `.com` via RDAP.

## Technical Gate Results
- App Store DE/CH: all 17 shortlisted names returned `0` direct software hits in quick search checks.
- Domain kill criterion used: eliminate if `.de` or `.ch` unavailable.
- Eliminated on domain gate: 7 names.
- Survivors after domain gate: 10 names.

## Survivors After Technical Gates
1. ImmoSaldo (91)
2. NebenKlar (91)
3. NebenSaldo (91)
4. UmlagePilot (90)
5. UmlagePlaner (89)
6. UmlageKlar (89)
7. HausUmlage (89)
8. MietUmlage (89)
9. WohnUmlage (89)
10. ObjektSaldo (88)

## Preliminary Trademark Risk Triage
- `medium`: ImmoSaldo, NebenKlar, NebenSaldo, UmlageKlar, ObjektSaldo
- `high` (descriptive/generic collision risk): UmlagePilot, UmlagePlaner, HausUmlage, MietUmlage, WohnUmlage

Notes:
- This is a preliminary risk triage only, not legal clearance.
- Official registry confirmation still required in DPMA, IGE/Swissreg, and EUIPO before final naming decision.

## Phase-3 Artifacts
- Raw technical checks: `docs/branding/naming_elimination_phase3.csv`
- Consolidated phase-3 assessment: `docs/branding/naming_phase3_assessment.csv`

## Handoff To 712.4
- Run 5-second comprehension + trust test on the 10 surviving names.
- Prioritize these 5 for first-round testing to reduce participant fatigue:
  1. ImmoSaldo
  2. NebenSaldo
  3. NebenKlar
  4. ObjektSaldo
  5. UmlageKlar

# Phase 4 - User Test Execution Kit (712.4)

Because live participant interviews were not run in this session, a ready-to-run protocol and result sheet were prepared.

## Prepared Files
- Protocol: `docs/branding/naming_user_test_protocol.md`
- Result template: `docs/branding/naming_user_test_results.csv`

## Required To Close 712.4
- Collect 5-10 participant responses using the provided template.
- Compute pass/fail per candidate:
  - comprehension_rate >= 70%
  - average_trust >= 4.0/5

# Phase 5 - Provisional Recommendation (712.5 draft)

## Current Decision Status
- Not final yet: awaiting real participant data from `712.4`.
- Provisional ranking based on scoring + technical gates + preliminary legal risk triage.

## Provisional Top 3
1. ImmoSaldo
- Why: highest trust/clarity among domain-viable options; strong financial signal in DE/CH.
- Risk: medium trademark collision risk due `Immo` + finance-style compound.

2. NebenSaldo
- Why: clear billing/settlement mental model with strong DE comprehension.
- Risk: medium trademark collision risk in accounting-like naming space.

3. ObjektSaldo
- Why: strong property-management anchor (`Objekt`) with accounting credibility.
- Risk: medium trademark collision risk; slightly less immediate layperson clarity than `NebenSaldo`.

## Candidates Deprioritized
- `Umlage*` cluster: passed technical gates but marked high legal collision risk due generic/descriptive nature.
- `*Klar` cluster with unavailable `.de` and/or `.ch` was eliminated under domain kill criteria.

## Finalization Checklist
- Run participant test (`docs/branding/naming_user_test_protocol.md`).
- Fill results (`docs/branding/naming_user_test_results.csv`).
- Re-rank top 3 using measured trust/comprehension outcomes.
- Execute formal trademark attorney review before filing/launch.

# Broader Scope Generator Approach

To avoid overfitting on German-compound names, use the generator to produce and screen broader candidates across market scopes:

- Script: `scripts/branding/name_generator.py`
- Guide: `docs/branding/name_generator_guide.md`

Recommended runs:
1. `--scope=dach` for DE/CH conversion-oriented naming.
2. `--scope=eu` for expansion-aware naming.
3. `--scope=global` for language-neutral brand exploration.

Recent generated samples:
- `docs/branding/generated_name_candidates_eu_sample.csv`
- `docs/branding/generated_name_candidates_global_seeded.csv`
- `docs/branding/generated_name_candidates_global_seeded_v2.csv`
- `docs/branding/shortlist_screening_global.csv`
- `docs/branding/shortlist_screening_global_handcrafted.csv`
- `docs/branding/shortlist_screening_global_gemini60_v2.csv`
- `docs/branding/broader_scope_shortlist.md`
