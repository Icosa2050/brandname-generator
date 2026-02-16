---
owner: product
status: superseded_by_strict_gate
last_validated: 2026-02-15
---

# Broader Scope Shortlist (Global-Leaning)

This shortlist comes from the new generator pipeline (`scripts/branding/name_generator.py`) using explicit and generated candidates with legal/challenge heuristics and domain checks.

## Screening Context
- Scope: `global`
- Required domain rule: `.com` OR viable fallback `.com` variant
- Fallback patterns: `get<name>.com`, `use<name>.com`, `<name>app.com`, `<name>hq.com`, `<name>cloud.com`
- Final recheck source: `docs/branding/shortlist_recheck_final.csv`
- App Store checks used for recheck: iTunes lookup in `DE`, `CH`, `US` (exact collisions are hard-fail)

## Strict-Gate Reality Check (2026-02-15)
- New strict gate is now the default in `scripts/branding/name_generator.py`.
- Strict gate requires base `<name>.com` to be available and rejects exact web collisions.
- Under strict gate, previously locked finalists (`numeris`, `exactis`, `nexiora`, plus runner-up `funduso`) all fail.
- Current strict-screen source: `docs/branding/shortlist_strict_gate.csv`.
- Use this document as historical context only; strict CSV outputs are now canonical.

## Final Top 3 (Locked for Decision)
1. `numeris`
- Recheck score: `80`
- Similarity risk: `28` (low)
- Domain: `usenumeris.com` available (base `.com` unavailable)
- Notes: strong trust + numeric precision signal; good DACH pronunciation.

2. `exactis`
- Recheck score: `79`
- Similarity risk: `30` (low)
- Domain: `getexactis.com` available
- Notes: strongest accuracy/compliance framing for legal-grade settlement workflows.

3. `nexiora`
- Recheck score: `79`
- Similarity risk: `30` (very low nearest-cluster pressure)
- Domain: `usenexiora.com` available
- Notes: most brandable/international of the finalists; neutral enough for expansion.

## Runner-Up
- `funduso`
- Recheck score: `79`
- Similarity risk: `30` (low)
- Domain: `getfunduso.com` available
- Notes: very usable fallback if `nexiora` or `exactis` fails legal deep check.

## Names To Avoid (from recent checks)
- `xrd`: acronym-like, low trust fit, all key domains registered.
- `saldeo`: direct collision with existing SaldeoSMART brand.
- `saldio`: existing company usage and registry presence.
- `saldaro`: better phonetics but base `.com` unavailable; elevated similarity cluster.
- `immosaldo`: elevated similarity risk to established `immo*` marketplace brands.
- `fluenta`: rejected on hard fail (`exact_app_store_collision` in CH).
- `elementa`: rejected on hard fail (`exact_app_store_collision` in CH).

## Practical Recommendation
- If you want maximum legal distance + global expansion headroom, prioritize:
  1. `numeris`
  2. `exactis`
  3. `nexiora`
- Use brand + descriptor architecture in launch copy:
  - `<Brand> — Utility Settlement for Landlords`
  - DE localization: `<Brand> — Nebenkostenabrechnung für Vermieter`

## Source Artifacts
- `docs/branding/shortlist_screening_global.csv`
- `docs/branding/shortlist_screening_global_handcrafted.csv`
- `docs/branding/shortlist_screening_global_gemini60_v2.csv`
- `docs/branding/shortlist_screening_global_top10_recheck.csv`
- `docs/branding/shortlist_recheck_final.csv`
