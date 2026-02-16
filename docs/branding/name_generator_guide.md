---
owner: product
status: draft
last_validated: 2026-02-16
---

# Name Generator Guide

## Purpose
`scripts/branding/name_generator.py` creates brand-name candidates and pre-screens them for:
- challenge risk (similarity to protected names + descriptiveness),
- App Store collisions (DE/CH/US quick signal),
- domain availability via RDAP (`.com`, `.de`, `.ch`).
- package namespace collisions (`PyPI` + `npm`),
- social-handle signal (`GitHub`, `LinkedIn`, `X`, `Instagram`),
- adversarial challenger similarity and confusion risk,
- expanded multilingual phonetic variation roots for less-crowded naming space,
- generated trademark lookup links (DPMA, Swissreg, TMview) for manual legal checks.
- run-history logging (`JSONL`) for long-term iterative search.
- strict gate checks:
  - base `.com` availability required (no fallback accepted),
  - broader App Store country checks (`de,ch,us,gb,fr,it` by default),
  - exact web collision detection (quoted-name search results).

It is a **screening** tool, not legal advice. Final legal clearance still requires professional trademark review.

## Quick Start

### 1) DACH-first run
```zsh
python3 scripts/branding/name_generator.py \
  --scope=dach \
  --gate=strict \
  --seeds="kostula,utilaro,saldaro,ledger" \
  --pool-size=280 \
  --check-limit=70 \
  --json-output=docs/branding/generated_name_candidates_dach_strict.json
```

### 2) Broader EU run
```zsh
python3 scripts/branding/name_generator.py \
  --scope=eu \
  --gate=strict \
  --seeds="utility,settlement,property,saldo" \
  --pool-size=320 \
  --check-limit=90
```

### 3) Global-leaning run
```zsh
python3 scripts/branding/name_generator.py \
  --scope=global \
  --gate=strict \
  --variation-profile=expanded \
  --seeds="utility,rent,property,ledger" \
  --pool-size=360 \
  --check-limit=100
```

### 4) Screen a handcrafted shortlist directly
```zsh
python3 scripts/branding/name_generator.py \
  --scope=global \
  --gate=strict \
  --candidates="utilaro,saldaro,saldio,immosaldo,nebensaldo,objektsaldo" \
  --only-candidates \
  --output=docs/branding/shortlist_screening.csv \
  --json-output=docs/branding/shortlist_screening.json
```

### 5) Long-run exploration (50 candidates, network-degraded tolerant)
```zsh
python3 scripts/branding/name_generator.py \
  --scope=global \
  --gate=balanced \
  --variation-profile=expanded \
  --degraded-network-mode \
  --seeds="kostula,utilaro,ledger,allocation,clarity,balance,tenant" \
  --pool-size=420 \
  --check-limit=50 \
  --store-countries=de,ch,us \
  --output=docs/branding/generated_name_candidates_global_balanced_degraded.csv \
  --json-output=docs/branding/generated_name_candidates_global_balanced_degraded.json \
  --run-log=docs/branding/name_generator_runs.jsonl
```

## Output
The script writes a CSV to:
- default: `docs/branding/generated_name_candidates_<scope>_<timestamp>.csv`
- or your custom `--output` path.

Key columns:
- `quality_score`: pronounceability/length/memorability quality.
- `challenge_risk`: similarity + descriptiveness + scope penalty.
- `total_score`: quality adjusted by risk.
- `gate`: `strict` or `balanced`.
- `itunes_*`: quick store collision signal.
- `itunes_exact_countries`: where exact App Store name matches were found.
- `itunes_unknown_countries`: countries that could not be checked.
- `domain_*_available`: RDAP availability signal.
- `domain_com_fallback_*`: availability of fallback `.com` patterns
  (`get<name>.com`, `use<name>.com`, `<name>app.com`, `<name>hq.com`, `<name>cloud.com`).
- `web_*`: quoted-name web-collision signal from top results.
- `pypi_exists` / `npm_exists`: package namespace collision signal.
- `social_*`: best-effort availability signal for key handles.
- `adversarial_*`: confusion signal versus likely challenger/incumbent marks.
- `psych_*`: spelling risk and trust-proxy heuristics for early filtering.
- `trademark_*_url`: prebuilt lookup URLs for DPMA/Swissreg/TMview checks.
- `external_penalty`: extra risk applied from web/store signals.
- `hard_fail` / `fail_reason`: automatic rejection reason.
- `recommendation`: `strong`, `consider`, `weak`, `reject`.

Important flags:
- `--candidates`: always include these names in screening.
- `--only-candidates`: skip generation and evaluate only explicit names.
- `--gate=strict`: default; requires base `.com`, rejects exact web collisions.
- `--gate=balanced`: allows fallback `.com` patterns and softer filtering.
- `--store-countries=de,ch,us,gb,fr,it`: set App Store check countries.
- `--no-store-check`: skip App Store queries.
- `--no-domain-check`: skip RDAP domain checks.
- `--no-web-check`: disable web collision checks (not recommended).
- `--no-package-check`: disable PyPI/npm collision checks.
- `--no-social-check`: disable social-handle checks.
- `--no-progress`: disable live per-candidate progress output.
- `--variation-profile=expanded`: adds broader multilingual phonetic roots.
- `--degraded-network-mode`: keep `unknown` external checks as soft warnings (useful with flaky network/bot throttling).
- `--adversarial-fail-threshold=82`: tune hard-fail threshold for challenger similarity.
- `--json-output=<path>`: write machine-readable JSON artifact.
- `--run-log=<path>`: append per-run summary JSONL for longitudinal tracking.

## Recommended Workflow
1. Run `--scope=dach` and `--scope=global`.
2. Keep names with:
- `recommendation in {strong, consider}`
- `hard_fail=false`
- required domains available for target scope.
3. Use generated trademark URLs for manual registry pre-screen (DPMA, IGE/Swissreg, EUIPO/TMview, Zefix).
4. Merge top 10 into the naming framework shortlist.
5. Run 5-second user trust/comprehension test before final choice.
6. Track every run in `docs/branding/name_generator_runs.jsonl` to monitor drift and avoid repeating dead-end candidate clusters.

## Notes
- The built-in protected list is heuristic and intentionally conservative.
- Add/remove known market marks directly in `PROTECTED_MARKS` for your category.
- Expanded variation includes curated Latin-script roots inspired by African-language phonetics (for example Swahili-origin forms) to widen search space; treat this as naming inspiration, not linguistic certification.
- Social and web checks are best-effort only and may return `unknown` due rate limiting or bot protection.
