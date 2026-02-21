---
owner: product
status: draft
last_validated: 2026-02-17
---

# Name Generator Guide

## Purpose
`scripts/branding/name_generator.py` creates brand-name candidates and pre-screens them for:
- generator-family mixing (`coined`, `stem`, `suggestive`, `seed`, `expression`, `source_pool`, `blend`),
- curated source-pool based generation with lineage tracking (`source_atoms` in SQLite),
- challenge risk (similarity to protected names + descriptiveness),
- App Store collisions (DE/CH/US quick signal),
- domain availability via RDAP (`.com`, `.de`, `.ch`).
- package namespace collisions (`PyPI` + `npm`),
- social-handle signal (`GitHub`, `LinkedIn`, `X`, `Instagram`),
- adversarial challenger similarity and confusion risk,
- expanded multilingual phonetic variation roots for less-crowded naming space,
- anti-gibberish gates and diversity controls (prefix/suffix/shape family balancing),
- false-friend and negative-association risk filtering,
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

### 6) Initialize candidate lake DB
```zsh
python3 scripts/branding/naming_db.py --db docs/branding/naming_pipeline.db init
```

### 7) Import historical artifacts into candidate lake
```zsh
python3 scripts/branding/naming_db.py \
  --db docs/branding/naming_pipeline.db \
  import \
  --inputs "docs/branding/generated_name_candidates_*.csv" "docs/branding/generated_name_candidates_*.json" \
  --source-type=import
```

### 8) Ingest AI-generated batches with provenance
```zsh
python3 scripts/branding/name_ideation_ingest.py \
  --db docs/branding/naming_pipeline.db \
  --names="Amaniro,Imarvia,Nuruvia" \
  --scope=global \
  --gate=balanced \
  --model="google/gemini-3-pro-preview" \
  --provider="pal" \
  --prompt="latin-script catchy names with trust tone" \
  --source-label="gemini_batch_01"
```

### 9) Run async validator orchestration on candidate lake
```zsh
python3 scripts/branding/naming_validate_async.py \
  --db docs/branding/naming_pipeline.db \
  --state-filter="new,checked" \
  --checks="adversarial,psych,descriptive,domain,web,app_store,package,social" \
  --candidate-limit=200 \
  --concurrency=16 \
  --max-retries=2
```

### 10) Ingest curated source atoms for generator v2
```zsh
python3 scripts/branding/name_input_ingest.py \
  --db docs/branding/naming_pipeline_v1.db \
  --inputs docs/branding/source_inputs_v2.csv \
  --source-label=curated_lexicon_v2 \
  --scope=global \
  --gate=balanced \
  --also-candidates
```

### 11) Run v2 generation using source-pool and blend families
```zsh
python3 scripts/branding/name_generator.py \
  --scope=global \
  --gate=balanced \
  --variation-profile=expanded \
  --generator-families=source_pool,blend,seed,suggestive,coined \
  --family-quotas=source_pool:260,blend:220,seed:120,suggestive:90,coined:90 \
  --source-pool-db=docs/branding/naming_pipeline_v1.db \
  --source-pool-limit=600 \
  --source-min-confidence=0.58 \
  --false-friend-lexicon=docs/branding/naming_false_friend_lexicon_v1.md \
  --degraded-network-mode \
  --pool-size=500 \
  --check-limit=120 \
  --output=docs/branding/candidate_batch_v2.csv \
  --json-output=docs/branding/candidate_batch_v2.json \
  --persist-db --db=docs/branding/naming_pipeline_v1.db
```

### 12) One-command test runner (smoke/full)
```zsh
# fast smoke run (writes to /tmp)
zsh scripts/branding/test_naming_pipeline_v2.sh smoke

# full run (writes canonical docs/branding outputs)
zsh scripts/branding/test_naming_pipeline_v2.sh full

# optional black format check (ruff runs by default)
USE_BLACK=1 zsh scripts/branding/test_naming_pipeline_v2.sh smoke
```

### 13) Campaign runner with active ideation (phase 0)
```zsh
python3 scripts/branding/naming_campaign_runner.py \
  --hours=1.0 \
  --max-runs=8 \
  --generator-quality-first \
  --llm-ideation-enabled \
  --llm-provider=openrouter_http \
  --llm-model="mistralai/mistral-small-creative" \
  --llm-openrouter-http-referer="https://github.com/Icosa2050/kostula" \
  --llm-openrouter-x-title="Kostula Naming Pipeline" \
  --llm-context-file=docs/branding/llm_context.example.json \
  --llm-rounds=2 \
  --llm-candidates-per-round=20 \
  --llm-max-call-latency-ms=8000 \
  --llm-stage-timeout-ms=30000 \
  --llm-max-usd-per-run=0.50 \
  --llm-pricing-input-per-1k=0.0006 \
  --llm-pricing-output-per-1k=0.0006 \
  --llm-cache-dir=test_outputs/branding/llm_cache \
  --dynamic-window-runs=5 \
  --dynamic-fail-threshold=0.20 \
  --dynamic-prefix-entropy-threshold=2.5 \
  --ab-mode \
  --ab-seed=722
```

Notes:
- Set `OPENROUTER_API_KEY` for `openrouter_http` mode.
- Optional attribution headers: `--llm-openrouter-http-referer` and `--llm-openrouter-x-title`
  (or env vars `OPENROUTER_HTTP_REFERER`, `OPENROUTER_X_TITLE`).
- `--llm-provider=fixture --llm-fixture-input=<file>` is useful for offline smoke tests.
- `--llm-context-file=<json>` injects product/user/tone guidance into the LLM prompt.
- Example context packet: `docs/branding/llm_context.example.json`.
- `llm_cost_usd` now prefers provider-reported `usage.cost` when available; token-price flags remain fallback estimation.
- OpenRouter calls use a compatibility fallback chain (`json_schema+require_parameters` -> `json_object` -> plain chat) so models that reject strict routing still return candidates.
- Campaign `llm_stage_status` now distinguishes empty/error cases (`empty_with_errors`, `empty`) instead of reporting `ok` with zero candidates.
- A/B mode writes `ab_report.json` and `ab_report.md` in campaign output root.

## Output
The script writes a CSV to:
- default: `docs/branding/generated_name_candidates_<scope>_<timestamp>.csv`
- or your custom `--output` path.

Key columns:
- `generator_family`: generation family producing the candidate.
- `lineage_atoms`: source atoms used to construct candidate.
- `source_confidence`: source confidence proxy from input corpus.
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
- `gibberish_*`: low-humanity pattern penalties and reasons.
- `false_friend_*`: negative-association and false-friend risk evidence.
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
- `--generator-families=<list>`: select generator families.
- `--family-quotas=<family:count,...>`: control family contribution.
- `--source-pool-db=<path>`: DB path for curated source atoms.
- `--source-pool-limit=<n>`: cap source atoms loaded for generation.
- `--source-min-confidence=<0..1>`: minimum source confidence.
- `--source-languages=<list>`: optional language filters for source atoms.
- `--source-categories=<list>`: optional semantic category filters.
- `--max-per-prefix2/--max-per-suffix2/--max-per-shape/--max-per-family`: diversity gates.
- `--false-friend-lexicon=<path>`: markdown lexicon for semantic safety checks.
- `--false-friend-fail-threshold=<n>`: fail threshold for false-friend risk.
- `--gibberish-fail-threshold=<n>`: fail threshold for gibberish penalty.
- `--degraded-network-mode`: keep `unknown` external checks as soft warnings (useful with flaky network/bot throttling).
- `--adversarial-fail-threshold=82`: tune hard-fail threshold for challenger similarity.
- `--json-output=<path>`: write machine-readable JSON artifact.
- `--run-log=<path>`: append per-run summary JSONL for longitudinal tracking.
- `--persist-db --db=<path>`: store scored candidates into SQLite candidate lake.
- `scripts/branding/naming_db.py`: initialize/import/stats for candidate lake.
- `scripts/branding/name_ideation_ingest.py`: import AI ideation batches with provenance metadata.
- `scripts/branding/name_input_ingest.py`: ingest curated source atoms into `source_atoms`.
- `scripts/branding/naming_validate_async.py`: async job orchestration and persisted validator lifecycle states.
- `scripts/branding/naming_campaign_runner.py`: long-running campaign sweeps with optional active LLM ideation stage.
- `--progress-every=<N>`: progress snapshot cadence for async validator.
- `--progress-interval-s=<seconds>`: time-based progress fallback.
- `--no-progress`: disable async validator progress output.

## Recommended Workflow
1. Ingest curated source atoms (`name_input_ingest.py`) before generation.
2. Run `--scope=dach` and `--scope=global` with mixed generator families.
3. Keep names with:
- `recommendation in {strong, consider}`
- `hard_fail=false`
- required domains available for target scope.
4. Use generated trademark URLs for manual registry pre-screen (DPMA, IGE/Swissreg, EUIPO/TMview, Zefix).
5. Merge top candidates into the naming framework shortlist.
6. Run 5-second user trust/comprehension test before final choice.
7. Track every run in `docs/branding/name_generator_runs.jsonl` to monitor drift and avoid repeating dead-end candidate clusters.

## Notes
- The built-in protected list is heuristic and intentionally conservative.
- Add/remove known market marks directly in `PROTECTED_MARKS` for your category.
- Expanded variation includes curated Latin-script roots inspired by African-language phonetics (for example Swahili-origin forms) to widen search space; treat this as naming inspiration, not linguistic certification.
- Social and web checks are best-effort only and may return `unknown` due rate limiting or bot protection.
