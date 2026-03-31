# Name Generator Guide

This is the current operational guide for the supported brandpipe flow.

For shortlist validation details and output semantics, see `docs/branding/validation_workflow.md`.

## Environment

Load secrets with `direnv` before remote or browser-backed runs:

```zsh
direnv allow .
direnv exec . env | rg OPENROUTER
```

One-time browser setup for TMView, browser-backed web search, and App Store checks:

```zsh
python3 -m playwright install chromium
```

## Supported entrypoints

- `scripts/branding/run_brandpipe_attack.py`
  - canonical shortlist generator
  - default lanes are curated: `expressive,plosive,angular,balanced,crossmarket`
  - experimental widening lanes are available through `--lanes all`
- `scripts/branding/run_brandpipe_validate.py`
  - canonical shortlist validator
  - stable check set: `domain,package,company,web,app_store,social,tm`
  - uses a blocking queue-backed flow with persisted state under `<out-dir>/validation_state/brandpipe.db`
  - effective worker count is always `1`
  - uses Brave-first web probing, browser-backed Google rechecks, direct TMView, and browser-only App Store probing

## Standard generation workflow

Generate a merged shortlist:

```zsh
direnv exec . python3 scripts/branding/run_brandpipe_attack.py \
  --briefs-file resources/brandpipe/example_batch_briefs.toml \
  --lanes default \
  --out-dir test_outputs/brandpipe/manual_run
```

Review the generated `merged_review_top*.csv`.

Recommended lane guidance:

- `default`: curated production lanes
- `all`: curated lanes plus experimental `short_recovery` and `short`
- explicit CSV lane lists: use when you want to force a custom mix

## Standard shortlist validation workflow

Validate a reviewed shortlist CSV:

```zsh
direnv exec . python3 scripts/branding/run_brandpipe_validate.py \
  --input-csv <review_csv> \
  --mode keep_maybe \
  --out-dir <validation_out_dir> \
  --web-browser-profile-dir test_outputs/brandpipe/playwright-profile \
  --tmview-profile-dir test_outputs/brandpipe/playwright-profile
```

This writes:

- `validated_all.csv`
- `validated_survivors.csv`
- `validated_review_queue.csv`
- `validated_rejected.csv`
- `validated_publish_summary.json`

Rerunning the same command with the same shortlist and validation config resumes from the persisted queue state.
If you intentionally want to discard that state for the same out-dir, add `--reset-state`.

## Manual name checks

Validate a hand-picked shortlist without a review CSV:

```zsh
direnv exec . python3 scripts/branding/run_brandpipe_validate.py \
  --names "orbiluna,scedaria,otarelan" \
  --out-dir <validation_out_dir> \
  --web-browser-profile-dir test_outputs/brandpipe/playwright-profile \
  --tmview-profile-dir test_outputs/brandpipe/playwright-profile
```

You can also use `--names-file <path>` for newline-delimited inputs.

## Browser-backed checks

For reliable shortlist validation, reuse one persistent browser profile for:

- `web`
- `app_store`
- `tm`

Recommended pattern:

```zsh
--web-browser-profile-dir test_outputs/brandpipe/playwright-profile \
--tmview-profile-dir test_outputs/brandpipe/playwright-profile
```

If Chrome lives outside the default macOS path, also pass:

```zsh
--web-browser-chrome-executable <chrome_binary> \
--tmview-chrome-executable <chrome_binary>
```

Compatibility note:

- `--concurrency` is still accepted by `run_brandpipe_validate.py` so existing wrappers do not break, but it is clamped to `1`

## Continuous / hybrid helpers

These helpers are still valid for long-running ideation and local/remote mixes:

```zsh
zsh scripts/branding/run_hybrid_lmstudio_mistral.sh
zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --fast
zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --quality
zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --creative
zsh scripts/branding/run_hybrid_ollama_mistral.sh
```

## Common paths

- active docs: `docs/branding/`
- static inputs and briefs: `resources/branding/`, `resources/brandpipe/`
- runtime outputs and mutable DBs: `test_outputs/branding/`, `test_outputs/brandpipe/`
- archived docs: `docs/archive/branding/2026/`
- historical artifacts: `artifacts/branding/legacy/2026-02/`

## Notes

- Run remote-dependent commands through `direnv exec .`.
- Use `run_brandpipe_attack.py` and `run_brandpipe_validate.py` as the supported public surface.
- The older lane wrappers and duplicated validation path are being retired from the active workflow.
