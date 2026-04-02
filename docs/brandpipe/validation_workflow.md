# Brandpipe Validation Workflow

The supported shortlist validation path is now a single CLI command:

- `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli validate`

It replaces the old split between acceptance-tail and async publish validation on the active surface.

## What the runner does

`brandpipe.cli validate` reads either:

- a reviewed shortlist CSV via `--input-csv`
- a newline-delimited file via `--names-file`
- inline names via `--names`

Then it runs the stable `src/brandpipe` validation stack and buckets each name into:

- `survivor`
- `review`
- `rejected`

The runner is blocking and reliability-first:

- it stores durable state inside a fresh invocation bundle under `<out-dir>/<invocation_id>/state/brandpipe.db`
- it processes the shortlist serially
- rerunning against the same label root creates a fresh invocation bundle by default
- if you intentionally want to wipe the fresh invocation state before execution, `--reset-state` is still accepted

## Canonical checks

Default shortlist checks:

- `domain`
- `package`
- `company`
- `web`
- `app_store`
- `social`
- `tm`

Reliability posture:

- `domain` and `package` are direct deterministic checks
- `company` uses exact Companies House matching, not search-engine approximation
- `web` defaults to `serper,brave`, with SERPER as the primary Google signal
- `app_store` is browser-only on the active surface
- `tm` uses direct TMView Playwright probing
- `social` is advisory, not a blocker

Compatibility note:

- `--concurrency` is still accepted for wrapper compatibility, but the effective worker count is always `1`

## Recommended command

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli validate \
  --input-csv <review_csv> \
  --mode keep_maybe \
  --out-dir test_outputs/brandpipe/validate/manual \
  --web-browser-profile-dir test_outputs/brandpipe/validate/playwright-profile \
  --tmview-profile-dir test_outputs/brandpipe/validate/playwright-profile
```

Use `--mode keep` when you want only the manually approved names.

If you need to discard the persisted queue state for an out-dir, add:

```zsh
--reset-state
```

## Outputs

Each invocation writes:

- `manifest.json`
- `exports/validated_all.csv`
- `exports/validated_survivors.csv`
- `exports/validated_review_queue.csv`
- `exports/validated_rejected.csv`
- `exports/validated_publish_summary.json`

Recommended placement:

- validator bundles: `test_outputs/brandpipe/validate/<label>/`
- shared browser profile: `test_outputs/brandpipe/validate/playwright-profile`

Interpretation:

- `survivor`: all selected blocker checks passed
- `review`: at least one selected check returned `warn`, `unavailable`, or `unsupported`
- `rejected`: at least one blocker check failed

## Browser profiles

Use one persistent browser profile for browser-backed checks:

```zsh
--web-browser-profile-dir test_outputs/brandpipe/validate/playwright-profile \
--tmview-profile-dir test_outputs/brandpipe/validate/playwright-profile
```

That gives you a stable path for:

- browser Google
- App Store search
- TMView

## Manual shortlist checks

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli validate \
  --names "orbiluna,scedaria,otarelan" \
  --out-dir test_outputs/brandpipe/validate/manual-names \
  --web-browser-profile-dir test_outputs/brandpipe/validate/playwright-profile \
  --tmview-profile-dir test_outputs/brandpipe/validate/playwright-profile
```
