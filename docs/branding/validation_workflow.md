# Validation Workflow

The supported shortlist validation path is now a single runner:

- `scripts/branding/run_brandpipe_validate.py`

It replaces the old split between acceptance-tail and async publish validation on the active surface.

## What the runner does

`run_brandpipe_validate.py` reads either:

- a reviewed shortlist CSV via `--input-csv`
- a newline-delimited file via `--names-file`
- inline names via `--names`

Then it runs the stable `src/brandpipe` validation stack and buckets each name into:

- `survivor`
- `review`
- `rejected`

The runner is blocking and reliability-first:

- it stores durable queue state in `<out-dir>/validation_state/brandpipe.db`
- it processes the shortlist serially
- rerunning the same command against the same shortlist/config resumes from persisted state
- if the shortlist/config fingerprint changes in the same out-dir, rerun with `--reset-state` or use a fresh out-dir

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
- `web` uses Brave first, with browser-backed Google as the reliable recheck path
- `app_store` is browser-only on the active surface
- `tm` uses direct TMView Playwright probing
- `social` is advisory, not a blocker

Compatibility note:

- `--concurrency` is still accepted for wrapper compatibility, but the effective worker count is always `1`

## Recommended command

```zsh
direnv exec . python3 scripts/branding/run_brandpipe_validate.py \
  --input-csv <review_csv> \
  --mode keep_maybe \
  --out-dir <validation_out_dir> \
  --web-browser-profile-dir test_outputs/brandpipe/playwright-profile \
  --tmview-profile-dir test_outputs/brandpipe/playwright-profile
```

Use `--mode keep` when you want only the manually approved names.

If you need to discard the persisted queue state for an out-dir, add:

```zsh
--reset-state
```

## Outputs

The runner writes:

- `validated_all.csv`
- `validated_survivors.csv`
- `validated_review_queue.csv`
- `validated_rejected.csv`
- `validated_publish_summary.json`

Interpretation:

- `survivor`: all selected blocker checks passed
- `review`: at least one selected check returned `warn`, `unavailable`, or `unsupported`
- `rejected`: at least one blocker check failed

## Browser profiles

Use one persistent browser profile for browser-backed checks:

```zsh
--web-browser-profile-dir test_outputs/brandpipe/playwright-profile \
--tmview-profile-dir test_outputs/brandpipe/playwright-profile
```

That gives you a stable path for:

- browser Google
- App Store search
- TMView

## Manual shortlist checks

```zsh
direnv exec . python3 scripts/branding/run_brandpipe_validate.py \
  --names "orbiluna,scedaria,otarelan" \
  --out-dir <validation_out_dir> \
  --web-browser-profile-dir test_outputs/brandpipe/playwright-profile \
  --tmview-profile-dir test_outputs/brandpipe/playwright-profile
```

## Retired active surface

The older wrapper/config path is no longer the supported entrypoint for shortlist validation:

- `run_validation_lane.py`
- `run_review_validation_bundle.py`
- `run_acceptance_tail.py`
- `naming_validate_async.py` as a user-facing shortlist command

Those scripts may still exist in archived or migration contexts, but the supported operator flow is now `run_brandpipe_validate.py`.
