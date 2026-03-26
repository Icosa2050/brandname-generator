# Name Generator Guide

This is the current operational guide for naming runs and post-review validation.

For the validation-lane rationale and output layout, see `docs/branding/validation_workflow.md`.

## Environment

Load secrets with `direnv` before remote or browser-backed runs:

```zsh
direnv allow .
direnv exec . env | rg OPENROUTER
```

One-time browser setup for EUIPO and Swissreg probes:

```zsh
python3 -m playwright install chromium
```

## Core entrypoints

- `scripts/branding/run_creation_lane.py`
  - generate names and build a decision pack for manual review
- `scripts/branding/run_validation_lane.py`
  - recommended post-review wrapper
  - default workflow is `dual`
- `scripts/branding/run_acceptance_tail.py`
  - low-level acceptance-tail only
- `scripts/branding/naming_validate_async.py`
  - low-level async publish validator
- `scripts/branding/run_review_validation_bundle.py`
  - low-level helper that chains both validation lanes on one reviewed CSV

## Standard reviewed-shortlist workflow

1. Create a decision pack:

```zsh
direnv exec . python3 scripts/branding/run_creation_lane.py \
  --config resources/branding/configs/creation_lane.default.toml
```

2. Review the generated `keep/maybe/drop` CSV in the new decision pack.

3. Run the validation lane:

```zsh
direnv exec . python3 scripts/branding/run_validation_lane.py \
  --config resources/branding/configs/validation_lane.default.toml \
  --pack-dir <decision_pack_dir>
```

That now runs:

- acceptance-tail live/legal screening
- async publish validation
- combined summary export

## Legal-heavier workflow

Use this when you want wider reviewed input plus stricter post-review narrowing:

```zsh
direnv exec . python3 scripts/branding/run_creation_lane.py \
  --config resources/branding/configs/creation_lane.creative_hybrid.toml

direnv exec . python3 scripts/branding/run_validation_lane.py \
  --config resources/branding/configs/validation_lane.legal_heavy.toml \
  --pack-dir <decision_pack_dir>
```

## Acceptance-only fallback

If you explicitly want only the live/legal last-mile pass, either:

- set `workflow = "acceptance_only"` in the validation config, then run `run_validation_lane.py`
- or call `run_acceptance_tail.py` directly

Direct command:

```zsh
direnv exec . python3 scripts/branding/run_acceptance_tail.py \
  --pack-dir <decision_pack_dir>
```

## Direct campaign runner

Use the campaign runner for long-form generation and built-in async validation:

```zsh
direnv exec . python3 scripts/branding/naming_campaign_runner.py --help
```

## Hybrid shortcut commands

```zsh
zsh scripts/branding/run_hybrid_lmstudio_mistral.sh
zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --fast
zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --quality
zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --creative
zsh scripts/branding/run_hybrid_ollama_mistral.sh
```

## Continuous supervisor

Foreground:

```zsh
zsh scripts/branding/run_continuous_branding_supervisor.sh \
  --out-dir test_outputs/branding/continuous_hybrid \
  --backend auto \
  --fallback-backend ollama \
  --profile-plan fast,quality,creative \
  --target-good 120 \
  --target-strong 40
```

Progress summary:

```zsh
zsh scripts/branding/report_campaign_progress.sh \
  --out-dir test_outputs/branding/continuous_hybrid \
  --top-n 25
```

## Common paths

- active docs: `docs/branding/`
- static inputs and configs: `resources/branding/`
- runtime outputs and mutable DBs: `test_outputs/branding/`
- archived docs: `docs/archive/branding/2026/`
- historical artifacts: `artifacts/branding/legacy/2026-02/`

## Notes

- Run remote-dependent commands through `direnv exec .`.
- After manual review, use `run_validation_lane.py` unless you intentionally want a lower-level script.
- The previous large guide has been archived under `docs/archive/branding/2026/`.
