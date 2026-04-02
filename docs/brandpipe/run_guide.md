# Brandpipe Run Guide

`brandpipe` has one supported generation lane:

- `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run --config <toml>`

The command reads one tracked config and writes one invocation bundle under:

- `test_outputs/brandpipe/run/<config_slug>/<invocation_id>/`

For validation details, see `docs/brandpipe/validation_workflow.md`.

## Environment

Load remote credentials with `direnv`:

```zsh
direnv allow .
direnv exec . env | rg OPENROUTER
```

Install browser dependencies once when you need TMView or App Store validation later:

```zsh
python3 -m playwright install chromium
```

## Supported Run Configs

- `resources/brandpipe/fixture_basic_run.toml`: deterministic fixture smoke.
- `resources/brandpipe/fixture_family_mix_run.toml`: fixture-backed family-mix example.
- `resources/brandpipe/lmstudio_runic_forge_smoke.toml`: LM Studio local smoke.
- `resources/brandpipe/openrouter_roles_smoke.toml`: OpenRouter multi-role smoke.

## Deterministic Fixture Smoke

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/fixture_basic_run.toml
```

## LM Studio Smoke

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/lmstudio_runic_forge_smoke.toml
```

## OpenRouter Smoke

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/openrouter_roles_smoke.toml
```

## Output Contract

Each run invocation creates:

- `manifest.json`
- `inputs/`
- `logs/`
- `state/brandpipe.db`
- `exports/finalists_<run_id>.csv`

Use one config per run. If a new market or product needs different behavior, create a new TOML under `resources/brandpipe/` and run it directly instead of adding wrappers or alternate lanes.
