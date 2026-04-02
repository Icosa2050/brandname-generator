---
owner: engineering
status: active
last_validated: 2026-04-01
---

# Brandpipe Local And Remote Workflow

`brandpipe` supports three practical generation modes:

1. Fixture smoke with `resources/brandpipe/fixture_basic_run.toml`
2. Local LM Studio run with `resources/brandpipe/lmstudio_runic_forge_smoke.toml`
3. Remote OpenRouter run with `resources/brandpipe/openrouter_roles_smoke.toml`

All three use the same command shape:

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run --config <toml>
```

## Fixture Smoke

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/fixture_basic_run.toml
```

## Local LM Studio

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/lmstudio_runic_forge_smoke.toml
```

Optional warm-cache probe:

```zsh
python3 scripts/brandpipe/local_llm_warm_cache_probe.py \
  --provider=openai_compat \
  --base-url=http://127.0.0.1:1234/v1 \
  --model=llama-3.3-8b-instruct-omniwriter \
  --ttl-s=3600 \
  --keep-alive=30m
```

## Remote OpenRouter

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/openrouter_roles_smoke.toml
```

## Validation After Generation

After manual shortlist review, run the one supported validation lane:

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli validate \
  --input-csv <review_csv> \
  --mode keep_maybe \
  --out-dir test_outputs/brandpipe/validate/manual \
  --web-browser-profile-dir test_outputs/brandpipe/validate/playwright-profile \
  --tmview-profile-dir test_outputs/brandpipe/validate/playwright-profile
```
