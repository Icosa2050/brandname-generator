# brandname-generator

Standalone Python `brandpipe` pipeline with:
- candidate generation
- LLM ideation (OpenRouter, OpenAI-compatible local runtimes, hybrid)
- shortlist validation with browser-backed web/TMView/App Store rechecks
- exclusion memory (SQLite) to avoid re-validating eliminated names

## Environment
This repository expects secrets to be loaded via `.envrc`:
- `OPENROUTER_API_KEY`
- `OPENROUTER_HTTP_REFERER`
- `OPENROUTER_X_TITLE`

Load and use env like this:
```zsh
direnv allow .
direnv exec . env | rg OPENROUTER
```

Important: run commands that need remote access via `direnv exec . <command>`.

## Automation Worktrees (protected)
The recurring branding automations currently run from dedicated Codex worktrees.
Do not remove these paths during routine worktree cleanup:
- `~/.codex/worktrees/automation-branding-fusion/brandname-generator`
- `~/.codex/worktrees/automation-branding-health/brandname-generator`

Current automation mapping:
- `branding-fusion-run` (generation lane): `automation-branding-fusion`
- `branding-fusion-run-2` (fusion lane): `automation-branding-fusion`
- `creative-run-check` (validation lane): `automation-branding-health`

If you need to reclaim them, pause or reconfigure the automations first.

## Python Setup (recommended)
Use Python 3.11+ and a local virtual environment:

```zsh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
python -m playwright install chromium
```

Notes:
- `requirements.txt` is intentionally small and only covers optional capabilities currently used by `brandpipe`:
  - `playwright` for EUIPO/Swissreg browser probes
  - `wordfreq` for corpus analysis utilities and future frequency-based tuning
- Core candidate generation/validation scripts are standard-library based.

## Canonical Operator Surface
- single-config runs: `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run --config <toml>`
- shortlist validation: `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli validate --input-csv <csv> --out-dir <label_root>`

## Quickstart (Deterministic Smoke)
```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/fixture_basic_run.toml
```

## Quickstart (Single-Config Fixture Run)
```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/fixture_basic_run.toml
```

## Quickstart (LM Studio Local)
Assumes LM Studio local server is running at `http://127.0.0.1:1234/v1`.

```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run \
  --config resources/brandpipe/lmstudio_runic_forge_smoke.toml
```

Optional provider warm-cache probe:

```zsh
python3 scripts/brandpipe/local_llm_warm_cache_probe.py \
  --provider=openai_compat \
  --base-url=http://127.0.0.1:1234/v1 \
  --model=llama-3.3-8b-instruct-omniwriter \
  --ttl-s=3600 \
  --keep-alive=30m \
  --runs=5 \
  --gap-s=1
```

## Quickstart (Shortlist Validation)
```zsh
direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli validate \
  --input-csv <review_csv> \
  --mode keep_maybe \
  --out-dir test_outputs/brandpipe/validate/manual \
  --web-browser-profile-dir test_outputs/brandpipe/validate/playwright-profile \
  --tmview-profile-dir test_outputs/brandpipe/validate/playwright-profile
```

## Prompt Strategy (recommended)
Canonical prompt location:
- Family prompts used by the supported generation flow: `resources/brandpipe/prompts/*.txt`

How to wire a prompt into generation:
- Single-config brandpipe runs: set `[ideation].prompt_template_file` in the TOML config you run.
- Family-surface generation uses the built-in templates under `resources/brandpipe/prompts/`.

Recommendation:
- Keep one prompt file per active family.
- Do not keep versioned prompt forks in the main tree unless they are the current supported prompt.
- Keep run outputs isolated per variant (`--out-dir`) so comparisons stay clean.

Brandpipe output contract:
- `test_outputs/brandpipe/run/<config_slug>/<invocation_id>/`: direct single-config pipeline runs
- `test_outputs/brandpipe/validate/<label>/<invocation_id>/`: validator bundles with the same contract

## Other Markets / Brands
For a new market or brand line, keep the flow simple and stay inside the brandpipe surfaces:
1. create a dedicated run config under `resources/brandpipe/`
2. if ideation mix changes, copy an existing TOML under `resources/brandpipe/` and adjust `[ideation]` there
3. keep outputs isolated under the right bucket:
   `test_outputs/brandpipe/run/<brand>_<market>/<invocation_id>/`
   or `test_outputs/brandpipe/validate/<brand>_<market>/<invocation_id>/`
4. validate the reviewed shortlist with `brandpipe.cli validate`
5. keep the flow inside the brandpipe CLI surfaces

Suggested pattern:
- `resources/brandpipe/<brand>_<market>.toml`
- optional custom prompt file referenced by `[ideation].prompt_template_file`
- `test_outputs/brandpipe/run/<brand>_<market>/...`
- `test_outputs/brandpipe/validate/<brand>_<market>/...`

## More
- Detailed supported runbook:
  - `docs/brandpipe/run_guide.md`
- Validation workflow explanation:
  - `docs/brandpipe/validation_workflow.md`
- Single-config CLI help:
  - `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli --help`
- Validation help:
  - `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli validate --help`
- Brandpipe docs index:
  - `docs/brandpipe/README.md`
- Active configs, prompts, and fixtures:
  - `resources/brandpipe/`
- Active helper scripts:
  - `scripts/brandpipe/`
- Historical legacy artifacts:
  - `artifacts/branding/legacy/`
