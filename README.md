# brandname-generator

Standalone Python naming/branding pipeline with:
- candidate generation
- LLM ideation (OpenRouter, OpenAI-compatible local runtimes, hybrid)
- async validation
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

## Quickstart (OpenRouter remote)
```zsh
direnv exec . scripts/branding/run_openrouter_lane.sh --lane 0 --out-dir /tmp/branding_openrouter_tuned
direnv exec . scripts/branding/run_openrouter_lane.sh --lane 1 --out-dir /tmp/branding_openrouter_tuned
```

## Quickstart (LM Studio local)
Assumes LM Studio local server is running at `http://127.0.0.1:1234/v1`.

```zsh
python3 scripts/branding/test_local_llm_warm_cache.py \
  --provider=openai_compat \
  --base-url=http://127.0.0.1:1234/v1 \
  --model=llama-3.3-8b-instruct-omniwriter \
  --ttl-s=3600 \
  --keep-alive=30m \
  --runs=5 \
  --gap-s=1
```

## Quickstart (Ollama local)
Assumes Ollama is running at `http://127.0.0.1:11434`.

```zsh
zsh scripts/branding/test_ollama_local_smoke.sh \
  --model gemma3:12b \
  --keep-alive 30m
```

## Quickstart (3-model local benchmark)
Runs one standardized probe per lane and prints one comparison table:

```zsh
zsh scripts/branding/benchmark_local_llm_profiles.sh
```

## Quickstart (Hybrid local + remote)
Uses local LM Studio plus OpenRouter in the same ideation stage.

```zsh
direnv exec . python3 scripts/branding/naming_campaign_runner.py \
  --max-runs=1 \
  --sleep-s=0 \
  --no-mini-test \
  --generator-no-external-checks \
  --generator-only-llm-candidates \
  --llm-ideation-enabled \
  --llm-provider=hybrid \
  --llm-hybrid-local-models=llama-3.3-8b-instruct-omniwriter \
  --llm-hybrid-remote-models=mistralai/mistral-small-creative \
  --llm-hybrid-local-share=0.75 \
  --llm-openai-base-url=http://127.0.0.1:1234/v1 \
  --llm-openai-ttl-s=3600 \
  --llm-openai-keep-alive=30m \
  --out-dir=/tmp/branding_hybrid
```

Quality mode (slower, stronger): switch local model to `qwen3-vl-30b-a3b-instruct-mlx`.

## Prompt Strategy (recommended)
Canonical prompt location:
- Base prompts: `resources/branding/llm/llm_prompt.*.txt`
- New prompt template: `resources/branding/llm/llm_prompt.brand_market_template_v1.txt`
- Recommended custom layout (per brand/market):
  - `resources/branding/llm/prompts/<brand_slug>/<market_slug>.txt`

How to wire a prompt into generation:
- Direct runner: pass `--llm-prompt-template-file <absolute_or_repo_path>`
- Two-lane creation config: set the same flag inside `generation_command` in:
  - `resources/branding/configs/creation_lane.default.toml`
  - `resources/branding/configs/creation_lane.creative_hybrid.toml`

Recommendation:
- Keep one prompt file per brand+market variant.
- Do not edit shared base prompts for experiments; fork them to a brand/market file.
- Keep run outputs isolated per variant (`--out-dir`) so comparisons stay clean.

Shortcut wrappers:
- `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh`
- `zsh scripts/branding/run_hybrid_ollama_mistral.sh`
- Profile shortcuts (LM Studio hybrid):
  - `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --fast`
  - `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --quality`
  - `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --creative`
  - Optional remote-model mix (Mistral + Claude via OpenRouter):
    - `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --creative --remote-models mistralai/mistral-small-creative,anthropic/claude-sonnet-4.5`

## Always-On Robust Mode (macOS 24/7)
Supervisor loop (foreground):

```zsh
zsh scripts/branding/run_continuous_branding_supervisor.sh \
  --out-dir test_outputs/branding/continuous_hybrid \
  --backend auto \
  --fallback-backend ollama \
  --profile-plan fast,quality,creative \
  --max-usd-per-run 0.75 \
  --target-good 120 \
  --target-strong 40
```

`--target-good` / `--target-strong` use strict survivors
(checked recommendations with full expensive-check pass/warn coverage and no fail/error).

LaunchAgent installer (background, survives terminal close/relogin):

```zsh
zsh scripts/branding/install_launchd_continuous_branding.sh --install
zsh scripts/branding/install_launchd_continuous_branding.sh --status
```

Progress report:

```zsh
zsh scripts/branding/report_campaign_progress.sh \
  --out-dir test_outputs/branding/continuous_hybrid \
  --top-n 25
```

## Other Markets / Brands
For a new market or brand line, duplicate the config pair and adjust:
1. `scope` / store countries:
   - `scope`: `global`, `eu`, `dach`
   - screening countries: `de,ch,it` (or your target list)
2. prompt file:
   - `--llm-prompt-template-file resources/branding/llm/prompts/<brand>/<market>.txt`
3. naming constraints:
   - `--generator-min-len`, `--generator-max-len`
   - `--llm-rounds`, `--llm-candidates-per-round`
4. market lexicon/seeds:
   - update seeds and source inputs (`resources/branding/inputs/source_inputs_v2.csv` or market-specific copy)
5. legal gate settings:
   - keep `validation_lane.legal_heavy.toml` defaults for final shortlist,
   - adjust only if target registry coverage differs.

Suggested pattern:
- `resources/branding/configs/creation_lane.<brand>_<market>.toml`
- `resources/branding/configs/validation_lane.<brand>_<market>.toml`
- `test_outputs/branding/<brand>_<market>/...`

## More
- Acceptance-tail automation (decision pack -> final survivors + legal precheck):
  - `direnv exec . python3 scripts/branding/run_acceptance_tail.py --pack-dir <decision_pack_dir>`
- Two-lane workflow (compact create lane -> manual review -> validation lane):
  - `direnv exec . python3 scripts/branding/run_creation_lane.py --config resources/branding/configs/creation_lane.default.toml`
  - Review generated `review_unique_top120.csv` in the new decision pack (`keep/maybe/drop`).
  - `direnv exec . python3 scripts/branding/run_validation_lane.py --config resources/branding/configs/validation_lane.default.toml --pack-dir <decision_pack_dir>`
  - Tuned profiles:
    - `direnv exec . python3 scripts/branding/run_creation_lane.py --config resources/branding/configs/creation_lane.creative_hybrid.toml`
    - Review generated `review_unique_top160.csv` in the tuned decision pack (`keep/maybe/drop`).
    - `direnv exec . python3 scripts/branding/run_validation_lane.py --config resources/branding/configs/validation_lane.legal_heavy.toml --pack-dir <decision_pack_dir>`
- Full runner flags:
  - `python3 scripts/branding/naming_campaign_runner.py --help`
- Branding docs index:
  - `docs/branding/README.md`
- Detailed operational guide:
  - `docs/branding/name_generator_guide.md`
- Continuous test plan (mostly automated):
  - `docs/branding/continuous_pipeline_test_plan.md`
- Deferred improvement backlog:
  - `docs/branding/continuous_pipeline_deferred_backlog.md`
- Static inputs/examples:
  - `resources/branding/`
- Historical output artifacts:
  - `artifacts/branding/legacy/2026-02/`
