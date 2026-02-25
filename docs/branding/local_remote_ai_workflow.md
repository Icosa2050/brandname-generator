---
owner: engineering
status: draft
last_validated: 2026-02-25
---

# Local + Remote AI Workflow

## Goal
Define how local and remote LLMs should be used together in the naming pipeline so runs are fast, cost-aware, and resilient.

## Workflow Summary
1. Load environment and credentials.
2. Warm local runtime so first-run latency does not dominate.
3. Run campaign in one of three modes:
- local-only for cost control and iteration speed,
- remote-only for quality or model diversity,
- hybrid for balanced throughput + quality.
4. Validate and score candidates.
5. Run progress reporting and archive non-review run documents when needed.

## Environment Setup
Use `direnv` whenever a run may use remote providers:

```zsh
direnv allow .
direnv exec . env | rg OPENROUTER
```

Remote mode requires:
- `OPENROUTER_API_KEY`
- optional attribution headers: `OPENROUTER_HTTP_REFERER`, `OPENROUTER_X_TITLE`

## Runtime Modes

### Local-only (LM Studio)
Use when cost must be near zero and you want fast iteration.

```zsh
python3 scripts/branding/test_local_llm_warm_cache.py \
  --provider=openai_compat \
  --base-url=http://127.0.0.1:1234/v1 \
  --model=llama-3.3-8b-instruct-omniwriter \
  --ttl-s=3600 \
  --keep-alive=30m
```

### Local-only (Ollama)
Use as fallback if LM Studio is unavailable.

```zsh
zsh scripts/branding/test_ollama_local_smoke.sh --model gemma3:12b --keep-alive 30m
```

### Remote-only (OpenRouter)
Use when you need stronger external models and local inference is unavailable.

```zsh
direnv exec . scripts/branding/run_openrouter_lane.sh --lane 0 --out-dir /tmp/branding_openrouter_tuned
```

### Hybrid (recommended default)
Use local models for most rounds and remote models for diversity/quality uplift.

```zsh
direnv exec . python3 scripts/branding/naming_campaign_runner.py \
  --llm-ideation-enabled \
  --llm-provider=hybrid \
  --llm-hybrid-local-models=llama-3.3-8b-instruct-omniwriter \
  --llm-hybrid-remote-models=mistralai/mistral-small-creative \
  --llm-hybrid-local-share=0.75 \
  --llm-max-usd-per-run=0.75 \
  --llm-openai-base-url=http://127.0.0.1:1234/v1 \
  --llm-openai-ttl-s=3600
```

## Choosing Local vs Remote Share
- `0.80-1.00` local share: lowest cost, best for exploration volume.
- `0.50-0.75` local share: balanced mode for continuous campaigns.
- `0.20-0.40` local share: creative/quality sweeps where remote model diversity matters.

## Continuous Supervisor Pattern
For long-running accumulation, rotate profiles and stop on strict survivor targets:

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

Use progress reporting:

```zsh
zsh scripts/branding/report_campaign_progress.sh \
  --out-dir test_outputs/branding/continuous_hybrid \
  --top-n 25
```

## Operational Guardrails
- Always set stop thresholds (`--target-good`, `--target-strong`) for unattended runs.
- Always set run-level spend caps (`--llm-max-usd-per-run` / `--max-usd-per-run`) for unattended runs.
- Keep local model residency on (`--llm-openai-ttl-s`, `--llm-openai-keep-alive`) to avoid repeated cold starts.
- Prefer `--backend auto --fallback-backend ollama` for resilience.
- Archive non-review run documents periodically to keep run directories reviewable.

## Review-Critical vs Archive-Candidate Artifacts
Keep for review:
- `naming_campaign.db`
- `campaign_summary.json`
- `campaign_progress.csv`

Usually archive after checkpoint:
- `continuous/logs/`
- `continuous/supervisor_heartbeat.log`
- `runs/campaign_heartbeat.jsonl`
- transient run logs under `runs/`
