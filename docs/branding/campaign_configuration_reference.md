---
owner: engineering
status: draft
last_validated: 2026-02-25
---

# Campaign Configuration Reference

## Why this exists
`scripts/branding/naming_campaign_runner.py --help` is exhaustive but not opinionated. This reference groups the high-impact options and configuration files into a practical operating model.

## Configuration Layers
1. Environment (`.env`, `.envrc`) for secrets and provider metadata.
2. Command flags for run behavior.
3. Wrapper scripts for profile presets.
4. Optional model/context config files for rotation and prompt context.

## Environment Variables
Required for OpenRouter mode:
- `OPENROUTER_API_KEY`

Optional headers/metadata:
- `OPENROUTER_HTTP_REFERER`
- `OPENROUTER_X_TITLE`

Recommendation:
- run remote or hybrid workloads using `direnv exec . <command>`.

## Core Run Controls
- `--out-dir`: root for mutable run outputs.
- `--db`: candidate lake DB path.
- `--hours`, `--max-runs`, `--sleep-s`: campaign loop boundaries.
- `--reset-db`: explicit clean-slate run.

## LLM Provider Controls
- `--llm-ideation-enabled`: enables model ideation stage.
- `--llm-provider`: `openrouter_http|openai_compat|ollama_native|hybrid|fixture`.
- `--llm-model`: single model for non-rotation mode.
- `--llm-openai-base-url`: LM Studio/OpenAI-compatible endpoint.
- `--llm-openai-ttl-s`, `--llm-openai-keep-alive`: local model residency hints.

Hybrid-specific:
- `--llm-hybrid-local-models`
- `--llm-hybrid-remote-models`
- `--llm-hybrid-local-share` (0..1)

## LLM Throughput and SLO Controls
- `--llm-rounds`
- `--llm-candidates-per-round`
- `--llm-stage-timeout-ms`
- `--llm-max-call-latency-ms`
- `--llm-slo-min-success-rate`
- `--llm-slo-max-timeout-rate`
- `--llm-slo-max-empty-rate`
- `--llm-slo-fail-open`

## Cost and Budget Controls
- `--llm-max-usd-per-run`
- `--llm-pricing-input-per-1k`
- `--llm-pricing-output-per-1k`

Use hard run-level cost caps before enabling unattended/background runs.

Wrapper defaults:
- `scripts/branding/run_hybrid_lmstudio_mistral.sh` and `scripts/branding/run_hybrid_ollama_mistral.sh` default to `0.75` via `--llm-max-usd-per-run` (override with `--max-usd-per-run`).
- `scripts/branding/run_continuous_branding_supervisor.sh` defaults to `--max-usd-per-run 0.75` and forwards it to every cycle.

## Validator Controls
- `--validator-state-filter` (`new` is usually preferred for incremental campaigns)
- `--validator-tier`
- `--validator-timeout-s`
- `--validator-max-concurrency`
- `--validator-expensive-finalist-limit`
- `--no-track-job-lifecycle`
- `--sqlite-busy-timeout-ms`

## History + Memory Controls
- `--validator-memory-db`
- `--validator-memory-ttl-days`
- `--skip-failed-history` (default behavior)
- `--no-skip-failed-history` (only for explicit re-screening)

## Sharding + Throughput Controls
- `--shard-count`, `--shard-id`
- `--shard-db-isolation`
- `--merge-shards`
- `--shard-scheduling=weighted`
- `--shard-history-progress-csv`
- `--shard-weight-fallback-s`

## Progress + Heartbeat Controls
- `--live-progress`
- `--live-progress-patterns`
- `--heartbeat-events`
- `--heartbeat-interval-s`
- `--heartbeat-jsonl`

## Configuration Files

### Model rotation config
File: `resources/branding/llm/llm_models.example.toml`
Purpose:
- Define provider-scoped model lists.
- Enable model selection strategies (`round_robin`, `random`).

### Prompt context packet
File: `resources/branding/llm/llm_context.example.json`
Purpose:
- Inject structured product/tone/target context into ideation prompts.

### Prompt templates
Files:
- `resources/branding/llm/llm_prompt.utility_split_v1.txt`
- `resources/branding/llm/llm_prompt.creative_longer_names_v1.txt`

Purpose:
- Lock prompt behavior for specific campaign goals (utility vs creative/longer-name bias).

## Recommended Presets
Fast local:
- provider local, low rounds, cheap validator tier.

Balanced hybrid:
- `--llm-provider=hybrid`
- local share `0.60-0.80`
- moderate rounds with expensive finalist limits.

Creative sweep:
- lower local share (`0.20-0.40`)
- creative prompt template
- longer-name bias + higher expensive-check budget.

## Suggested Validation Flow
1. Check configuration shape with `--help` before long runs.
2. Run one smoke campaign (`--max-runs=1`).
3. Inspect report output.
4. Start continuous supervisor only after smoke run passes.
