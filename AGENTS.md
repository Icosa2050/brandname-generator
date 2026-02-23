# Agent Notes

## Environment Loading (important)
- This repo uses `.envrc` + `.env`.
- `OPENROUTER_*` variables are **not** guaranteed to exist in plain subprocesses.
- For commands that need environment variables, run through direnv:
  - First time / after `.envrc` changes: `direnv allow .`
  - Then execute with: `direnv exec . <command>`

## LLM Runtime Modes
- Local LM Studio (`openai_compat`):
  - Base URL: `http://127.0.0.1:1234/v1`
  - Default speed model: `llama-3.3-8b-instruct-omniwriter`
  - Optional quality model (slower): `qwen3-vl-30b-a3b-instruct-mlx`
- Ollama (`ollama_native`) is supported by the local warm-cache probe script.
- Hybrid mode (`--llm-provider=hybrid`) mixes:
  - local: `openai_compat`
  - remote: `openrouter_http`

## Repo Path Layout
- Active docs only: `docs/branding/`
- Static inputs/templates/examples: `resources/branding/`
- Runtime outputs and mutable DBs: `test_outputs/branding/`
- Historical artifacts (not canonical docs): `artifacts/branding/legacy/2026-02/`
- Archived obsolete docs: `docs/archive/branding/2026/`

## Recommended Run Pattern
- Always prefer `direnv exec .` for campaign runs so remote credentials are present when needed.
- Keep local model resident with:
  - `--llm-openai-ttl-s=<seconds>`
  - `--llm-openai-keep-alive=<duration>`

## Useful Commands
- Show runner flags:
  - `python3 scripts/branding/naming_campaign_runner.py --help`
- Local warm-cache probe (LM Studio):
  - `python3 scripts/branding/test_local_llm_warm_cache.py --provider=openai_compat --base-url=http://127.0.0.1:1234/v1 --model=llama-3.3-8b-instruct-omniwriter`
- One-command Ollama smoke (probe + campaign):
  - `zsh scripts/branding/test_ollama_local_smoke.sh --model gemma3:12b --keep-alive 30m`
- One-command 3-model benchmark table:
  - `zsh scripts/branding/benchmark_local_llm_profiles.sh`
- One-command hybrid shortcuts:
  - `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh`
  - `zsh scripts/branding/run_hybrid_ollama_mistral.sh`
  - Fast profile: `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --fast`
  - Quality profile: `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --quality`
- Hybrid campaign example:
  - `direnv exec . python3 scripts/branding/naming_campaign_runner.py --llm-ideation-enabled --llm-provider=hybrid --llm-hybrid-local-models=llama-3.3-8b-instruct-omniwriter --llm-hybrid-remote-models=mistralai/mistral-small-creative --llm-hybrid-local-share=0.75 --llm-openai-base-url=http://127.0.0.1:1234/v1 --llm-openai-ttl-s=3600`
