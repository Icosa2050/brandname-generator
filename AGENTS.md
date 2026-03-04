# Agent Notes

## Shared Policies
- If available, load and follow the second-brain policy from `$CODEX_POLICY_PATH` or `~/.codex/policies/second-brain.md`.
- This project `AGENTS.md` may define stricter local rules; on conflict, local project rules win.

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
  - Creative profile (more OpenRouter share + longer-name bias): `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --creative`
  - Optional remote-model mix: `zsh scripts/branding/run_hybrid_lmstudio_mistral.sh --creative --remote-models mistralai/mistral-small-creative,anthropic/claude-sonnet-4.5`
  - Continuous supervisor (foreground): `zsh scripts/branding/run_continuous_branding_supervisor.sh --backend auto --fallback-backend ollama --profile-plan fast,quality,creative --target-good 120 --target-strong 40`
  - Continuous supervisor (LaunchAgent): `zsh scripts/branding/install_launchd_continuous_branding.sh --install`
  - Progress summary: `zsh scripts/branding/report_campaign_progress.sh --out-dir test_outputs/branding/continuous_hybrid --top-n 25`
  - Continuous targets are strict survivors (checked recommendation + full expensive-check pass/warn coverage + no expensive-check fail/error).
- Hybrid campaign example:
  - `direnv exec . python3 scripts/branding/naming_campaign_runner.py --llm-ideation-enabled --llm-provider=hybrid --llm-hybrid-local-models=llama-3.3-8b-instruct-omniwriter --llm-hybrid-remote-models=mistralai/mistral-small-creative --llm-hybrid-local-share=0.75 --llm-openai-base-url=http://127.0.0.1:1234/v1 --llm-openai-ttl-s=3600`

## Git & Worktree Discipline (PR-First)
- Do not start task work on `main` unless explicitly requested.
- Use one dedicated branch + worktree per task/thread (`codex/<task-slug>`), created from `origin/main`:
  - `git fetch origin --prune`
  - `git worktree add -b codex/<task-slug> <worktree-path> origin/main`
- Mandatory startup check before edits or task tests:
  - `git rev-parse --abbrev-ref HEAD`
  - If output is `main` or `HEAD`, stop and switch/create the task worktree first.
- Branch/worktree lock per thread: once work starts, do not switch branch/worktree in that thread unless explicitly requested.
- Required PR flow: implement -> run relevant tests -> commit -> push -> open PR -> merge.
- Mandatory post-merge cleanup for completed tasks:
  - `git fetch origin --prune`
  - `git branch --merged origin/main`
  - `git branch -d codex/<task-slug>`
  - `git worktree remove <worktree-path>`
  - `git worktree prune`
- Detached worktrees are temporary only (inspection/rescue). Before finishing a session:
  - If clean, remove the detached worktree.
  - If dirty, attach changes to a rescue branch before cleanup:
    - `git -C <worktree-path> switch -c codex/rescue-<date>-<slug>`
- Hygiene cadence (recommended at least weekly):
  - `zsh scripts/branding/git_worktree_hygiene.sh` (report-only)
  - `zsh scripts/branding/git_worktree_hygiene.sh --apply` (safe cleanup)
  - `git worktree list --porcelain`
  - `git branch -vv --all`
  - `git remote prune origin`
