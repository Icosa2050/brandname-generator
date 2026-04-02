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
- Active docs only: `docs/brandpipe/`
- Supported pipeline name: `brandpipe`
- Supported `brandpipe` configs, prompts, and fixtures: `resources/brandpipe/`
- Supported `brandpipe` helper scripts: `scripts/brandpipe/`
- Supported `brandpipe` runtime outputs and mutable DBs: `test_outputs/brandpipe/`
- Historical artifacts (not canonical docs): `artifacts/branding/legacy/2026-02/`
- Archived obsolete docs: `docs/archive/branding/2026/`

## Recommended Run Pattern
- Always prefer `direnv exec .` for campaign runs so remote credentials are present when needed.
- Keep local model resident with:
  - `--llm-openai-ttl-s=<seconds>`
  - `--llm-openai-keep-alive=<duration>`

## Useful Commands
- Show CLI flags:
  - `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli --help`
- Run the supported generation lane:
  - `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli run --config resources/brandpipe/fixture_basic_run.toml`
- Run the supported validation lane:
  - `direnv exec . env PYTHONPATH=src python3 -m brandpipe.cli validate --input-csv <csv> --out-dir test_outputs/brandpipe/validate/manual`
- Local warm-cache probe (LM Studio):
  - `python3 scripts/brandpipe/local_llm_warm_cache_probe.py --provider=openai_compat --base-url=http://127.0.0.1:1234/v1 --model=llama-3.3-8b-instruct-omniwriter`
- One-command 3-model benchmark table:
  - `zsh scripts/brandpipe/benchmark_local_llm_profiles.sh`

## Git & Worktree Discipline (PR-First, codex.app compatible)
- Preferred flow for local CLI/manual work: use one dedicated branch + worktree per task/thread (`codex/<task-slug>`), created from `origin/main`:
  - `git fetch origin --prune`
  - `git worktree add -b codex/<task-slug> <worktree-path> origin/main`
- `codex.app` compatibility rule: if Codex is already running inside a managed repo checkout on `main`, do not stop work solely because the current branch is `main`. In that environment, continue the task in place unless the user explicitly asks for branch/worktree setup.
- `main` is not a hard blocker for Codex App sessions. When launched on `main` or `HEAD`, do not refuse the task solely because of branch state.
- For local CLI/manual sessions, startup check before edits or task tests:
  - `git rev-parse --abbrev-ref HEAD`
  - If output is `main` or `HEAD`, prefer creating/switching to the task worktree first.
- In a managed `codex.app` checkout already on `main`, treat `main` or `HEAD` as a prompt to assess risk, not as an unconditional stop:
  - For multi-step coding work, git-heavy changes, or anything likely to become a PR, create/switch to a dedicated task branch/worktree before editing when practical.
  - For small doc/config tweaks, inspection, or one-off local maintenance explicitly requested in the current checkout, it is acceptable to continue in place.
- Branch/worktree lock per thread: once work starts, do not switch branch/worktree in that thread unless explicitly requested.
- Preferred PR flow when branch workflow is in use: implement -> run relevant tests -> commit -> push -> open PR -> merge.
- If work was done directly in a `codex.app` managed checkout on `main`, avoid inventing extra cleanup or branch-migration steps inside the task unless the user asks for them.
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
  - `zsh scripts/brandpipe/git_worktree_hygiene.sh` (report-only)
  - `zsh scripts/brandpipe/git_worktree_hygiene.sh --apply` (safe cleanup)
  - `git worktree list --porcelain`
  - `git branch -vv --all`
  - `git remote prune origin`

## Persistent Automation Worktrees (do not remove)
- Keep these worktrees present even during cleanup; active automations depend on them:
  - `~/.codex/worktrees/automation-branding-fusion/brandname-generator`
  - `~/.codex/worktrees/automation-branding-health/brandname-generator`
- Current mapping:
  - `branding-fusion-run` (generation lane) -> `automation-branding-fusion`
  - `branding-fusion-run-2` (fusion lane) -> `automation-branding-fusion`
  - `creative-run-check` (validation lane) -> `automation-branding-health`
- Cleanup rule: never remove either path unless the linked automations are paused or re-pointed first.
