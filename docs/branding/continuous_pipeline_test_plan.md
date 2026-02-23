# Continuous Pipeline Test Plan (Mostly Automated)

Goal: verify that the 24/7 loop converges toward **strict survivors** (checked `strong/consider` with no expensive-check `fail/error`).

## Scope
- Local runners:
  - `scripts/branding/run_hybrid_lmstudio_mistral.sh`
  - `scripts/branding/run_hybrid_ollama_mistral.sh`
- Continuous orchestrator:
  - `scripts/branding/run_continuous_branding_supervisor.sh`
  - `scripts/branding/install_launchd_continuous_branding.sh`
- Reporting:
  - `scripts/branding/report_campaign_progress.sh`

## Exit criteria
- Reliability:
  - No crash loops from stale lock.
  - LaunchAgent does not restart after clean target completion.
- Quality progress:
  - `strict_good` and `strict_strong` show non-zero growth over soak window.
  - Strict definition: all expensive checks (`domain, web, app_store, package, social`)
    have `pass/warn` coverage and zero `fail/error`.
- Operational:
  - Logs and metrics are inspectable from one output root.

## Automated phases
### Phase A: Static checks (no network, fast)
```zsh
zsh -n scripts/branding/run_hybrid_lmstudio_mistral.sh
zsh -n scripts/branding/run_hybrid_ollama_mistral.sh
zsh -n scripts/branding/run_continuous_branding_supervisor.sh
zsh -n scripts/branding/install_launchd_continuous_branding.sh
zsh -n scripts/branding/report_campaign_progress.sh
```

### Phase B: CLI behavior checks (no long run)
```zsh
zsh scripts/branding/run_continuous_branding_supervisor.sh --help
zsh scripts/branding/install_launchd_continuous_branding.sh --help
zsh scripts/branding/install_launchd_continuous_branding.sh --print >/tmp/branding_launchd_preview.plist
```

### Phase C: Supervisor dry-run (automated orchestration validation)
```zsh
zsh scripts/branding/run_continuous_branding_supervisor.sh \
  --dry-run \
  --max-cycles 3 \
  --sleep-ok-s 0 \
  --out-dir /tmp/branding_supervisor_dryrun \
  --backend auto \
  --fallback-backend ollama \
  --profile-plan fast,quality \
  --target-good 9999 \
  --target-strong 9999
```

Expected:
- Logs include `event=cycle_start`, `event=cycle_ok`, `event=stop`.
- `strict_*` fields appear in `cycle_ok` log lines.

### Phase D: Short live smoke (automated, external deps required)
Prereqs:
- LM Studio and/or Ollama available.
- `OPENROUTER_API_KEY` present via `direnv`.

```zsh
zsh scripts/branding/run_continuous_branding_supervisor.sh \
  --out-dir /tmp/branding_continuous_smoke \
  --backend auto \
  --fallback-backend ollama \
  --profile-plan fast,quality \
  --max-cycles 2 \
  --sleep-ok-s 0 \
  --target-good 9999 \
  --target-strong 9999
```

Then:
```zsh
zsh scripts/branding/report_campaign_progress.sh \
  --out-dir /tmp/branding_continuous_smoke \
  --top-n 15
```

Expected:
- `strict_good` and `strict_strong` reported.
- At least one completed run in DB.

### Phase E: LaunchAgent lifecycle test (mostly automated)
```zsh
zsh scripts/branding/install_launchd_continuous_branding.sh \
  --install \
  --out-dir /Users/bernhard/Development/brandname-generator/test_outputs/branding/continuous_hybrid \
  --backend auto \
  --fallback-backend ollama \
  --profile-plan fast,fast,quality \
  --target-good 180 \
  --target-strong 70

zsh scripts/branding/install_launchd_continuous_branding.sh --status
```

Monitor:
```zsh
zsh scripts/branding/report_campaign_progress.sh \
  --out-dir /Users/bernhard/Development/brandname-generator/test_outputs/branding/continuous_hybrid \
  --top-n 25
```

Stop/remove:
```zsh
zsh scripts/branding/install_launchd_continuous_branding.sh --uninstall
```

## Soak test plan (24h)
- Keep LaunchAgent running 24h.
- Collect every 2h:
  - `strict_good`, `strict_strong`, `shortlist_strict_good`, `shortlist_unique`.
  - expensive-check fail/error mix from SQLite.
- Decision gate after 24h:
  - If strict survivor growth is adequate: continue.
  - If inadequate: implement deferred item #1 (two-lane architecture).

## Manual checks kept intentionally minimal
- Human triage of top `strict_strong` names (pronounceability, fit, legal-precheck readiness).
- Confirm at least 10 candidates move from strict pass to “human shortlisted”.
