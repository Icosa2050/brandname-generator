---
owner: engineering
status: draft
last_validated: 2026-02-25
---

# Background Daemon Setup (macOS, Linux, Windows)

## Does daemon mode make sense?
Yes for unattended output accumulation, provided guardrails are in place:
- stop thresholds (`target-good`, `target-strong`),
- API spend limits,
- backend health checks + fallback,
- periodic archival of non-review logs.

When not to daemonize:
- short exploratory runs (<1 hour),
- active prompt tuning loops,
- strict budget constraints without enforced caps.

This recommendation is aligned with a PAL Opus review of long-running pipeline operations.

## Decision Matrix
Use foreground/manual when:
- you are tuning parameters,
- you need immediate interactive feedback,
- run duration is short.

Use background daemon when:
- run is multi-hour or continuous,
- config is stable,
- outputs are accumulated and reviewed periodically.

## macOS (LaunchAgent)
Use existing installer script:

```zsh
zsh scripts/branding/install_launchd_continuous_branding.sh --install
zsh scripts/branding/install_launchd_continuous_branding.sh --status
```

Custom example:

```zsh
zsh scripts/branding/install_launchd_continuous_branding.sh \
  --install \
  --out-dir /Users/$USER/Development/brandname-generator/test_outputs/branding/continuous_hybrid \
  --backend auto \
  --fallback-backend ollama \
  --profile-plan fast,quality,creative \
  --max-usd-per-run 0.75 \
  --target-good 120 \
  --target-strong 40
```

Uninstall:

```zsh
zsh scripts/branding/install_launchd_continuous_branding.sh --uninstall
```

## Linux (systemd user service)
Create `~/.config/systemd/user/brandname-generator.service`:

```ini
[Unit]
Description=Brandname Generator Continuous Supervisor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/ABS/PATH/brandname-generator
ExecStart=/bin/zsh /ABS/PATH/brandname-generator/scripts/branding/run_continuous_branding_supervisor.sh --out-dir /ABS/PATH/brandname-generator/test_outputs/branding/continuous_hybrid --backend auto --fallback-backend ollama --profile-plan fast,quality,creative --max-usd-per-run 0.75 --target-good 120 --target-strong 40
Restart=on-failure
RestartSec=30
Environment=PATH=/usr/local/bin:/usr/bin:/bin
# Optional hard limits:
MemoryMax=4G
CPUQuota=80%

[Install]
WantedBy=default.target
```

Activate:

```bash
systemctl --user daemon-reload
systemctl --user enable --now brandname-generator.service
loginctl enable-linger "$USER"
systemctl --user status brandname-generator.service
```

## Windows (Task Scheduler)
PowerShell example (user logon trigger):

```powershell
$project = 'C:\ABS\PATH\brandname-generator'
$script = "$project\scripts\branding\run_continuous_branding_supervisor.sh"
$args = "--out-dir $project\test_outputs\branding\continuous_hybrid --backend auto --fallback-backend ollama --profile-plan fast,quality,creative --max-usd-per-run 0.75 --target-good 120 --target-strong 40"

$action = New-ScheduledTaskAction -Execute "C:\Program Files\Git\bin\bash.exe" -Argument "$script $args" -WorkingDirectory $project
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit (New-TimeSpan -Hours 0)

Register-ScheduledTask -TaskName "BrandnameGeneratorContinuous" -Action $action -Trigger $trigger -Settings $settings
```

Note:
- Windows setup depends on shell/runtime availability (`bash` from Git for Windows or WSL). If this is a primary deployment target, a native `.ps1` supervisor wrapper is recommended.

## Operational Guardrails for All Platforms
- Use explicit output directory under `test_outputs/branding/`.
- Keep remote credentials in environment only, never in unit/plist files.
- Set run-level spend caps (`--max-usd-per-run`) in service command lines.
- Set strict stopping targets to avoid endless unattended runs.
- Run periodic progress checks:

```zsh
zsh scripts/branding/report_campaign_progress.sh --out-dir test_outputs/branding/continuous_hybrid --top-n 25
```

- Archive non-review run documents regularly:

```zsh
zsh scripts/branding/archive_run_documents.sh --out-dir test_outputs/branding/continuous_hybrid --prune
```
