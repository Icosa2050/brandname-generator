# Continuous Pipeline Deferred Backlog

This document tracks intentionally deferred work while we prioritize throughput testing and robust operations.

## Deferred now
1. Two-lane architecture (`exploration` vs `promotion`)
- Why deferred:
  - Current loop already supports profile rotation (`fast`, `quality`) and can generate calibration data first.
  - We need empirical yield numbers before locking architecture.
- Trigger to implement:
  - Strict survivor yield remains below target after a 24h soak + profile tuning.

2. Per-check retry/adaptive policy for expensive validators
- Why deferred:
  - First gather baseline timeout/error rates under the new strict-target loop.
  - Avoid tuning retries blindly and increasing runtime without evidence.
- Trigger to implement:
  - Expensive-check timeout/error rates are a top-3 bottleneck in soak metrics.

3. Deeper scoring/recommendation recalibration
- Why deferred:
  - Existing rubric is already producing `strong/consider`; current bottleneck is expensive-check survivability.
  - Need outcome labels from shortlisted strict survivors before threshold retuning.
- Trigger to implement:
  - Large gap between manual “human-good” judgments and current strict survivor rankings.

## Not deferred (already implemented)
- Strict target gating in continuous supervisor:
  - Targets now use strict survivors (no expensive `fail/error`) rather than raw `checked` counts.
- LaunchAgent completion semantics:
  - Service restarts on failure but not on clean target completion.
- Stale lock recovery:
  - Supervisor reclaims stale lock when PID is no longer alive.
