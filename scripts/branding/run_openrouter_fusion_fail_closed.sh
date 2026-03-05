#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

QUALITY_OUT_DIR="${BRANDING_AUTOMATION_QUALITY_OUT_DIR:-test_outputs/branding/automation_openrouter_quality_v4}"
REMOTE_OUT_DIR="${BRANDING_AUTOMATION_REMOTE_OUT_DIR:-test_outputs/branding/automation_openrouter_remote_quality_v4}"
FUSED_OUT_DIR="${BRANDING_AUTOMATION_FUSED_OUT_DIR:-test_outputs/branding/automation_openrouter_fused_v4}"

if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
  echo "OPENROUTER_API_KEY is not set; refusing to run." >&2
  exit 2
fi

QUALITY_LOG="$(mktemp)"
REMOTE_LOG="$(mktemp)"

cleanup_logs() {
  rm -f "$QUALITY_LOG" "$REMOTE_LOG"
}
trap cleanup_logs EXIT

run_quality_lane() {
  zsh scripts/branding/run_openrouter_lane.sh \
    --profile quality \
    --lane 0 \
    --shard-count 1 \
    --bundle-a \
    --max-runs 1 \
    --no-live-progress \
    --post-rank-top-n 30 \
    --out-dir "$QUALITY_OUT_DIR" \
    -- \
    --llm-max-call-latency-ms 45000 \
    --llm-stage-timeout-ms 180000 \
    --llm-max-retries 1
}

run_remote_lane() {
  zsh scripts/branding/run_openrouter_lane.sh \
    --profile remote_quality \
    --lane 0 \
    --shard-count 1 \
    --bundle-a \
    --max-runs 1 \
    --no-live-progress \
    --post-rank-top-n 30 \
    --out-dir "$REMOTE_OUT_DIR" \
    -- \
    --llm-max-call-latency-ms 60000 \
    --llm-stage-timeout-ms 240000 \
    --llm-max-retries 1
}

echo "starting parallel lanes: quality + remote_quality"
run_quality_lane_with_tee() {
  run_quality_lane 2>&1 | tee "$QUALITY_LOG"
}

run_remote_lane_with_tee() {
  run_remote_lane 2>&1 | tee "$REMOTE_LOG"
}

run_quality_lane_with_tee &
quality_pid=$!
run_remote_lane_with_tee &
remote_pid=$!

set +e
wait "$quality_pid"
quality_rc=$?
wait "$remote_pid"
remote_rc=$?
set -e

if [[ "$quality_rc" -ne 0 ]]; then
  echo "fail_closed: quality lane failed (rc=$quality_rc)." >&2
  echo "fail_closed: quality lane log follows." >&2
  cat "$QUALITY_LOG" >&2
  exit "$quality_rc"
fi
if [[ "$remote_rc" -ne 0 ]]; then
  echo "fail_closed: remote_quality lane failed (rc=$remote_rc)." >&2
  echo "fail_closed: remote_quality lane log follows." >&2
  cat "$REMOTE_LOG" >&2
  exit "$remote_rc"
fi

python3 scripts/branding/fuse_postrank_profiles.py \
  --quality-out-dir "$QUALITY_OUT_DIR" \
  --remote-quality-out-dir "$REMOTE_OUT_DIR" \
  --out-dir "$FUSED_OUT_DIR" \
  --top-n 40
