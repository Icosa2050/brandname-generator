#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

MODEL="${LMSTUDIO_MODEL:-llama-3.3-8b-instruct-omniwriter}"
BASE_URL="${LMSTUDIO_BASE_URL:-http://127.0.0.1:1234/v1}"
TTL_S="${LMSTUDIO_TTL_S:-3600}"
PROBE_RUNS="${LMSTUDIO_PROBE_RUNS:-6}"
PROBE_GAP_S="${LMSTUDIO_PROBE_GAP_S:-1}"
PROBE_EVICTION_GAP_S="${LMSTUDIO_PROBE_EVICTION_GAP_S:-90}"
PROBE_TIMEOUT_S="${LMSTUDIO_PROBE_TIMEOUT_S:-60}"
CAMPAIGN_ROUNDS="${LMSTUDIO_CAMPAIGN_ROUNDS:-2}"
CAMPAIGN_CANDIDATES_PER_ROUND="${LMSTUDIO_CAMPAIGN_CANDIDATES_PER_ROUND:-20}"
OUT_DIR="${LMSTUDIO_OUT_DIR:-}"
RUN_PROBE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --model)
      MODEL="$2"
      shift 2
      ;;
    --base-url)
      BASE_URL="$2"
      shift 2
      ;;
    --ttl-s)
      TTL_S="$2"
      shift 2
      ;;
    --probe-runs)
      PROBE_RUNS="$2"
      shift 2
      ;;
    --probe-gap-s)
      PROBE_GAP_S="$2"
      shift 2
      ;;
    --probe-eviction-gap-s)
      PROBE_EVICTION_GAP_S="$2"
      shift 2
      ;;
    --probe-timeout-s)
      PROBE_TIMEOUT_S="$2"
      shift 2
      ;;
    --campaign-rounds)
      CAMPAIGN_ROUNDS="$2"
      shift 2
      ;;
    --campaign-candidates-per-round)
      CAMPAIGN_CANDIDATES_PER_ROUND="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --skip-probe)
      RUN_PROBE=0
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: scripts/branding/test_lmstudio_local_smoke.sh [options]

Runs:
1) local LM Studio warm/cold probe
2) one-run local campaign smoke against openai_compat endpoint

Options:
  --model <id>                          LM Studio model identifier
  --base-url <url>                      OpenAI-compatible base URL
  --ttl-s <seconds>                     Residency TTL hint
  --probe-runs <n>                      Probe sequential call count
  --probe-gap-s <seconds>               Gap between probe calls
  --probe-eviction-gap-s <seconds>      Idle gap before post-idle probe
  --probe-timeout-s <seconds>           Probe per-call timeout
  --campaign-rounds <n>                 LLM rounds in campaign smoke
  --campaign-candidates-per-round <n>   Candidates requested each round
  --out-dir <path>                      Campaign output root
  --skip-probe                          Skip probe stage
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="/tmp/branding_lmstudio_smoke_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$OUT_DIR"

PROBE_JSON="$OUT_DIR/lmstudio_probe.json"
PROBE_LOG="$OUT_DIR/lmstudio_probe.log"
CAMPAIGN_LOG="$OUT_DIR/campaign.stdout.log"

echo "lmstudio_smoke_config model=$MODEL base_url=$BASE_URL ttl_s=$TTL_S out_dir=$OUT_DIR"

echo "[0/2] Running preflight checks..."
if ! zsh "$ROOT_DIR/scripts/branding/preflight_llm.sh" \
  --check-lmstudio \
  --lmstudio-base-url="$BASE_URL" \
  --lmstudio-model="$MODEL"; then
  echo "LMSTUDIO_LOCAL_SMOKE FAIL stage=preflight base_url=$BASE_URL model=$MODEL"
  exit 2
fi

if [[ "$RUN_PROBE" == "1" ]]; then
  echo "[1/2] Running LM Studio warm/cold probe..."
  if ! python3 "$ROOT_DIR/scripts/branding/test_local_llm_warm_cache.py" \
    --provider=openai_compat \
    --base-url="$BASE_URL" \
    --model="$MODEL" \
    --ttl-s="$TTL_S" \
    --runs="$PROBE_RUNS" \
    --gap-s="$PROBE_GAP_S" \
    --eviction-gap-s="$PROBE_EVICTION_GAP_S" \
    --timeout-s="$PROBE_TIMEOUT_S" \
    --output-json="$PROBE_JSON" \
    > "$PROBE_LOG" 2>&1; then
    echo "LMSTUDIO_LOCAL_SMOKE FAIL stage=probe log=$PROBE_LOG"
    cat "$PROBE_LOG"
    exit 3
  fi
else
  echo "[1/2] Probe skipped."
fi

echo "[2/2] Running one-run campaign smoke..."
if ! python3 "$ROOT_DIR/scripts/branding/naming_campaign_runner.py" \
  --hours=0.04 \
  --max-runs=1 \
  --sleep-s=0 \
  --no-mini-test \
  --generator-no-external-checks \
  --generator-only-llm-candidates \
  --llm-ideation-enabled \
  --llm-provider=openai_compat \
  --llm-model="$MODEL" \
  --llm-openai-base-url="$BASE_URL" \
  --llm-openai-ttl-s="$TTL_S" \
  --llm-rounds="$CAMPAIGN_ROUNDS" \
  --llm-candidates-per-round="$CAMPAIGN_CANDIDATES_PER_ROUND" \
  --llm-max-call-latency-ms=30000 \
  --llm-stage-timeout-ms=70000 \
  --validator-checks=adversarial,psych,descriptive \
  --validator-tier=cheap \
  --validator-candidate-limit=25 \
  --validator-concurrency=4 \
  --out-dir="$OUT_DIR" \
  > "$CAMPAIGN_LOG" 2>&1; then
  echo "LMSTUDIO_LOCAL_SMOKE FAIL stage=campaign log=$CAMPAIGN_LOG out_dir=$OUT_DIR"
  tail -n 120 "$CAMPAIGN_LOG"
  exit 4
fi

SUMMARY_JSON="$OUT_DIR/campaign_summary.json"
if [[ ! -f "$SUMMARY_JSON" ]]; then
  echo "LMSTUDIO_LOCAL_SMOKE FAIL stage=campaign_summary_missing out_dir=$OUT_DIR log=$CAMPAIGN_LOG"
  exit 5
fi

if [[ "$RUN_PROBE" == "1" ]]; then
  read -r PROBE_COLD_MS PROBE_WARM_MEDIAN_MS PROBE_POST_IDLE_MS <<<"$(python3 - "$PROBE_JSON" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, 'r', encoding='utf-8') as handle:
    payload = json.load(handle)
summary = payload.get('summary', {})
print(
    f"{summary.get('cold_elapsed_ms', '')} "
    f"{summary.get('warm_median_ms', '')} "
    f"{summary.get('post_idle_elapsed_ms', '')}"
)
PY
)"
else
  PROBE_COLD_MS="skipped"
  PROBE_WARM_MEDIAN_MS="skipped"
  PROBE_POST_IDLE_MS="skipped"
fi

read -r CAMPAIGN_STATUS CAMPAIGN_ERRORS UNIQUE_SHORTLIST RUNS_EXECUTED <<<"$(python3 - "$SUMMARY_JSON" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, 'r', encoding='utf-8') as handle:
    payload = json.load(handle)
print(
    f"{payload.get('status', '')} "
    f"{payload.get('errors', '')} "
    f"{payload.get('unique_shortlist_names', '')} "
    f"{payload.get('runs_executed', '')}"
)
PY
)"

if [[ "$CAMPAIGN_STATUS" != "ok" ]]; then
  echo "LMSTUDIO_LOCAL_SMOKE FAIL stage=campaign_status status=$CAMPAIGN_STATUS errors=$CAMPAIGN_ERRORS summary=$SUMMARY_JSON log=$CAMPAIGN_LOG"
  exit 6
fi

echo "LMSTUDIO_LOCAL_SMOKE PASS probe_cold_ms=$PROBE_COLD_MS probe_warm_median_ms=$PROBE_WARM_MEDIAN_MS probe_post_idle_ms=$PROBE_POST_IDLE_MS campaign_status=$CAMPAIGN_STATUS runs=$RUNS_EXECUTED shortlist_unique=$UNIQUE_SHORTLIST out_dir=$OUT_DIR probe_json=$PROBE_JSON campaign_log=$CAMPAIGN_LOG"
