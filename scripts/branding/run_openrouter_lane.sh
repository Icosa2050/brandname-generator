#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

LANE=0
SHARD_COUNT=2
OUT_DIR="/tmp/branding_openrouter_tuned"
MODEL="${OPENROUTER_MODEL:-mistralai/mistral-small-creative}"
MAX_RUNS=1
SLEEP_S=0
POOL_SIZE=400
CHECK_LIMIT=80
VALIDATOR_CANDIDATE_LIMIT=40
VALIDATOR_EXPENSIVE_FINALIST_LIMIT=20
VALIDATOR_CONCURRENCY=8
VALIDATOR_MIN_CONCURRENCY=4
VALIDATOR_MAX_CONCURRENCY=12
VALIDATOR_TIMEOUT_S=6
LLM_ROUNDS=1
LLM_CANDIDATES_PER_ROUND=10
LIVE_PROGRESS=1
NO_EXTERNAL_CHECKS=1
HTTP_REFERER="${OPENROUTER_HTTP_REFERER:-https://github.com/Icosa2050/brandname-generator}"
X_TITLE="${OPENROUTER_X_TITLE:-brand-name-generator}"
EXTRA_ARGS=()

usage() {
  cat <<'EOF'
Run one tuned OpenRouter campaign lane.

Usage:
  scripts/branding/run_openrouter_lane.sh [options] [-- <extra runner args>]

Options:
  --lane <n>                         Shard id (default: 0)
  --shard-count <n>                  Total shard workers (default: 2)
  --out-dir <path>                   Campaign output root (default: /tmp/branding_openrouter_tuned)
  --model <id>                       OpenRouter model id
  --max-runs <n>                     Max runs per invocation (default: 1)
  --pool-size <n>                    Generator pool size (default: 400)
  --check-limit <n>                  Generator check limit (default: 80)
  --validator-candidate-limit <n>    Validator candidate limit (default: 40)
  --validator-expensive-limit <n>    Expensive finalist limit (default: 20)
  --validator-concurrency <n>        Initial validator concurrency (default: 8)
  --validator-min-concurrency <n>    Validator adaptive min concurrency (default: 4)
  --validator-max-concurrency <n>    Validator adaptive max concurrency (default: 12)
  --validator-timeout-s <seconds>    Validator per-check timeout (default: 6)
  --llm-rounds <n>                   LLM rounds per run (default: 1)
  --llm-candidates-per-round <n>     LLM candidates requested per round (default: 10)
  --http-referer <url>               OpenRouter HTTP-Referer header
  --x-title <text>                   OpenRouter X-Title header
  --with-external-checks             Keep generator external checks enabled
  --no-live-progress                 Disable live progress stream
  -h, --help                         Show this help

Examples:
  scripts/branding/run_openrouter_lane.sh --lane 0
  scripts/branding/run_openrouter_lane.sh --lane 1 --out-dir /tmp/branding_openrouter_tuned
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lane|--shard-id)
      LANE="$2"
      shift 2
      ;;
    --shard-count)
      SHARD_COUNT="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --model)
      MODEL="$2"
      shift 2
      ;;
    --max-runs)
      MAX_RUNS="$2"
      shift 2
      ;;
    --pool-size)
      POOL_SIZE="$2"
      shift 2
      ;;
    --check-limit)
      CHECK_LIMIT="$2"
      shift 2
      ;;
    --validator-candidate-limit)
      VALIDATOR_CANDIDATE_LIMIT="$2"
      shift 2
      ;;
    --validator-expensive-limit)
      VALIDATOR_EXPENSIVE_FINALIST_LIMIT="$2"
      shift 2
      ;;
    --validator-concurrency)
      VALIDATOR_CONCURRENCY="$2"
      shift 2
      ;;
    --validator-min-concurrency)
      VALIDATOR_MIN_CONCURRENCY="$2"
      shift 2
      ;;
    --validator-max-concurrency)
      VALIDATOR_MAX_CONCURRENCY="$2"
      shift 2
      ;;
    --validator-timeout-s)
      VALIDATOR_TIMEOUT_S="$2"
      shift 2
      ;;
    --llm-rounds)
      LLM_ROUNDS="$2"
      shift 2
      ;;
    --llm-candidates-per-round)
      LLM_CANDIDATES_PER_ROUND="$2"
      shift 2
      ;;
    --http-referer)
      HTTP_REFERER="$2"
      shift 2
      ;;
    --x-title)
      X_TITLE="$2"
      shift 2
      ;;
    --with-external-checks)
      NO_EXTERNAL_CHECKS=0
      shift
      ;;
    --no-live-progress)
      LIVE_PROGRESS=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
  echo "OPENROUTER_API_KEY is not set." >&2
  exit 2
fi

if ! [[ "$LANE" =~ '^[0-9]+$' ]]; then
  echo "--lane must be a non-negative integer." >&2
  exit 2
fi
if ! [[ "$SHARD_COUNT" =~ '^[1-9][0-9]*$' ]]; then
  echo "--shard-count must be >= 1." >&2
  exit 2
fi
if (( LANE >= SHARD_COUNT )); then
  echo "--lane must be smaller than --shard-count." >&2
  exit 2
fi

CMD=(
  python3 "$ROOT_DIR/scripts/branding/naming_campaign_runner.py"
  --max-runs "$MAX_RUNS"
  --sleep-s "$SLEEP_S"
  --no-mini-test
  --pool-size "$POOL_SIZE"
  --check-limit "$CHECK_LIMIT"
  --validator-candidate-limit "$VALIDATOR_CANDIDATE_LIMIT"
  --validator-expensive-finalist-limit "$VALIDATOR_EXPENSIVE_FINALIST_LIMIT"
  --validator-concurrency "$VALIDATOR_CONCURRENCY"
  --validator-min-concurrency "$VALIDATOR_MIN_CONCURRENCY"
  --validator-max-concurrency "$VALIDATOR_MAX_CONCURRENCY"
  --validator-timeout-s "$VALIDATOR_TIMEOUT_S"
  --llm-ideation-enabled
  --llm-provider openrouter_http
  --llm-model "$MODEL"
  --llm-openrouter-http-referer "$HTTP_REFERER"
  --llm-openrouter-x-title "$X_TITLE"
  --llm-context-file "$ROOT_DIR/resources/branding/llm/llm_context.example.json"
  --llm-rounds "$LLM_ROUNDS"
  --llm-candidates-per-round "$LLM_CANDIDATES_PER_ROUND"
  --shard-id "$LANE"
  --shard-count "$SHARD_COUNT"
  --out-dir "$OUT_DIR"
)

if (( NO_EXTERNAL_CHECKS )); then
  CMD+=(--generator-no-external-checks)
fi
if (( LIVE_PROGRESS )); then
  CMD+=(--live-progress)
else
  CMD+=(--no-live-progress)
fi
if (( ${#EXTRA_ARGS[@]} > 0 )); then
  CMD+=("${EXTRA_ARGS[@]}")
fi

echo "running lane=$LANE/$SHARD_COUNT out_dir=$OUT_DIR model=$MODEL"
printf '$ '
printf '%q ' "${CMD[@]}"
echo

cd "$ROOT_DIR"
if command -v direnv >/dev/null 2>&1; then
  exec direnv exec . "${CMD[@]}"
fi
exec "${CMD[@]}"
