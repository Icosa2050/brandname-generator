#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

LANE=0
SHARD_COUNT=2
OUT_DIR="/tmp/branding_openrouter_tuned"
BUNDLE_A="mistralai/mistral-small-creative,qwen/qwen3-next-80b-a3b-instruct,anthropic/claude-sonnet-4.6"
BUNDLE_B="qwen/qwen3-next-80b-a3b-instruct,anthropic/claude-sonnet-4.6"
BUNDLE_C="mistralai/mistral-small-creative,qwen/qwen3-next-80b-a3b-instruct,openai/gpt-5.2"
MODEL="${OPENROUTER_MODEL:-$BUNDLE_A}"
MODEL_SELECTION="${OPENROUTER_MODEL_SELECTION:-random}"
PROFILE="${OPENROUTER_PROFILE:-quality}"
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
VALIDATOR_CHECKS="${OPENROUTER_VALIDATOR_CHECKS:-adversarial,psych,descriptive,tm_cheap,company_cheap,domain,web,web_google_like,tm_registry_global,app_store,package,social}"
LLM_ROUNDS=1
LLM_CANDIDATES_PER_ROUND=10
LLM_TEMPERATURE="${OPENROUTER_LLM_TEMPERATURE:-0.8}"
LLM_MAX_CALL_LATENCY_MS="${OPENROUTER_LLM_MAX_CALL_LATENCY_MS:-60000}"
LLM_STAGE_TIMEOUT_MS="${OPENROUTER_LLM_STAGE_TIMEOUT_MS:-180000}"
LLM_MAX_RETRIES="${OPENROUTER_LLM_MAX_RETRIES:-1}"
PROMPT_TEMPLATE_FILE="${OPENROUTER_PROMPT_TEMPLATE_FILE:-}"
LIVE_PROGRESS=1
NO_EXTERNAL_CHECKS=1
POST_RANK=1
POST_RANK_TOP_N="${OPENROUTER_POST_RANK_TOP_N:-40}"
POST_RANK_INCLUDE_NON_SHORTLIST=0
HEALTH_CHECK=1
HEALTH_MIN_NEW_SHORTLIST="${OPENROUTER_HEALTH_MIN_NEW_SHORTLIST:-}"
HEALTH_MIN_NEW_SHORTLIST_QUALITY="${OPENROUTER_HEALTH_MIN_NEW_SHORTLIST_QUALITY:-6}"
HEALTH_MIN_NEW_SHORTLIST_REMOTE_QUALITY="${OPENROUTER_HEALTH_MIN_NEW_SHORTLIST_REMOTE_QUALITY:-10}"
HTTP_REFERER="${OPENROUTER_HTTP_REFERER:-https://github.com/Icosa2050/brandname-generator}"
X_TITLE="${OPENROUTER_X_TITLE:-brand-name-generator}"
EXTRA_ARGS=()
USER_MODEL=""
USER_MODEL_SELECTION=""
USER_LLM_ROUNDS=""
USER_LLM_CANDIDATES_PER_ROUND=""
USER_LLM_TEMPERATURE=""
USER_LLM_MAX_CALL_LATENCY_MS=""
USER_LLM_STAGE_TIMEOUT_MS=""
USER_LLM_MAX_RETRIES=""
USER_VALIDATOR_CANDIDATE_LIMIT=""
USER_VALIDATOR_EXPENSIVE_FINALIST_LIMIT=""
USER_VALIDATOR_CONCURRENCY=""
USER_VALIDATOR_MIN_CONCURRENCY=""
USER_VALIDATOR_MAX_CONCURRENCY=""
USER_VALIDATOR_TIMEOUT_S=""
USER_VALIDATOR_CHECKS=""
USER_PROMPT_TEMPLATE_FILE=""

usage() {
  cat <<'EOF'
Run one tuned OpenRouter campaign lane.

Usage:
  scripts/branding/run_openrouter_lane.sh [options] [-- <extra runner args>]

Options:
  --profile <fast|quality|remote_quality> Apply tuning preset (default: quality)
  --lane <n>                         Shard id (default: 0)
  --shard-count <n>                  Total shard workers (default: 2)
  --out-dir <path>                   Campaign output root (default: /tmp/branding_openrouter_tuned)
  --model <id|csv>                   OpenRouter model id(s)
  --models <csv>                     Alias for --model
  --bundle-a                         Use model Bundle A (default)
  --bundle-b                         Use model Bundle B
  --bundle-c                         Use model Bundle C
  --model-selection <mode>           round_robin|random (default: random)
  --max-runs <n>                     Max runs per invocation (default: 1)
  --pool-size <n>                    Generator pool size (default: 400)
  --check-limit <n>                  Generator check limit (default: 80)
  --validator-candidate-limit <n>    Validator candidate limit (default: 36 via profile=quality)
  --validator-expensive-limit <n>    Expensive finalist limit (default: 16 via profile=quality)
  --validator-concurrency <n>        Initial validator concurrency (default: 6 via profile=quality)
  --validator-min-concurrency <n>    Validator adaptive min concurrency (default: 3 via profile=quality)
  --validator-max-concurrency <n>    Validator adaptive max concurrency (default: 8 via profile=quality)
  --validator-timeout-s <seconds>    Validator per-check timeout (default: 30 via profile=quality)
  --validator-checks <csv>           Explicit validator checks list
  --llm-rounds <n>                   LLM rounds per run (default: 1)
  --llm-candidates-per-round <n>     LLM candidates requested per round (default: 10)
  --llm-temperature <float>          LLM sampling temperature (default: profile-specific)
  --llm-max-call-latency-ms <ms>     Per-call LLM timeout (default: profile-specific)
  --llm-stage-timeout-ms <ms>        Total ideation-stage timeout (default: profile-specific)
  --llm-max-retries <n>              Retriable LLM call retries (default: profile-specific)
  --llm-prompt-template-file <path>  Optional prompt template file
  --post-rank                        Run deterministic DE/EN post-ranker (default: on)
  --no-post-rank                     Skip deterministic post-ranker
  --post-rank-top-n <n>              Names kept by post-ranker (default: 40)
  --post-rank-all                    Score all names, not only shortlist-selected ones
  --health-check                     Run post-run health checks (default: on)
  --no-health-check                  Skip post-run health checks
  --health-min-new-shortlist <n>     Override minimum new shortlist names for health check
  --health-min-new-shortlist-quality <n>
                                     Quality-profile minimum new shortlist names (default: 6)
  --health-min-new-shortlist-remote-quality <n>
                                     Remote-quality minimum new shortlist names (default: 10)
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

apply_profile() {
  case "$PROFILE" in
    ""|"custom")
      ;;
    "fast")
      LLM_ROUNDS="2"
      LLM_CANDIDATES_PER_ROUND="10"
      LLM_TEMPERATURE="0.8"
      LLM_MAX_CALL_LATENCY_MS="30000"
      LLM_STAGE_TIMEOUT_MS="90000"
      LLM_MAX_RETRIES="1"
      VALIDATOR_CANDIDATE_LIMIT="28"
      VALIDATOR_EXPENSIVE_FINALIST_LIMIT="10"
      VALIDATOR_TIMEOUT_S="6"
      VALIDATOR_CONCURRENCY="8"
      VALIDATOR_MIN_CONCURRENCY="4"
      VALIDATOR_MAX_CONCURRENCY="12"
      PROMPT_TEMPLATE_FILE=""
      ;;
    "quality")
      LLM_ROUNDS="3"
      LLM_CANDIDATES_PER_ROUND="14"
      LLM_TEMPERATURE="0.7"
      LLM_MAX_CALL_LATENCY_MS="60000"
      LLM_STAGE_TIMEOUT_MS="180000"
      LLM_MAX_RETRIES="1"
      VALIDATOR_CANDIDATE_LIMIT="36"
      VALIDATOR_EXPENSIVE_FINALIST_LIMIT="16"
      VALIDATOR_TIMEOUT_S="30"
      VALIDATOR_CONCURRENCY="6"
      VALIDATOR_MIN_CONCURRENCY="3"
      VALIDATOR_MAX_CONCURRENCY="8"
      PROMPT_TEMPLATE_FILE="$ROOT_DIR/resources/branding/llm/llm_prompt.constrained_pronounceable_de_en_v3.txt"
      ;;
    "remote_quality")
      LLM_ROUNDS="6"
      LLM_CANDIDATES_PER_ROUND="14"
      LLM_TEMPERATURE="1.05"
      LLM_MAX_CALL_LATENCY_MS="90000"
      LLM_STAGE_TIMEOUT_MS="240000"
      LLM_MAX_RETRIES="2"
      VALIDATOR_CANDIDATE_LIMIT="48"
      VALIDATOR_EXPENSIVE_FINALIST_LIMIT="24"
      VALIDATOR_TIMEOUT_S="30"
      VALIDATOR_CONCURRENCY="6"
      VALIDATOR_MIN_CONCURRENCY="3"
      VALIDATOR_MAX_CONCURRENCY="8"
      PROMPT_TEMPLATE_FILE="$ROOT_DIR/resources/branding/llm/llm_prompt.constrained_pronounceable_de_en_v3.txt"
      ;;
    *)
      echo "Unknown profile: $PROFILE (expected fast|quality|remote_quality)." >&2
      exit 2
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
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
      USER_MODEL="$2"
      shift 2
      ;;
    --models)
      MODEL="$2"
      USER_MODEL="$2"
      shift 2
      ;;
    --bundle-a)
      MODEL="$BUNDLE_A"
      USER_MODEL="$BUNDLE_A"
      shift
      ;;
    --bundle-b)
      MODEL="$BUNDLE_B"
      USER_MODEL="$BUNDLE_B"
      shift
      ;;
    --bundle-c)
      MODEL="$BUNDLE_C"
      USER_MODEL="$BUNDLE_C"
      shift
      ;;
    --model-selection)
      MODEL_SELECTION="$2"
      USER_MODEL_SELECTION="$2"
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
      USER_VALIDATOR_CANDIDATE_LIMIT="$2"
      shift 2
      ;;
    --validator-expensive-limit)
      VALIDATOR_EXPENSIVE_FINALIST_LIMIT="$2"
      USER_VALIDATOR_EXPENSIVE_FINALIST_LIMIT="$2"
      shift 2
      ;;
    --validator-concurrency)
      VALIDATOR_CONCURRENCY="$2"
      USER_VALIDATOR_CONCURRENCY="$2"
      shift 2
      ;;
    --validator-min-concurrency)
      VALIDATOR_MIN_CONCURRENCY="$2"
      USER_VALIDATOR_MIN_CONCURRENCY="$2"
      shift 2
      ;;
    --validator-max-concurrency)
      VALIDATOR_MAX_CONCURRENCY="$2"
      USER_VALIDATOR_MAX_CONCURRENCY="$2"
      shift 2
      ;;
    --validator-timeout-s)
      VALIDATOR_TIMEOUT_S="$2"
      USER_VALIDATOR_TIMEOUT_S="$2"
      shift 2
      ;;
    --validator-checks)
      VALIDATOR_CHECKS="$2"
      USER_VALIDATOR_CHECKS="$2"
      shift 2
      ;;
    --llm-rounds)
      LLM_ROUNDS="$2"
      USER_LLM_ROUNDS="$2"
      shift 2
      ;;
    --llm-candidates-per-round)
      LLM_CANDIDATES_PER_ROUND="$2"
      USER_LLM_CANDIDATES_PER_ROUND="$2"
      shift 2
      ;;
    --llm-temperature)
      LLM_TEMPERATURE="$2"
      USER_LLM_TEMPERATURE="$2"
      shift 2
      ;;
    --llm-max-call-latency-ms)
      LLM_MAX_CALL_LATENCY_MS="$2"
      USER_LLM_MAX_CALL_LATENCY_MS="$2"
      shift 2
      ;;
    --llm-stage-timeout-ms)
      LLM_STAGE_TIMEOUT_MS="$2"
      USER_LLM_STAGE_TIMEOUT_MS="$2"
      shift 2
      ;;
    --llm-max-retries)
      LLM_MAX_RETRIES="$2"
      USER_LLM_MAX_RETRIES="$2"
      shift 2
      ;;
    --llm-prompt-template-file)
      PROMPT_TEMPLATE_FILE="$2"
      USER_PROMPT_TEMPLATE_FILE="$2"
      shift 2
      ;;
    --post-rank)
      POST_RANK=1
      shift
      ;;
    --no-post-rank)
      POST_RANK=0
      shift
      ;;
    --post-rank-top-n)
      POST_RANK_TOP_N="$2"
      shift 2
      ;;
    --post-rank-all)
      POST_RANK_INCLUDE_NON_SHORTLIST=1
      shift
      ;;
    --health-check)
      HEALTH_CHECK=1
      shift
      ;;
    --no-health-check)
      HEALTH_CHECK=0
      shift
      ;;
    --health-min-new-shortlist)
      HEALTH_MIN_NEW_SHORTLIST="$2"
      shift 2
      ;;
    --health-min-new-shortlist-quality)
      HEALTH_MIN_NEW_SHORTLIST_QUALITY="$2"
      shift 2
      ;;
    --health-min-new-shortlist-remote-quality)
      HEALTH_MIN_NEW_SHORTLIST_REMOTE_QUALITY="$2"
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

apply_profile
if [[ -n "$USER_MODEL" ]]; then
  MODEL="$USER_MODEL"
fi
if [[ -n "$USER_MODEL_SELECTION" ]]; then
  MODEL_SELECTION="$USER_MODEL_SELECTION"
fi
if [[ -n "$USER_LLM_ROUNDS" ]]; then
  LLM_ROUNDS="$USER_LLM_ROUNDS"
fi
if [[ -n "$USER_LLM_CANDIDATES_PER_ROUND" ]]; then
  LLM_CANDIDATES_PER_ROUND="$USER_LLM_CANDIDATES_PER_ROUND"
fi
if [[ -n "$USER_LLM_TEMPERATURE" ]]; then
  LLM_TEMPERATURE="$USER_LLM_TEMPERATURE"
fi
if [[ -n "$USER_LLM_MAX_CALL_LATENCY_MS" ]]; then
  LLM_MAX_CALL_LATENCY_MS="$USER_LLM_MAX_CALL_LATENCY_MS"
fi
if [[ -n "$USER_LLM_STAGE_TIMEOUT_MS" ]]; then
  LLM_STAGE_TIMEOUT_MS="$USER_LLM_STAGE_TIMEOUT_MS"
fi
if [[ -n "$USER_LLM_MAX_RETRIES" ]]; then
  LLM_MAX_RETRIES="$USER_LLM_MAX_RETRIES"
fi
if [[ -n "$USER_VALIDATOR_CANDIDATE_LIMIT" ]]; then
  VALIDATOR_CANDIDATE_LIMIT="$USER_VALIDATOR_CANDIDATE_LIMIT"
fi
if [[ -n "$USER_VALIDATOR_EXPENSIVE_FINALIST_LIMIT" ]]; then
  VALIDATOR_EXPENSIVE_FINALIST_LIMIT="$USER_VALIDATOR_EXPENSIVE_FINALIST_LIMIT"
fi
if [[ -n "$USER_VALIDATOR_CONCURRENCY" ]]; then
  VALIDATOR_CONCURRENCY="$USER_VALIDATOR_CONCURRENCY"
fi
if [[ -n "$USER_VALIDATOR_MIN_CONCURRENCY" ]]; then
  VALIDATOR_MIN_CONCURRENCY="$USER_VALIDATOR_MIN_CONCURRENCY"
fi
if [[ -n "$USER_VALIDATOR_MAX_CONCURRENCY" ]]; then
  VALIDATOR_MAX_CONCURRENCY="$USER_VALIDATOR_MAX_CONCURRENCY"
fi
if [[ -n "$USER_VALIDATOR_TIMEOUT_S" ]]; then
  VALIDATOR_TIMEOUT_S="$USER_VALIDATOR_TIMEOUT_S"
fi
if [[ -n "$USER_VALIDATOR_CHECKS" ]]; then
  VALIDATOR_CHECKS="$USER_VALIDATOR_CHECKS"
fi
if [[ -n "$USER_PROMPT_TEMPLATE_FILE" ]]; then
  PROMPT_TEMPLATE_FILE="$USER_PROMPT_TEMPLATE_FILE"
fi

MODEL_SELECTION="${MODEL_SELECTION:l}"
case "$MODEL_SELECTION" in
  round_robin|random)
    ;;
  *)
    echo "--model-selection must be round_robin or random." >&2
    exit 2
    ;;
esac

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
if ! [[ "$POST_RANK_TOP_N" =~ '^[1-9][0-9]*$' ]]; then
  echo "--post-rank-top-n must be >= 1." >&2
  exit 2
fi
if ! [[ "$LLM_MAX_CALL_LATENCY_MS" =~ '^[1-9][0-9]*$' ]]; then
  echo "--llm-max-call-latency-ms must be a positive integer." >&2
  exit 2
fi
if (( LLM_MAX_CALL_LATENCY_MS < 1000 )); then
  echo "--llm-max-call-latency-ms must be >= 1000." >&2
  exit 2
fi
if ! [[ "$LLM_STAGE_TIMEOUT_MS" =~ '^[1-9][0-9]*$' ]]; then
  echo "--llm-stage-timeout-ms must be a positive integer." >&2
  exit 2
fi
if (( LLM_STAGE_TIMEOUT_MS < 1000 )); then
  echo "--llm-stage-timeout-ms must be >= 1000." >&2
  exit 2
fi
if ! [[ "$LLM_MAX_RETRIES" =~ '^[0-9]+$' ]]; then
  echo "--llm-max-retries must be >= 0." >&2
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
  --validator-checks "$VALIDATOR_CHECKS"
  --llm-ideation-enabled
  --llm-provider openrouter_http
  --llm-models "$MODEL"
  --llm-model-selection "$MODEL_SELECTION"
  --llm-openrouter-http-referer "$HTTP_REFERER"
  --llm-openrouter-x-title "$X_TITLE"
  --llm-context-file "$ROOT_DIR/resources/branding/llm/llm_context.example.json"
  --llm-rounds "$LLM_ROUNDS"
  --llm-candidates-per-round "$LLM_CANDIDATES_PER_ROUND"
  --llm-temperature "$LLM_TEMPERATURE"
  --llm-max-call-latency-ms "$LLM_MAX_CALL_LATENCY_MS"
  --llm-stage-timeout-ms "$LLM_STAGE_TIMEOUT_MS"
  --llm-max-retries "$LLM_MAX_RETRIES"
  --shard-id "$LANE"
  --shard-count "$SHARD_COUNT"
  --out-dir "$OUT_DIR"
)

if (( NO_EXTERNAL_CHECKS )); then
  CMD+=(--generator-no-external-checks)
fi
# Keep Google CSE settings in environment so secrets are not exposed in argv/logs.
# naming_validate_async reads OPENROUTER_GOOGLE_CSE_API_KEY / OPENROUTER_GOOGLE_CSE_CX directly.
if [[ -n "${PROMPT_TEMPLATE_FILE:-}" ]]; then
  CMD+=(--llm-prompt-template-file "$PROMPT_TEMPLATE_FILE")
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
  set +e
  direnv exec . "${CMD[@]}"
  RUN_RC=$?
  set -e
else
  set +e
  "${CMD[@]}"
  RUN_RC=$?
  set -e
fi

if (( RUN_RC == 0 && POST_RANK )); then
  POST_CMD=(
    python3 "$ROOT_DIR/scripts/branding/rerank_shortlist_deterministic.py"
    --out-dir "$OUT_DIR"
    --top-n "$POST_RANK_TOP_N"
  )
  if (( POST_RANK_INCLUDE_NON_SHORTLIST )); then
    POST_CMD+=(--include-non-shortlist)
  fi
  echo "running post-rank out_dir=$OUT_DIR top_n=$POST_RANK_TOP_N include_non_shortlist=$POST_RANK_INCLUDE_NON_SHORTLIST"
  printf '$ '
  printf '%q ' "${POST_CMD[@]}"
  echo
  if command -v direnv >/dev/null 2>&1; then
    if ! direnv exec . "${POST_CMD[@]}"; then
      echo "post_rank_warn reason=postrank_failed"
    fi
  elif ! "${POST_CMD[@]}"; then
    echo "post_rank_warn reason=postrank_failed"
  fi
fi

if (( RUN_RC == 0 && POST_RANK && HEALTH_CHECK )); then
  HEALTH_MIN_NEW_SHORTLIST_EFFECTIVE="$HEALTH_MIN_NEW_SHORTLIST"
  if [[ -z "$HEALTH_MIN_NEW_SHORTLIST_EFFECTIVE" ]]; then
    case "$PROFILE" in
      "quality")
        HEALTH_MIN_NEW_SHORTLIST_EFFECTIVE="$HEALTH_MIN_NEW_SHORTLIST_QUALITY"
        ;;
      "remote_quality")
        HEALTH_MIN_NEW_SHORTLIST_EFFECTIVE="$HEALTH_MIN_NEW_SHORTLIST_REMOTE_QUALITY"
        ;;
      *)
        HEALTH_MIN_NEW_SHORTLIST_EFFECTIVE="10"
        ;;
    esac
  fi
  HEALTH_CMD=(
    python3 "$ROOT_DIR/scripts/branding/check_campaign_health.py"
    --out-dir "$OUT_DIR"
    --min-new-shortlist "$HEALTH_MIN_NEW_SHORTLIST_EFFECTIVE"
  )
  echo "running health-check out_dir=$OUT_DIR"
  printf '$ '
  printf '%q ' "${HEALTH_CMD[@]}"
  echo
  if command -v direnv >/dev/null 2>&1; then
    set +e
    direnv exec . "${HEALTH_CMD[@]}"
    HEALTH_RC=$?
    set -e
  else
    set +e
    "${HEALTH_CMD[@]}"
    HEALTH_RC=$?
    set -e
  fi
  if (( HEALTH_RC != 0 )); then
    echo "health_check_warn reason=failed code=$HEALTH_RC"
    RUN_RC="$HEALTH_RC"
  fi
fi

exit "$RUN_RC"
