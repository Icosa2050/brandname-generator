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
LANE_PROFILE="${OPENROUTER_LANE_PROFILE:-constrained}"
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
LLM_CONTEXT_FILE="${OPENROUTER_LLM_CONTEXT_FILE:-$ROOT_DIR/resources/branding/llm/llm_context.example.json}"
DIVERSITY_MEMORY_FILE="${OPENROUTER_DIVERSITY_MEMORY_FILE:-$ROOT_DIR/test_outputs/branding/curated/diversity_memory.json}"
LIVE_PROGRESS=1
NO_EXTERNAL_CHECKS=1
IDEATION_ONLY=0
POST_RANK=1
POST_RANK_TOP_N="${OPENROUTER_POST_RANK_TOP_N:-40}"
POST_RANK_INCLUDE_NON_SHORTLIST=0
PUBLISH_COVERAGE=1
PUBLISH_COVERAGE_LIMIT="${OPENROUTER_PUBLISH_COVERAGE_LIMIT:-60}"
PUBLISH_COVERAGE_CHECKS="${OPENROUTER_PUBLISH_COVERAGE_CHECKS:-company_cheap,domain,web_google_like,tm_registry_global}"
PUBLISH_COVERAGE_REQUIRED_DOMAIN_TLDS="${OPENROUTER_PUBLISH_COVERAGE_REQUIRED_DOMAIN_TLDS:-}"
PUBLISH_NO_SURVIVORS_FAIL=1
HEALTH_CHECK=1
HEALTH_MIN_NEW_SHORTLIST="${OPENROUTER_HEALTH_MIN_NEW_SHORTLIST:-}"
HEALTH_MIN_NEW_SHORTLIST_QUALITY="${OPENROUTER_HEALTH_MIN_NEW_SHORTLIST_QUALITY:-6}"
HEALTH_MIN_NEW_SHORTLIST_REMOTE_QUALITY="${OPENROUTER_HEALTH_MIN_NEW_SHORTLIST_REMOTE_QUALITY:-10}"
HEALTH_MIN_POSTRANK_STRONG="${OPENROUTER_HEALTH_MIN_POSTRANK_STRONG:-}"
HEALTH_MIN_POSTRANK_STRONG_QUALITY="${OPENROUTER_HEALTH_MIN_POSTRANK_STRONG_QUALITY:-4}"
HEALTH_MIN_POSTRANK_STRONG_REMOTE_QUALITY="${OPENROUTER_HEALTH_MIN_POSTRANK_STRONG_REMOTE_QUALITY:-6}"
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
USER_LLM_CONTEXT_FILE=""
USER_VALIDATOR_CANDIDATE_LIMIT=""
USER_VALIDATOR_EXPENSIVE_FINALIST_LIMIT=""
USER_VALIDATOR_CONCURRENCY=""
USER_VALIDATOR_MIN_CONCURRENCY=""
USER_VALIDATOR_MAX_CONCURRENCY=""
USER_VALIDATOR_TIMEOUT_S=""
USER_VALIDATOR_CHECKS=""
USER_PROMPT_TEMPLATE_FILE=""
LANE_PROFILE_ARGS=()

usage() {
  cat <<'EOF'
Run one tuned OpenRouter campaign lane.

Usage:
  scripts/branding/run_openrouter_lane.sh [options] [-- <extra runner args>]

Options:
  --profile <fast|quality|remote_quality> Apply tuning preset (default: quality)
  --lane-profile <constrained|heritage|expressive|plosive|minimal>
                                    Apply ideation taste profile (default: constrained)
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
  --llm-context-file <path>          Optional ideation context JSON file
  --ideation-only                  Stop after ideation/generation and skip validator-facing downstream stages
  --post-rank                        Run deterministic DE/EN post-ranker (default: on)
  --no-post-rank                     Skip deterministic post-ranker
  --post-rank-top-n <n>              Names kept by post-ranker (default: 40)
  --post-rank-all                    Score all names, not only shortlist-selected ones
  --publish-coverage                 Run dedicated publish-coverage pass on shortlisted names (default: on)
  --no-publish-coverage              Skip dedicated publish-coverage pass
  --publish-coverage-limit <n>       Max shortlisted names checked for publish coverage (default: 60)
  --publish-coverage-checks <csv>    Checks used for publish coverage (default: company_cheap,domain,web_google_like,tm_registry_global)
  --publish-coverage-required-domain-tlds <csv>
                                    Required domain TLDs for publish coverage (default: scope-derived)
  --publish-no-survivors-fail        Fail lane when publish survivor count is zero (default: on)
  --publish-no-survivors-warn        Warn instead of failing when publish survivor count is zero
  --health-check                     Run post-run health checks (default: on)
  --no-health-check                  Skip post-run health checks
  --health-min-new-shortlist <n>     Override minimum new shortlist names for health check
  --health-min-new-shortlist-quality <n>
                                     Quality-profile minimum new shortlist names (default: 6)
  --health-min-new-shortlist-remote-quality <n>
                                     Remote-quality minimum new shortlist names (default: 10)
  --health-min-postrank-strong <n>   Override minimum strong post-rank names for health check
  --health-min-postrank-strong-quality <n>
                                     Quality-profile minimum strong post-rank names (default: 4)
  --health-min-postrank-strong-remote-quality <n>
                                     Remote-quality minimum strong post-rank names (default: 6)
  --http-referer <url>               OpenRouter HTTP-Referer header
  --x-title <text>                   OpenRouter X-Title header
  --with-external-checks             Keep generator external checks enabled
  --no-live-progress                 Disable live progress stream
  -h, --help                         Show this help

Examples:
  scripts/branding/run_openrouter_lane.sh --lane 0
  scripts/branding/run_openrouter_lane.sh --lane 1 --lane-profile plosive --out-dir /tmp/branding_openrouter_tuned
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

apply_lane_profile() {
  LANE_PROFILE_ARGS=()
  case "${LANE_PROFILE:l}" in
    ""|"constrained")
      LANE_PROFILE="constrained"
      PROMPT_TEMPLATE_FILE="$ROOT_DIR/resources/branding/llm/llm_prompt.constrained_pronounceable_de_en_v3.txt"
      LLM_CONTEXT_FILE="$ROOT_DIR/resources/branding/llm/llm_context.lane_constrained.json"
      LANE_PROFILE_ARGS=(
        --generator-min-len 7
        --generator-max-len 13
        --generator-seeds anchor,beacon,clarity,harbor,meridian,signal
        --source-influence-shares 0.20
      )
      ;;
    "heritage")
      LANE_PROFILE="heritage"
      PROMPT_TEMPLATE_FILE="$ROOT_DIR/resources/branding/llm/llm_prompt.lane_heritage_trust_de_en_v1.txt"
      LLM_CONTEXT_FILE="$ROOT_DIR/resources/branding/llm/llm_context.lane_heritage.json"
      LLM_ROUNDS="4"
      LLM_CANDIDATES_PER_ROUND="12"
      LLM_TEMPERATURE="0.68"
      LANE_PROFILE_ARGS=(
        --generator-min-len 7
        --generator-max-len 12
        --generator-seeds beacon,harbor,meridian,serein,signal,steady
        --source-influence-shares 0.25
        --quota-profiles 'coined:190,stem:120,suggestive:165,morphology:135,seed:90,expression:85,source_pool:90,blend:170,lattice:175'
      )
      ;;
    "expressive")
      LANE_PROFILE="expressive"
      PROMPT_TEMPLATE_FILE="$ROOT_DIR/resources/branding/llm/llm_prompt.lane_expressive_trust_de_en_v1.txt"
      LLM_CONTEXT_FILE="$ROOT_DIR/resources/branding/llm/llm_context.lane_expressive.json"
      LLM_ROUNDS="4"
      LLM_CANDIDATES_PER_ROUND="16"
      LLM_TEMPERATURE="0.92"
      LANE_PROFILE_ARGS=(
        --generator-min-len 8
        --generator-max-len 14
        --generator-seeds beacon,canopy,harbor,meridian,signal,serein,verge
        --source-influence-shares 0.35
        --quota-profiles 'coined:170,stem:90,suggestive:175,morphology:145,seed:80,expression:140,source_pool:80,blend:235,lattice:205'
      )
      ;;
    "plosive")
      LANE_PROFILE="plosive"
      PROMPT_TEMPLATE_FILE="$ROOT_DIR/resources/branding/llm/llm_prompt.lane_plosive_precision_de_en_v1.txt"
      LLM_CONTEXT_FILE="$ROOT_DIR/resources/branding/llm/llm_context.lane_plosive.json"
      LLM_ROUNDS="4"
      LLM_CANDIDATES_PER_ROUND="14"
      LLM_TEMPERATURE="0.86"
      LANE_PROFILE_ARGS=(
        --generator-min-len 6
        --generator-max-len 11
        --generator-seeds brisk,flint,forge,grain,pivot,signal,stone,vector
        --source-influence-shares 0.12
        --quota-profiles 'coined:255,stem:145,suggestive:110,morphology:95,seed:70,expression:55,source_pool:75,blend:150,lattice:245'
      )
      ;;
    "minimal")
      LANE_PROFILE="minimal"
      PROMPT_TEMPLATE_FILE="$ROOT_DIR/resources/branding/llm/llm_prompt.lane_minimal_abstract_de_en_v1.txt"
      LLM_CONTEXT_FILE="$ROOT_DIR/resources/branding/llm/llm_context.lane_minimal.json"
      LLM_ROUNDS="4"
      LLM_CANDIDATES_PER_ROUND="12"
      LLM_TEMPERATURE="0.74"
      LANE_PROFILE_ARGS=(
        --generator-min-len 6
        --generator-max-len 10
        --generator-seeds axis,beacon,frame,grain,meridian,signal,stone
        --source-influence-shares 0.10
        --quota-profiles 'coined:265,stem:145,suggestive:95,morphology:85,seed:60,expression:50,source_pool:70,blend:155,lattice:255'
      )
      ;;
    *)
      echo "Unknown --lane-profile: $LANE_PROFILE (expected constrained|heritage|expressive|plosive|minimal)." >&2
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
    --lane-profile)
      LANE_PROFILE="$2"
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
    --llm-context-file)
      LLM_CONTEXT_FILE="$2"
      USER_LLM_CONTEXT_FILE="$2"
      shift 2
      ;;
    --ideation-only)
      IDEATION_ONLY=1
      shift
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
    --publish-coverage)
      PUBLISH_COVERAGE=1
      shift
      ;;
    --no-publish-coverage)
      PUBLISH_COVERAGE=0
      shift
      ;;
    --publish-coverage-limit)
      PUBLISH_COVERAGE_LIMIT="$2"
      shift 2
      ;;
    --publish-coverage-checks)
      PUBLISH_COVERAGE_CHECKS="$2"
      shift 2
      ;;
    --publish-coverage-required-domain-tlds)
      PUBLISH_COVERAGE_REQUIRED_DOMAIN_TLDS="$2"
      shift 2
      ;;
    --publish-no-survivors-fail)
      PUBLISH_NO_SURVIVORS_FAIL=1
      shift
      ;;
    --publish-no-survivors-warn)
      PUBLISH_NO_SURVIVORS_FAIL=0
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
    --health-min-postrank-strong)
      HEALTH_MIN_POSTRANK_STRONG="$2"
      shift 2
      ;;
    --health-min-postrank-strong-quality)
      HEALTH_MIN_POSTRANK_STRONG_QUALITY="$2"
      shift 2
      ;;
    --health-min-postrank-strong-remote-quality)
      HEALTH_MIN_POSTRANK_STRONG_REMOTE_QUALITY="$2"
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
apply_lane_profile
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
if [[ -n "$USER_LLM_CONTEXT_FILE" ]]; then
  LLM_CONTEXT_FILE="$USER_LLM_CONTEXT_FILE"
fi
if (( IDEATION_ONLY )); then
  POST_RANK=0
  PUBLISH_COVERAGE=0
  HEALTH_CHECK=0
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
if ! [[ "$PUBLISH_COVERAGE_LIMIT" =~ '^[1-9][0-9]*$' ]]; then
  echo "--publish-coverage-limit must be >= 1." >&2
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
  --llm-context-file "$LLM_CONTEXT_FILE"
  --llm-rounds "$LLM_ROUNDS"
  --llm-candidates-per-round "$LLM_CANDIDATES_PER_ROUND"
  --llm-temperature "$LLM_TEMPERATURE"
  --llm-max-call-latency-ms "$LLM_MAX_CALL_LATENCY_MS"
  --llm-stage-timeout-ms "$LLM_STAGE_TIMEOUT_MS"
  --llm-max-retries "$LLM_MAX_RETRIES"
  --shard-id "$LANE"
  --shard-count "$SHARD_COUNT"
  --out-dir "$OUT_DIR"
  --diversity-memory-file "$DIVERSITY_MEMORY_FILE"
)

if (( IDEATION_ONLY )); then
  CMD+=(--ideation-only)
fi

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
if (( ${#LANE_PROFILE_ARGS[@]} > 0 )); then
  CMD+=("${LANE_PROFILE_ARGS[@]}")
fi
if (( ${#EXTRA_ARGS[@]} > 0 )); then
  CMD+=("${EXTRA_ARGS[@]}")
fi

echo "running lane=$LANE/$SHARD_COUNT lane_profile=$LANE_PROFILE ideation_only=$IDEATION_ONLY out_dir=$OUT_DIR model=$MODEL diversity_memory=$DIVERSITY_MEMORY_FILE"
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

if (( RUN_RC == 0 && PUBLISH_COVERAGE )); then
  COVERAGE_DB="$OUT_DIR/naming_campaign.db"
  if [[ ! -f "$COVERAGE_DB" ]]; then
    echo "publish_coverage_warn reason=db_not_found path=$COVERAGE_DB"
    RUN_RC=4
  else
    COVERAGE_CONTEXT="$(
      python3 - "$COVERAGE_DB" <<'PY'
import json
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
try:
    row = conn.execute(
        """
        SELECT scope, gate_mode, config_json
        FROM naming_runs
        WHERE variation_profile = 'validator_async'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
finally:
    conn.close()

payload = {
    "scope": "eu",
    "gate": "strict",
    "policy_version": "collision_first_v1",
    "class_profile": "9,42",
    "market_scope": "eu,ch",
}
if row:
    payload["scope"] = str(row[0] or payload["scope"]).strip() or payload["scope"]
    payload["gate"] = str(row[1] or payload["gate"]).strip() or payload["gate"]
    try:
        config = json.loads(row[2] or "{}")
    except json.JSONDecodeError:
        config = {}
    if isinstance(config, dict):
        payload["policy_version"] = str(config.get("policy_version") or payload["policy_version"]).strip() or payload["policy_version"]
        payload["class_profile"] = str(config.get("class_profile") or payload["class_profile"]).strip() or payload["class_profile"]
        payload["market_scope"] = str(config.get("market_scope") or payload["market_scope"]).strip() or payload["market_scope"]
print(json.dumps(payload, ensure_ascii=True))
PY
    )"
    COVERAGE_SCOPE="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["scope"])' "$COVERAGE_CONTEXT")"
    COVERAGE_GATE="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["gate"])' "$COVERAGE_CONTEXT")"
    COVERAGE_POLICY_VERSION="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["policy_version"])' "$COVERAGE_CONTEXT")"
    COVERAGE_CLASS_PROFILE="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["class_profile"])' "$COVERAGE_CONTEXT")"
    COVERAGE_MARKET_SCOPE="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["market_scope"])' "$COVERAGE_CONTEXT")"
    COVERAGE_CMD=(
      python3 "$ROOT_DIR/scripts/branding/naming_validate_async.py"
      --db "$COVERAGE_DB"
      --pipeline-version=v3
      --enable-v3
      --candidate-source shortlist_selected
      --candidate-limit "$PUBLISH_COVERAGE_LIMIT"
      --shortlist-source-run-id 0
      --state-filter new,checked
      --scope "$COVERAGE_SCOPE"
      --gate "$COVERAGE_GATE"
      --expensive-finalist-limit "$PUBLISH_COVERAGE_LIMIT"
      --checks "$PUBLISH_COVERAGE_CHECKS"
      --policy-version "$COVERAGE_POLICY_VERSION"
      --class-profile "$COVERAGE_CLASS_PROFILE"
      --market-scope "$COVERAGE_MARKET_SCOPE"
      --validation-tier all
      --concurrency "$VALIDATOR_CONCURRENCY"
      --min-concurrency "$VALIDATOR_MIN_CONCURRENCY"
      --max-concurrency "$VALIDATOR_MAX_CONCURRENCY"
      --timeout-s "$VALIDATOR_TIMEOUT_S"
    )
    if [[ -n "$PUBLISH_COVERAGE_REQUIRED_DOMAIN_TLDS" ]]; then
      COVERAGE_CMD+=(--required-domain-tlds "$PUBLISH_COVERAGE_REQUIRED_DOMAIN_TLDS")
      echo "running publish-coverage out_dir=$OUT_DIR limit=$PUBLISH_COVERAGE_LIMIT checks=$PUBLISH_COVERAGE_CHECKS required_domains=$PUBLISH_COVERAGE_REQUIRED_DOMAIN_TLDS"
    else
      echo "running publish-coverage out_dir=$OUT_DIR limit=$PUBLISH_COVERAGE_LIMIT checks=$PUBLISH_COVERAGE_CHECKS required_domains=scope-derived"
    fi
    printf '$ '
    printf '%q ' "${COVERAGE_CMD[@]}"
    echo
    if command -v direnv >/dev/null 2>&1; then
      set +e
      direnv exec . "${COVERAGE_CMD[@]}"
      COVERAGE_RC=$?
      set -e
    else
      set +e
      "${COVERAGE_CMD[@]}"
      COVERAGE_RC=$?
      set -e
    fi
    if (( COVERAGE_RC != 0 )); then
      echo "publish_coverage_warn reason=validator_failed code=$COVERAGE_RC"
      RUN_RC="$COVERAGE_RC"
    else
      COVERAGE_SUMMARY="$OUT_DIR/postrank/validated_publish_summary.json"
      if [[ ! -f "$COVERAGE_SUMMARY" ]]; then
        echo "publish_coverage_warn reason=summary_missing path=$COVERAGE_SUMMARY"
        RUN_RC=4
      else
        read -r SURVIVOR_COUNT REVIEW_COUNT REJECTED_COUNT < <(
          python3 - "$COVERAGE_SUMMARY" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    payload = json.load(fh)
print(
    int(payload.get("survivor_count", 0)),
    int(payload.get("review_count", 0)),
    int(payload.get("rejected_count", 0)),
)
PY
        )
        echo "publish_coverage_summary survivors=$SURVIVOR_COUNT review=$REVIEW_COUNT rejected=$REJECTED_COUNT summary=$COVERAGE_SUMMARY"
        if (( SURVIVOR_COUNT == 0 && PUBLISH_NO_SURVIVORS_FAIL )); then
          echo "publish_gate_fail reason=no_publish_survivors survivors=0 review=$REVIEW_COUNT rejected=$REJECTED_COUNT"
          RUN_RC=4
        fi
      fi
    fi
  fi
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
  HEALTH_MIN_POSTRANK_STRONG_EFFECTIVE="$HEALTH_MIN_POSTRANK_STRONG"
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
  if [[ -z "$HEALTH_MIN_POSTRANK_STRONG_EFFECTIVE" ]]; then
    case "$PROFILE" in
      "quality")
        HEALTH_MIN_POSTRANK_STRONG_EFFECTIVE="$HEALTH_MIN_POSTRANK_STRONG_QUALITY"
        ;;
      "remote_quality")
        HEALTH_MIN_POSTRANK_STRONG_EFFECTIVE="$HEALTH_MIN_POSTRANK_STRONG_REMOTE_QUALITY"
        ;;
      *)
        HEALTH_MIN_POSTRANK_STRONG_EFFECTIVE="6"
        ;;
    esac
  fi
  HEALTH_CMD=(
    python3 "$ROOT_DIR/scripts/branding/check_campaign_health.py"
    --out-dir "$OUT_DIR"
    --min-new-shortlist "$HEALTH_MIN_NEW_SHORTLIST_EFFECTIVE"
    --min-postrank-strong "$HEALTH_MIN_POSTRANK_STRONG_EFFECTIVE"
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
