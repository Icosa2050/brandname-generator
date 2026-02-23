#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

OUT_DIR="${HYBRID_OUT_DIR:-/tmp/branding_hybrid_lmstudio}"
LOCAL_MODEL="${HYBRID_LOCAL_MODEL:-llama-3.3-8b-instruct-omniwriter}"
REMOTE_MODEL="${HYBRID_REMOTE_MODEL:-mistralai/mistral-small-creative}"
LOCAL_SHARE="${HYBRID_LOCAL_SHARE:-0.75}"
BASE_URL="${HYBRID_LMSTUDIO_BASE_URL:-http://127.0.0.1:1234/v1}"
TTL_S="${HYBRID_LMSTUDIO_TTL_S:-3600}"
MAX_RUNS="${HYBRID_MAX_RUNS:-1}"
SLEEP_S="${HYBRID_SLEEP_S:-0}"
LLM_ROUNDS="${HYBRID_LLM_ROUNDS:-2}"
LLM_CANDIDATES_PER_ROUND="${HYBRID_LLM_CANDIDATES_PER_ROUND:-12}"
PROFILE="${HYBRID_PROFILE:-}"
LIVE_PROGRESS=1
NO_EXTERNAL_CHECKS=1
PROFILE_ARGS=()
EXTRA_ARGS=()
USER_LOCAL_SHARE=""
USER_LLM_ROUNDS=""
USER_LLM_CANDIDATES_PER_ROUND=""

usage() {
  cat <<'EOF'
Run hybrid ideation with LM Studio local model + OpenRouter Mistral Creative.

Usage:
  scripts/branding/run_hybrid_lmstudio_mistral.sh [options] [-- <extra runner args>]

Options:
  --profile <fast|quality>           Apply preset args (default: custom/manual)
  --fast                             Alias for --profile fast
  --quality                          Alias for --profile quality
  --out-dir <path>                   Campaign output root (default: /tmp/branding_hybrid_lmstudio)
  --local-model <id>                 LM Studio model id (default: llama-3.3-8b-instruct-omniwriter)
  --remote-model <id>                OpenRouter model id (default: mistralai/mistral-small-creative)
  --local-share <0..1>               Share of local rounds (default: 0.75)
  --base-url <url>                   LM Studio OpenAI-compatible URL (default: http://127.0.0.1:1234/v1)
  --ttl-s <seconds>                  LM Studio residency TTL hint (default: 3600)
  --max-runs <n>                     Max campaign runs (default: 1)
  --sleep-s <seconds>                Sleep between runs (default: 0)
  --llm-rounds <n>                   LLM rounds per run (default: 2)
  --llm-candidates-per-round <n>     Candidates requested each round (default: 12)
  --with-external-checks             Keep generator external checks enabled
  --no-live-progress                 Disable live progress stream
  -h, --help                         Show this help

Profiles:
  fast:
    --local-share 1.0
    --llm-rounds 1
    --llm-candidates-per-round 24
    --validator-tier cheap

  quality:
    --local-share 0.75
    --llm-rounds 4
    --llm-candidates-per-round 12
    --validator-expensive-finalist-limit 20
    --validator-timeout-s 12
    --validator-max-concurrency 16

Notes:
  - Extra runner args after '--' are appended last and can override profile args.
  - Explicit CLI options (for share/rounds/candidates-per-round) override profile defaults.
EOF
}

apply_profile() {
  PROFILE_ARGS=()
  case "$PROFILE" in
    ""|"custom")
      ;;
    "fast")
      LOCAL_SHARE="1.0"
      LLM_ROUNDS="1"
      LLM_CANDIDATES_PER_ROUND="24"
      PROFILE_ARGS=(
        --validator-tier cheap
      )
      ;;
    "quality")
      LOCAL_SHARE="0.75"
      LLM_ROUNDS="4"
      LLM_CANDIDATES_PER_ROUND="12"
      PROFILE_ARGS=(
        --validator-expensive-finalist-limit 20
        --validator-timeout-s 12
        --validator-max-concurrency 16
      )
      ;;
    *)
      echo "Unknown profile: $PROFILE (expected fast|quality)." >&2
      exit 1
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --fast)
      PROFILE="fast"
      shift
      ;;
    --quality)
      PROFILE="quality"
      shift
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --local-model)
      LOCAL_MODEL="$2"
      shift 2
      ;;
    --remote-model)
      REMOTE_MODEL="$2"
      shift 2
      ;;
    --local-share)
      LOCAL_SHARE="$2"
      USER_LOCAL_SHARE="$2"
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
    --max-runs)
      MAX_RUNS="$2"
      shift 2
      ;;
    --sleep-s)
      SLEEP_S="$2"
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
if [[ -n "$USER_LOCAL_SHARE" ]]; then
  LOCAL_SHARE="$USER_LOCAL_SHARE"
fi
if [[ -n "$USER_LLM_ROUNDS" ]]; then
  LLM_ROUNDS="$USER_LLM_ROUNDS"
fi
if [[ -n "$USER_LLM_CANDIDATES_PER_ROUND" ]]; then
  LLM_CANDIDATES_PER_ROUND="$USER_LLM_CANDIDATES_PER_ROUND"
fi

if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
  echo "OPENROUTER_API_KEY is not set." >&2
  echo "Tip: run via direnv (direnv exec . ...)." >&2
  exit 2
fi

CMD=(
  python3 "$ROOT_DIR/scripts/branding/naming_campaign_runner.py"
  --max-runs "$MAX_RUNS"
  --sleep-s "$SLEEP_S"
  --no-mini-test
  --generator-only-llm-candidates
  --llm-ideation-enabled
  --llm-provider hybrid
  --llm-hybrid-local-models "$LOCAL_MODEL"
  --llm-hybrid-remote-models "$REMOTE_MODEL"
  --llm-hybrid-local-share "$LOCAL_SHARE"
  --llm-openai-base-url "$BASE_URL"
  --llm-openai-ttl-s "$TTL_S"
  --llm-rounds "$LLM_ROUNDS"
  --llm-candidates-per-round "$LLM_CANDIDATES_PER_ROUND"
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
if (( ${#PROFILE_ARGS[@]} > 0 )); then
  CMD+=("${PROFILE_ARGS[@]}")
fi
if (( ${#EXTRA_ARGS[@]} > 0 )); then
  CMD+=("${EXTRA_ARGS[@]}")
fi

echo "running hybrid=lmstudio+openrouter out_dir=$OUT_DIR profile=${PROFILE:-custom} local_model=$LOCAL_MODEL remote_model=$REMOTE_MODEL local_share=$LOCAL_SHARE llm_rounds=$LLM_ROUNDS"
printf '$ '
printf '%q ' "${CMD[@]}"
echo

cd "$ROOT_DIR"
if command -v direnv >/dev/null 2>&1; then
  exec direnv exec . "${CMD[@]}"
fi
exec "${CMD[@]}"
