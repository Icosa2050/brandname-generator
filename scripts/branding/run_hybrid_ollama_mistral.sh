#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

OUT_DIR="${HYBRID_OUT_DIR:-/tmp/branding_hybrid_ollama}"
LOCAL_MODEL="${HYBRID_LOCAL_MODEL:-gemma3:12b}"
REMOTE_MODEL="${HYBRID_REMOTE_MODEL:-mistralai/mistral-small-creative}"
LOCAL_SHARE="${HYBRID_LOCAL_SHARE:-0.75}"
OPENAI_BASE_URL="${HYBRID_OLLAMA_OPENAI_BASE_URL:-http://127.0.0.1:11434/v1}"
KEEP_ALIVE="${HYBRID_OLLAMA_KEEP_ALIVE:-30m}"
MAX_RUNS="${HYBRID_MAX_RUNS:-1}"
SLEEP_S="${HYBRID_SLEEP_S:-0}"
LLM_ROUNDS="${HYBRID_LLM_ROUNDS:-2}"
LLM_CANDIDATES_PER_ROUND="${HYBRID_LLM_CANDIDATES_PER_ROUND:-12}"
GENERATOR_MIN_LEN="${HYBRID_GENERATOR_MIN_LEN:-6}"
GENERATOR_MAX_LEN="${HYBRID_GENERATOR_MAX_LEN:-11}"
PROMPT_TEMPLATE_FILE="${HYBRID_LLM_PROMPT_TEMPLATE_FILE:-}"
LIVE_PROGRESS=1
NO_EXTERNAL_CHECKS=1
EXTRA_ARGS=()

usage() {
  cat <<'EOF'
Run hybrid ideation with Ollama local model + OpenRouter Mistral Creative.

Usage:
  scripts/branding/run_hybrid_ollama_mistral.sh [options] [-- <extra runner args>]

Options:
  --out-dir <path>                   Campaign output root (default: /tmp/branding_hybrid_ollama)
  --local-model <id|csv>             Ollama model id(s), comma-separated allowed
  --local-models <csv>               Alias for --local-model
  --remote-model <id|csv>            OpenRouter model id(s), comma-separated allowed
  --remote-models <csv>              Alias for --remote-model
  --local-share <0..1>               Share of local rounds (default: 0.75)
  --openai-base-url <url>            Ollama OpenAI-compatible URL (default: http://127.0.0.1:11434/v1)
  --keep-alive <value>               keep_alive hint (default: 30m)
  --max-runs <n>                     Max campaign runs (default: 1)
  --sleep-s <seconds>                Sleep between runs (default: 0)
  --llm-rounds <n>                   LLM rounds per run (default: 2)
  --llm-candidates-per-round <n>     Candidates requested each round (default: 12)
  --generator-min-len <n>            Generator min length filter (default: 6)
  --generator-max-len <n>            Generator max length filter (default: 11)
  --llm-prompt-template-file <path>  Optional prompt template passed to campaign runner
  --with-external-checks             Keep generator external checks enabled
  --no-live-progress                 Disable live progress stream
  -h, --help                         Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --local-model)
      LOCAL_MODEL="$2"
      shift 2
      ;;
    --local-models)
      LOCAL_MODEL="$2"
      shift 2
      ;;
    --remote-model)
      REMOTE_MODEL="$2"
      shift 2
      ;;
    --remote-models)
      REMOTE_MODEL="$2"
      shift 2
      ;;
    --local-share)
      LOCAL_SHARE="$2"
      shift 2
      ;;
    --openai-base-url)
      OPENAI_BASE_URL="$2"
      shift 2
      ;;
    --keep-alive)
      KEEP_ALIVE="$2"
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
      shift 2
      ;;
    --llm-candidates-per-round)
      LLM_CANDIDATES_PER_ROUND="$2"
      shift 2
      ;;
    --generator-min-len)
      GENERATOR_MIN_LEN="$2"
      shift 2
      ;;
    --generator-max-len)
      GENERATOR_MAX_LEN="$2"
      shift 2
      ;;
    --llm-prompt-template-file)
      PROMPT_TEMPLATE_FILE="$2"
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

if [[ "$GENERATOR_MIN_LEN" != <-> || "$GENERATOR_MAX_LEN" != <-> ]]; then
  echo "Generator length bounds must be non-negative integers." >&2
  exit 2
fi
if (( GENERATOR_MIN_LEN < 4 || GENERATOR_MAX_LEN > 20 || GENERATOR_MIN_LEN > GENERATOR_MAX_LEN )); then
  echo "Invalid generator length bounds: min=$GENERATOR_MIN_LEN max=$GENERATOR_MAX_LEN (expected 4..20 and min<=max)." >&2
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
  --llm-openai-base-url "$OPENAI_BASE_URL"
  --llm-openai-keep-alive "$KEEP_ALIVE"
  --llm-rounds "$LLM_ROUNDS"
  --llm-candidates-per-round "$LLM_CANDIDATES_PER_ROUND"
  --generator-min-len "$GENERATOR_MIN_LEN"
  --generator-max-len "$GENERATOR_MAX_LEN"
  --out-dir "$OUT_DIR"
)

if (( NO_EXTERNAL_CHECKS )); then
  CMD+=(--generator-no-external-checks)
fi
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

echo "running hybrid=ollama+openrouter out_dir=$OUT_DIR local_model=$LOCAL_MODEL remote_model=$REMOTE_MODEL local_share=$LOCAL_SHARE generator_len=${GENERATOR_MIN_LEN}-${GENERATOR_MAX_LEN}"
printf '$ '
printf '%q ' "${CMD[@]}"
echo

cd "$ROOT_DIR"
if command -v direnv >/dev/null 2>&1; then
  if ! direnv exec . python3 -c 'import os, sys; sys.exit(0 if os.getenv("OPENROUTER_API_KEY") else 1)' >/dev/null 2>&1; then
    echo "OPENROUTER_API_KEY is not set (after direnv)." >&2
    echo "Tip: add it to .env/.envrc, then run: direnv allow" >&2
    exit 2
  fi
  exec direnv exec . "${CMD[@]}"
fi
if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
  echo "OPENROUTER_API_KEY is not set." >&2
  echo "Tip: run via direnv (direnv exec . ...)." >&2
  exit 2
fi
exec "${CMD[@]}"
