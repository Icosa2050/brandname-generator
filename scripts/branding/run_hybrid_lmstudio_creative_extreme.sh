#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BASE_RUNNER="$ROOT_DIR/scripts/branding/run_hybrid_lmstudio_mistral.sh"
PROMPT_TEMPLATE="$ROOT_DIR/resources/branding/llm/llm_prompt.creative_extreme_de_en_v1.txt"

OUT_DIR="${EXTREME_CREATIVE_OUT_DIR:-$ROOT_DIR/test_outputs/branding/hybrid_creative_extreme}"
REMOTE_MODELS="${EXTREME_CREATIVE_REMOTE_MODELS:-mistralai/mistral-small-creative,moonshotai/kimi-k2.5,qwen/qwen3-next-80b-a3b-instruct,anthropic/claude-sonnet-4.6}"
LOCAL_SHARE="${EXTREME_CREATIVE_LOCAL_SHARE:-0.10}"
LLM_ROUNDS="${EXTREME_CREATIVE_LLM_ROUNDS:-10}"
LLM_CANDIDATES_PER_ROUND="${EXTREME_CREATIVE_LLM_CANDIDATES_PER_ROUND:-20}"
MAX_USD_PER_RUN="${EXTREME_CREATIVE_MAX_USD_PER_RUN:-1.80}"
GENERATOR_MIN_LEN="${EXTREME_CREATIVE_GENERATOR_MIN_LEN:-8}"
GENERATOR_MAX_LEN="${EXTREME_CREATIVE_GENERATOR_MAX_LEN:-16}"
POST_RANK_TOP_N="${EXTREME_CREATIVE_POST_RANK_TOP_N:-80}"
GENERATOR_SEEDS="${EXTREME_CREATIVE_GENERATOR_SEEDS:-anchor,aperture,archipelago,beacon,cinder,ember,harbor,keystone,lodestar,meridian,quarry,serein,signal,solace,vector,velora}"
SOURCE_INFLUENCE_SHARES="${EXTREME_CREATIVE_SOURCE_INFLUENCE_SHARES:-0.05,0.10,0.18,0.28}"
QUOTA_PROFILES="${EXTREME_CREATIVE_QUOTA_PROFILES:-coined:260,stem:70,suggestive:180,morphology:130,seed:55,expression:220,source_pool:45,blend:250,lattice:320|coined:240,stem:60,suggestive:160,morphology:150,seed:60,expression:250,source_pool:45,blend:270,lattice:300|coined:280,stem:65,suggestive:170,morphology:120,seed:50,expression:210,source_pool:40,blend:240,lattice:340}"

typeset -a USER_BASE_ARGS=()
typeset -a USER_RUNNER_ARGS=()

usage() {
  cat <<EOF
Run the most adventurous LM Studio/OpenRouter hybrid naming sweep in this repo.

Usage:
  zsh scripts/branding/run_hybrid_lmstudio_creative_extreme.sh [hybrid-runner args] [-- naming-campaign-runner args]

Defaults:
  out_dir:                 $OUT_DIR
  remote_models:           $REMOTE_MODELS
  local_share:             $LOCAL_SHARE
  llm_rounds:              $LLM_ROUNDS
  llm_candidates_per_round:$LLM_CANDIDATES_PER_ROUND
  max_usd_per_run:         $MAX_USD_PER_RUN
  generator_len:           $GENERATOR_MIN_LEN-$GENERATOR_MAX_LEN
  post_rank_top_n:         $POST_RANK_TOP_N

What this runner changes:
  - starts from the existing creative_wide hybrid profile
  - biases heavily toward remote creative models
  - uses a more aggressive creative prompt template
  - widens generator seeds into more metaphorical territory
  - skews quota profiles toward coined, blend, expression, and lattice families
  - lowers source influence to keep names less literal

Examples:
  zsh scripts/branding/run_hybrid_lmstudio_creative_extreme.sh
  zsh scripts/branding/run_hybrid_lmstudio_creative_extreme.sh --max-runs 2 --local-share 0.05
  zsh scripts/branding/run_hybrid_lmstudio_creative_extreme.sh -- --scopes global,eu --llm-temperature 1.05

Environment overrides:
  EXTREME_CREATIVE_OUT_DIR
  EXTREME_CREATIVE_REMOTE_MODELS
  EXTREME_CREATIVE_LOCAL_SHARE
  EXTREME_CREATIVE_LLM_ROUNDS
  EXTREME_CREATIVE_LLM_CANDIDATES_PER_ROUND
  EXTREME_CREATIVE_MAX_USD_PER_RUN
  EXTREME_CREATIVE_GENERATOR_MIN_LEN
  EXTREME_CREATIVE_GENERATOR_MAX_LEN
  EXTREME_CREATIVE_POST_RANK_TOP_N
  EXTREME_CREATIVE_GENERATOR_SEEDS
  EXTREME_CREATIVE_SOURCE_INFLUENCE_SHARES
  EXTREME_CREATIVE_QUOTA_PROFILES
EOF
}

split_args() {
  local mode="base"
  local arg
  for arg in "$@"; do
    if [[ "$arg" == "--" && "$mode" == "base" ]]; then
      mode="runner"
      continue
    fi
    if [[ "$mode" == "base" ]]; then
      USER_BASE_ARGS+=("$arg")
    else
      USER_RUNNER_ARGS+=("$arg")
    fi
  done
}

split_args "$@"

for arg in "${USER_BASE_ARGS[@]}"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    usage
    exit 0
  fi
done

typeset -a CMD=(
  zsh
  "$BASE_RUNNER"
  --profile creative_wide
  --out-dir "$OUT_DIR"
  --remote-models "$REMOTE_MODELS"
  --local-share "$LOCAL_SHARE"
  --llm-rounds "$LLM_ROUNDS"
  --llm-candidates-per-round "$LLM_CANDIDATES_PER_ROUND"
  --max-usd-per-run "$MAX_USD_PER_RUN"
  --generator-min-len "$GENERATOR_MIN_LEN"
  --generator-max-len "$GENERATOR_MAX_LEN"
  --llm-prompt-template-file "$PROMPT_TEMPLATE"
  --post-rank-top-n "$POST_RANK_TOP_N"
)

if (( ${#USER_BASE_ARGS[@]} > 0 )); then
  CMD+=("${USER_BASE_ARGS[@]}")
fi

CMD+=(
  --
  --llm-model-selection random
  --llm-temperature 1.02
  --source-influence-shares "$SOURCE_INFLUENCE_SHARES"
  --generator-seeds "$GENERATOR_SEEDS"
  --quota-profiles "$QUOTA_PROFILES"
  --validator-expensive-finalist-limit 36
  --validator-timeout-s 18
  --validator-max-concurrency 18
)

if (( ${#USER_RUNNER_ARGS[@]} > 0 )); then
  CMD+=("${USER_RUNNER_ARGS[@]}")
fi

printf '$ '
printf '%q ' "${CMD[@]}"
echo

exec "${CMD[@]}"
