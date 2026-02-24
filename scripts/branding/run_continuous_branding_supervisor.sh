#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

OUT_DIR="${CONTINUOUS_OUT_DIR:-$ROOT_DIR/test_outputs/branding/continuous_hybrid}"
PRIMARY_BACKEND="${CONTINUOUS_BACKEND:-auto}"          # auto|lmstudio|ollama
FALLBACK_BACKEND="${CONTINUOUS_FALLBACK_BACKEND:-ollama}"  # none|lmstudio|ollama
PROFILE_PLAN_RAW="${CONTINUOUS_PROFILE_PLAN:-fast,fast,quality}"

TARGET_GOOD="${CONTINUOUS_TARGET_GOOD:-120}"
TARGET_STRONG="${CONTINUOUS_TARGET_STRONG:-40}"
MAX_CYCLES="${CONTINUOUS_MAX_CYCLES:-0}"  # 0 => run until target or manual stop

SLEEP_OK_S="${CONTINUOUS_SLEEP_OK_S:-20}"
SLEEP_FAIL_BASE_S="${CONTINUOUS_SLEEP_FAIL_BASE_S:-30}"
SLEEP_FAIL_MAX_S="${CONTINUOUS_SLEEP_FAIL_MAX_S:-300}"
MAX_FAIL_STREAK="${CONTINUOUS_MAX_FAIL_STREAK:-12}"

LMSTUDIO_HEALTH_URL="${LMSTUDIO_HEALTH_URL:-http://127.0.0.1:1234/v1/models}"
OLLAMA_HEALTH_URL="${OLLAMA_HEALTH_URL:-http://127.0.0.1:11434/api/tags}"
HEALTHCHECK=1
DRY_RUN=0

EXTRA_RUNNER_ARGS=()

usage() {
  cat <<'EOF'
Run branding campaigns continuously with retries, profile rotation, and target-based stopping.

Usage:
  scripts/branding/run_continuous_branding_supervisor.sh [options] [-- <extra runner args>]

Options:
  --out-dir <path>                 Persistent campaign output dir (default: test_outputs/branding/continuous_hybrid)
  --backend <auto|lmstudio|ollama> Primary backend selection (default: auto)
  --fallback-backend <none|lmstudio|ollama>
                                   Backend fallback on failed cycle (default: ollama)
  --profile-plan <csv>             Profile rotation per cycle (default: fast,fast,quality)
                                   Allowed profiles: fast, quality, balanced, creative
  --target-good <n>                Stop when strict checked strong+consider >= n (default: 120)
                                   strict = all expensive checks pass/warn and zero fail/error
                                   expensive checks: domain,web,app_store,package,social
  --target-strong <n>              Stop when strict checked strong >= n (default: 40)
  --max-cycles <n>                 Max supervisor cycles; 0 => unlimited (default: 0)
  --sleep-ok-s <seconds>           Sleep after successful cycle (default: 20)
  --sleep-fail-base-s <seconds>    Failure backoff base (default: 30)
  --sleep-fail-max-s <seconds>     Failure backoff cap (default: 300)
  --max-fail-streak <n>            Abort after this many consecutive failures (default: 12)
  --no-healthcheck                 Skip local backend health probes
  --dry-run                        Print intended cycle commands without running them
  -h, --help                       Show this help

Profile definitions:
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

  balanced:
    --local-share 0.75
    --llm-rounds 2
    --llm-candidates-per-round 12

  creative:
    --local-share 0.20
    --llm-rounds 6
    --llm-candidates-per-round 14
    --generator-min-len 8
    --generator-max-len 14
    --llm-prompt-template-file resources/branding/llm/llm_prompt.creative_longer_names_v1.txt
    --validator-expensive-finalist-limit 24
    --validator-timeout-s 14
    --validator-max-concurrency 16

Notes:
  - Uses existing wrappers:
    - scripts/branding/run_hybrid_lmstudio_mistral.sh
    - scripts/branding/run_hybrid_ollama_mistral.sh
  - Extra args after '--' are forwarded to naming_campaign_runner.py.
EOF
}

is_nonneg_int() {
  [[ "${1:-}" == <-> ]]
}

normalize_backend() {
  local raw="${1:-}"
  local lowered="${raw:l}"
  case "$lowered" in
    auto|lmstudio|ollama|none)
      print -r -- "$lowered"
      ;;
    *)
      return 1
      ;;
  esac
}

timestamp_utc() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

render_cmd() {
  local part
  local rendered=""
  for part in "$@"; do
    rendered+="${(q)part} "
  done
  print -r -- "${rendered% }"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --backend)
      PRIMARY_BACKEND="$2"
      shift 2
      ;;
    --fallback-backend)
      FALLBACK_BACKEND="$2"
      shift 2
      ;;
    --profile-plan)
      PROFILE_PLAN_RAW="$2"
      shift 2
      ;;
    --target-good)
      TARGET_GOOD="$2"
      shift 2
      ;;
    --target-strong)
      TARGET_STRONG="$2"
      shift 2
      ;;
    --max-cycles)
      MAX_CYCLES="$2"
      shift 2
      ;;
    --sleep-ok-s)
      SLEEP_OK_S="$2"
      shift 2
      ;;
    --sleep-fail-base-s)
      SLEEP_FAIL_BASE_S="$2"
      shift 2
      ;;
    --sleep-fail-max-s)
      SLEEP_FAIL_MAX_S="$2"
      shift 2
      ;;
    --max-fail-streak)
      MAX_FAIL_STREAK="$2"
      shift 2
      ;;
    --no-healthcheck)
      HEALTHCHECK=0
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_RUNNER_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if ! PRIMARY_BACKEND="$(normalize_backend "$PRIMARY_BACKEND")"; then
  echo "Invalid --backend: $PRIMARY_BACKEND (expected auto|lmstudio|ollama)." >&2
  exit 2
fi
if ! FALLBACK_BACKEND="$(normalize_backend "$FALLBACK_BACKEND")"; then
  echo "Invalid --fallback-backend: $FALLBACK_BACKEND (expected none|lmstudio|ollama)." >&2
  exit 2
fi
for raw in "$TARGET_GOOD" "$TARGET_STRONG" "$MAX_CYCLES" "$SLEEP_OK_S" "$SLEEP_FAIL_BASE_S" "$SLEEP_FAIL_MAX_S" "$MAX_FAIL_STREAK"; do
  if ! is_nonneg_int "$raw"; then
    echo "Expected non-negative integer, got: $raw" >&2
    exit 2
  fi
done

typeset -a PROFILE_PLAN=()
typeset -a PROFILE_LOCAL_ARGS=()
typeset -a PROFILE_VALIDATOR_ARGS=()
for raw_profile in ${(s:,:)PROFILE_PLAN_RAW}; do
  local_profile="${raw_profile:l}"
  local_profile="${local_profile//[[:space:]]/}"
  [[ -z "$local_profile" ]] && continue
  case "$local_profile" in
    fast|quality|balanced|creative)
      PROFILE_PLAN+=("$local_profile")
      ;;
    *)
      echo "Invalid profile in --profile-plan: $local_profile (allowed: fast,quality,balanced,creative)." >&2
      exit 2
      ;;
  esac
done
if (( ${#PROFILE_PLAN[@]} == 0 )); then
  echo "Profile plan is empty. Provide --profile-plan with at least one profile." >&2
  exit 2
fi

runner_for_backend() {
  case "$1" in
    lmstudio)
      print -r -- "$ROOT_DIR/scripts/branding/run_hybrid_lmstudio_mistral.sh"
      ;;
    ollama)
      print -r -- "$ROOT_DIR/scripts/branding/run_hybrid_ollama_mistral.sh"
      ;;
    *)
      return 1
      ;;
  esac
}

backend_health() {
  local backend="$1"
  if (( ! HEALTHCHECK )); then
    return 0
  fi
  case "$backend" in
    lmstudio)
      curl -fsS --max-time 3 "$LMSTUDIO_HEALTH_URL" >/dev/null 2>&1
      ;;
    ollama)
      curl -fsS --max-time 3 "$OLLAMA_HEALTH_URL" >/dev/null 2>&1
      ;;
    *)
      return 1
      ;;
  esac
}

choose_backend_for_cycle() {
  if [[ "$PRIMARY_BACKEND" == "auto" ]]; then
    if backend_health lmstudio; then
      print -r -- "lmstudio"
      return
    fi
    if backend_health ollama; then
      print -r -- "ollama"
      return
    fi
    # Last resort preference for command construction.
    print -r -- "lmstudio"
    return
  fi

  if backend_health "$PRIMARY_BACKEND"; then
    print -r -- "$PRIMARY_BACKEND"
    return
  fi

  if [[ "$FALLBACK_BACKEND" != "none" && "$FALLBACK_BACKEND" != "$PRIMARY_BACKEND" ]] && backend_health "$FALLBACK_BACKEND"; then
    print -r -- "$FALLBACK_BACKEND"
    return
  fi

  print -r -- "$PRIMARY_BACKEND"
}

build_profile_args() {
  local profile="$1"
  PROFILE_LOCAL_ARGS=()
  PROFILE_VALIDATOR_ARGS=()
  case "$profile" in
    fast)
      PROFILE_LOCAL_ARGS=(--local-share 1.0 --llm-rounds 1 --llm-candidates-per-round 24)
      PROFILE_VALIDATOR_ARGS=(--validator-tier cheap)
      ;;
    quality)
      PROFILE_LOCAL_ARGS=(--local-share 0.75 --llm-rounds 4 --llm-candidates-per-round 12)
      PROFILE_VALIDATOR_ARGS=(
        --validator-expensive-finalist-limit 20
        --validator-timeout-s 12
        --validator-max-concurrency 16
      )
      ;;
    balanced)
      PROFILE_LOCAL_ARGS=(--local-share 0.75 --llm-rounds 2 --llm-candidates-per-round 12)
      ;;
    creative)
      PROFILE_LOCAL_ARGS=(--local-share 0.20 --llm-rounds 6 --llm-candidates-per-round 14)
      PROFILE_VALIDATOR_ARGS=(
        --generator-min-len 8
        --generator-max-len 14
        --llm-prompt-template-file "$ROOT_DIR/resources/branding/llm/llm_prompt.creative_longer_names_v1.txt"
        --validator-expensive-finalist-limit 24
        --validator-timeout-s 14
        --validator-max-concurrency 16
      )
      ;;
    *)
      echo "Unknown profile: $profile" >&2
      return 1
      ;;
  esac
}

STATE_DIR="$OUT_DIR/continuous"
LOG_DIR="$STATE_DIR/logs"
HEARTBEAT_LOG="$STATE_DIR/supervisor_heartbeat.log"
LOCK_DIR="$STATE_DIR/.supervisor_lock"
DB_PATH="$OUT_DIR/naming_campaign.db"
mkdir -p "$LOG_DIR"

log_event() {
  local line="continuous_supervisor ts=$(timestamp_utc) $*"
  echo "$line"
  echo "$line" >> "$HEARTBEAT_LOG"
}

sqlite_scalar() {
  local sql="$1"
  if [[ ! -f "$DB_PATH" ]]; then
    print -r -- "0"
    return
  fi
  local raw
  raw="$(sqlite3 -cmd ".timeout 3000" -noheader "$DB_PATH" "$sql" 2>/dev/null || true)"
  raw="${raw//$'\r'/}"
  raw="${raw//$'\n'/}"
  raw="${raw//[[:space:]]/}"
  if [[ -z "$raw" ]]; then
    raw="0"
  fi
  print -r -- "$raw"
}

sqlite_text() {
  local sql="$1"
  if [[ ! -f "$DB_PATH" ]]; then
    print -r -- ""
    return
  fi
  local raw
  raw="$(sqlite3 -cmd ".timeout 3000" -noheader "$DB_PATH" "$sql" 2>/dev/null || true)"
  raw="${raw//$'\r'/}"
  raw="${raw//$'\n'/}"
  print -r -- "$raw"
}

GOOD_COUNT=0
STRONG_COUNT=0
STRICT_GOOD_COUNT=0
STRICT_STRONG_COUNT=0
SHORTLIST_GOOD_COUNT=0
SHORTLIST_STRICT_GOOD_COUNT=0
UNIQUE_SHORTLIST_COUNT=0
TOTAL_CANDIDATES=0
RUN_COUNT=0
LAST_RUN_STATUS=""

collect_metrics() {
  GOOD_COUNT="$(sqlite_scalar "SELECT COUNT(*) FROM candidates WHERE state='checked' AND current_recommendation IN ('strong','consider');")"
  STRONG_COUNT="$(sqlite_scalar "SELECT COUNT(*) FROM candidates WHERE state='checked' AND current_recommendation='strong';")"
  STRICT_GOOD_COUNT="$(sqlite_scalar "WITH exp AS (SELECT candidate_id, COUNT(DISTINCT CASE WHEN check_type IN ('domain','web','app_store','package','social') AND status IN ('pass','warn') THEN check_type END) AS ok_types, SUM(CASE WHEN check_type IN ('domain','web','app_store','package','social') AND status IN ('fail','error') THEN 1 ELSE 0 END) AS bad_cnt FROM validation_results GROUP BY candidate_id) SELECT COUNT(*) FROM candidates c LEFT JOIN exp e ON e.candidate_id=c.id WHERE c.state='checked' AND c.current_recommendation IN ('strong','consider') AND IFNULL(e.bad_cnt,0)=0 AND IFNULL(e.ok_types,0)=5;")"
  STRICT_STRONG_COUNT="$(sqlite_scalar "WITH exp AS (SELECT candidate_id, COUNT(DISTINCT CASE WHEN check_type IN ('domain','web','app_store','package','social') AND status IN ('pass','warn') THEN check_type END) AS ok_types, SUM(CASE WHEN check_type IN ('domain','web','app_store','package','social') AND status IN ('fail','error') THEN 1 ELSE 0 END) AS bad_cnt FROM validation_results GROUP BY candidate_id) SELECT COUNT(*) FROM candidates c LEFT JOIN exp e ON e.candidate_id=c.id WHERE c.state='checked' AND c.current_recommendation='strong' AND IFNULL(e.bad_cnt,0)=0 AND IFNULL(e.ok_types,0)=5;")"
  SHORTLIST_GOOD_COUNT="$(sqlite_scalar "SELECT COUNT(DISTINCT c.id) FROM candidates c JOIN shortlist_decisions s ON s.candidate_id=c.id WHERE s.selected=1 AND c.state='checked' AND c.current_recommendation IN ('strong','consider');")"
  SHORTLIST_STRICT_GOOD_COUNT="$(sqlite_scalar "WITH exp AS (SELECT candidate_id, COUNT(DISTINCT CASE WHEN check_type IN ('domain','web','app_store','package','social') AND status IN ('pass','warn') THEN check_type END) AS ok_types, SUM(CASE WHEN check_type IN ('domain','web','app_store','package','social') AND status IN ('fail','error') THEN 1 ELSE 0 END) AS bad_cnt FROM validation_results GROUP BY candidate_id) SELECT COUNT(DISTINCT c.id) FROM shortlist_decisions s JOIN candidates c ON c.id=s.candidate_id LEFT JOIN exp e ON e.candidate_id=c.id WHERE s.selected=1 AND c.state='checked' AND c.current_recommendation IN ('strong','consider') AND IFNULL(e.bad_cnt,0)=0 AND IFNULL(e.ok_types,0)=5;")"
  UNIQUE_SHORTLIST_COUNT="$(sqlite_scalar "SELECT COUNT(DISTINCT c.name_normalized) FROM candidates c JOIN shortlist_decisions s ON s.candidate_id=c.id WHERE s.selected=1;")"
  TOTAL_CANDIDATES="$(sqlite_scalar "SELECT COUNT(*) FROM candidates;")"
  RUN_COUNT="$(sqlite_scalar "SELECT IFNULL(MAX(id),0) FROM naming_runs;")"
  LAST_RUN_STATUS="$(sqlite_text "SELECT COALESCE(status,'') FROM naming_runs ORDER BY id DESC LIMIT 1;")"
}

cleanup() {
  rm -rf "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  existing_pid=""
  if [[ -f "$LOCK_DIR/pid" ]]; then
    existing_pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
  fi
  if [[ -n "$existing_pid" && "$existing_pid" == <-> ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
    echo "Supervisor lock already exists: $LOCK_DIR (pid=$existing_pid)" >&2
    exit 3
  fi
  echo "Recovering stale supervisor lock: $LOCK_DIR (stale_pid=${existing_pid:-unknown})"
  rm -rf "$LOCK_DIR"
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "Failed to recover lock directory: $LOCK_DIR" >&2
    exit 3
  fi
fi
echo "$$" > "$LOCK_DIR/pid"

log_event event=start out_dir="$OUT_DIR" backend="$PRIMARY_BACKEND" fallback="$FALLBACK_BACKEND" profile_plan="$PROFILE_PLAN_RAW" target_good="$TARGET_GOOD" target_strong="$TARGET_STRONG" max_cycles="$MAX_CYCLES" dry_run="$DRY_RUN"

collect_metrics
prev_good_count="$GOOD_COUNT"
prev_strong_count="$STRONG_COUNT"
prev_strict_good_count="$STRICT_GOOD_COUNT"
prev_strict_strong_count="$STRICT_STRONG_COUNT"
prev_total_candidates="$TOTAL_CANDIDATES"

cycle=0
fail_streak=0

while true; do
  cycle=$((cycle + 1))
  if (( MAX_CYCLES > 0 && cycle > MAX_CYCLES )); then
    log_event event=stop reason=max_cycles cycle="$cycle"
    break
  fi

  profile_index=$(( ((cycle - 1) % ${#PROFILE_PLAN[@]}) + 1 ))
  profile="${PROFILE_PLAN[$profile_index]}"
  backend="$(choose_backend_for_cycle)"
  if ! runner_path="$(runner_for_backend "$backend")"; then
    log_event event=cycle_fail cycle="$cycle" profile="$profile" backend="$backend" reason=invalid_backend
    exit 4
  fi

  build_profile_args "$profile"
  cmd=(
    zsh "$runner_path"
    --out-dir "$OUT_DIR"
    --max-runs 1
    --sleep-s 0
    --no-live-progress
    "${PROFILE_LOCAL_ARGS[@]}"
  )
  if (( ${#PROFILE_VALIDATOR_ARGS[@]} > 0 || ${#EXTRA_RUNNER_ARGS[@]} > 0 )); then
    cmd+=(--)
    if (( ${#PROFILE_VALIDATOR_ARGS[@]} > 0 )); then
      cmd+=("${PROFILE_VALIDATOR_ARGS[@]}")
    fi
    if (( ${#EXTRA_RUNNER_ARGS[@]} > 0 )); then
      cmd+=("${EXTRA_RUNNER_ARGS[@]}")
    fi
  fi

  cycle_stamp="$(date +%Y%m%d_%H%M%S)"
  cycle_log="$LOG_DIR/cycle_${cycle}_${backend}_${profile}_${cycle_stamp}.log"
  log_event event=cycle_start cycle="$cycle" profile="$profile" backend="$backend" cmd="$(render_cmd "${cmd[@]}")"

  rc=0
  if (( DRY_RUN )); then
    log_event event=cycle_dry_run cycle="$cycle" profile="$profile" backend="$backend"
  else
    set +e
    "${cmd[@]}" > "$cycle_log" 2>&1
    rc=$?
    set -e
  fi

  used_fallback=0
  if (( rc != 0 )) && [[ "$FALLBACK_BACKEND" != "none" && "$FALLBACK_BACKEND" != "$backend" ]]; then
    if runner_fallback="$(runner_for_backend "$FALLBACK_BACKEND" 2>/dev/null)"; then
      fallback_cmd=(
        zsh "$runner_fallback"
        --out-dir "$OUT_DIR"
        --max-runs 1
        --sleep-s 0
        --no-live-progress
        "${PROFILE_LOCAL_ARGS[@]}"
      )
      if (( ${#PROFILE_VALIDATOR_ARGS[@]} > 0 || ${#EXTRA_RUNNER_ARGS[@]} > 0 )); then
        fallback_cmd+=(--)
        if (( ${#PROFILE_VALIDATOR_ARGS[@]} > 0 )); then
          fallback_cmd+=("${PROFILE_VALIDATOR_ARGS[@]}")
        fi
        if (( ${#EXTRA_RUNNER_ARGS[@]} > 0 )); then
          fallback_cmd+=("${EXTRA_RUNNER_ARGS[@]}")
        fi
      fi
      fallback_log="$LOG_DIR/cycle_${cycle}_${FALLBACK_BACKEND}_${profile}_${cycle_stamp}_fallback.log"
      log_event event=fallback_start cycle="$cycle" from_backend="$backend" to_backend="$FALLBACK_BACKEND" profile="$profile" cmd="$(render_cmd "${fallback_cmd[@]}")"
      set +e
      "${fallback_cmd[@]}" > "$fallback_log" 2>&1
      rc=$?
      set -e
      if (( rc == 0 )); then
        used_fallback=1
        backend="$FALLBACK_BACKEND"
        cycle_log="$fallback_log"
      fi
    fi
  fi

  collect_metrics
  new_good_count=$(( GOOD_COUNT - prev_good_count ))
  new_strong_count=$(( STRONG_COUNT - prev_strong_count ))
  new_strict_good_count=$(( STRICT_GOOD_COUNT - prev_strict_good_count ))
  new_strict_strong_count=$(( STRICT_STRONG_COUNT - prev_strict_strong_count ))
  new_total_candidates=$(( TOTAL_CANDIDATES - prev_total_candidates ))
  prev_good_count="$GOOD_COUNT"
  prev_strong_count="$STRONG_COUNT"
  prev_strict_good_count="$STRICT_GOOD_COUNT"
  prev_strict_strong_count="$STRICT_STRONG_COUNT"
  prev_total_candidates="$TOTAL_CANDIDATES"

  if (( rc == 0 )); then
    fail_streak=0
    log_event event=cycle_ok cycle="$cycle" profile="$profile" backend="$backend" used_fallback="$used_fallback" new_good="$new_good_count" new_strong="$new_strong_count" new_strict_good="$new_strict_good_count" new_strict_strong="$new_strict_strong_count" new_candidates="$new_total_candidates" good_total="$GOOD_COUNT" strong_total="$STRONG_COUNT" strict_good_total="$STRICT_GOOD_COUNT" strict_strong_total="$STRICT_STRONG_COUNT" shortlist_good_total="$SHORTLIST_GOOD_COUNT" shortlist_strict_good_total="$SHORTLIST_STRICT_GOOD_COUNT" unique_shortlist="$UNIQUE_SHORTLIST_COUNT" candidates_total="$TOTAL_CANDIDATES" run_count="$RUN_COUNT" last_run_status="${LAST_RUN_STATUS:-none}" log="$cycle_log"

    target_good_ok=1
    target_strong_ok=1
    if (( TARGET_GOOD > 0 && STRICT_GOOD_COUNT < TARGET_GOOD )); then
      target_good_ok=0
    fi
    if (( TARGET_STRONG > 0 && STRICT_STRONG_COUNT < TARGET_STRONG )); then
      target_strong_ok=0
    fi
    if (( (TARGET_GOOD > 0 || TARGET_STRONG > 0) && target_good_ok == 1 && target_strong_ok == 1 )); then
      log_event event=target_reached cycle="$cycle" strict_good_total="$STRICT_GOOD_COUNT" strict_strong_total="$STRICT_STRONG_COUNT" good_total="$GOOD_COUNT" strong_total="$STRONG_COUNT" shortlist_good_total="$SHORTLIST_GOOD_COUNT" shortlist_strict_good_total="$SHORTLIST_STRICT_GOOD_COUNT" unique_shortlist="$UNIQUE_SHORTLIST_COUNT"
      exit 0
    fi

    if (( SLEEP_OK_S > 0 )); then
      sleep "$SLEEP_OK_S"
    fi
    continue
  fi

  fail_streak=$((fail_streak + 1))
  backoff_s=$((SLEEP_FAIL_BASE_S * fail_streak))
  if (( backoff_s > SLEEP_FAIL_MAX_S )); then
    backoff_s="$SLEEP_FAIL_MAX_S"
  fi
  log_event event=cycle_fail cycle="$cycle" profile="$profile" backend="$backend" fail_streak="$fail_streak" backoff_s="$backoff_s" log="$cycle_log"
  if (( fail_streak >= MAX_FAIL_STREAK )); then
    log_event event=abort reason=max_fail_streak fail_streak="$fail_streak" max_fail_streak="$MAX_FAIL_STREAK"
    exit 1
  fi
  if (( backoff_s > 0 )); then
    sleep "$backoff_s"
  fi
done

exit 0
