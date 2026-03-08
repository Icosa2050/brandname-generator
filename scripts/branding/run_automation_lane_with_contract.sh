#!/usr/bin/env zsh
set -euo pipefail
# Avoid noisy "nice(5) failed: operation not permitted" when this script backgrounds lanes.
unsetopt bgnice 2>/dev/null || true

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

ARTIFACT_ROOT="${BRANDING_AUTOMATION_DATA_ROOT:-$ROOT_DIR/test_outputs/branding/automation-data}"
STATE_DIR="$ARTIFACT_ROOT/state"
RUNS_DIR="$ARTIFACT_ROOT/runs"
LOCK_DIR="$ARTIFACT_ROOT/locks"
LOCK_FILE="$LOCK_DIR/branding_generation_queue.lock"
SMOKE_DIR="$ARTIFACT_ROOT/smoke"

WORK_DIR="$ARTIFACT_ROOT/work"
GEN_QUALITY_OUT="$WORK_DIR/automation_openrouter_quality_v4"
GEN_REMOTE_OUT="$WORK_DIR/automation_openrouter_remote_quality_v4"
FUSED_OUT="$WORK_DIR/automation_openrouter_fused_v4"

MAX_WAIT_S="${BRANDING_AUTOMATION_LOCK_WAIT_S:-7200}"
POLL_S=10
GEN_MAX_AGE_S="${BRANDING_AUTOMATION_GEN_MAX_AGE_S:-21600}"
FUS_MAX_AGE_S="${BRANDING_AUTOMATION_FUS_MAX_AGE_S:-21600}"
CONTRACT_SCHEMA_VERSION="branding_automation_contract_v1"
CAMPAIGN_ID="${BRANDING_AUTOMATION_CAMPAIGN_ID:-cmp_$(date -u +%Y%m%d)}"
SHARD_ID="${BRANDING_AUTOMATION_SHARD_ID:-0}"
ATTEMPT_ID="${BRANDING_AUTOMATION_ATTEMPT_ID:-1}"
WORKER_ID="${BRANDING_AUTOMATION_WORKER_ID:-$(hostname -s 2>/dev/null || echo worker)}"
WORKER_ID="${WORKER_ID//[^A-Za-z0-9_.-]/_}"
ENV_BOOTSTRAP_MODE="${BRANDING_AUTOMATION_ENV_BOOTSTRAP_MODE:-none}"
ENV_REQUIRE_DIRENV="${BRANDING_AUTOMATION_REQUIRE_DIRENV:-0}"
ENV_DOTENV_FILE="${BRANDING_AUTOMATION_DOTENV_FILE:-.env}"
ENV_LOADED_WITH_DIRENV=0

usage() {
  cat <<'EOF'
Run branding automation lanes with local artifact contracts (no git sync dependency).

Usage:
  scripts/branding/run_automation_lane_with_contract.sh --lane <generation|fusion|validation|smoke>

Environment:
  BRANDING_AUTOMATION_DATA_ROOT      Artifact root (default: <repo>/test_outputs/branding/automation-data)
  BRANDING_AUTOMATION_LOCK_WAIT_S    Lock wait timeout seconds (default: 7200)
  BRANDING_AUTOMATION_GEN_MAX_AGE_S  Max age for generation artifacts in fusion/validation (default: 21600)
  BRANDING_AUTOMATION_FUS_MAX_AGE_S  Max age for fusion artifacts in validation (default: 21600)
  BRANDING_AUTOMATION_ENV_BOOTSTRAP_MODE
                                   Env bootstrap mode: none|auto|direnv|dotenv (default: none)
  BRANDING_AUTOMATION_DOTENV_FILE   Dotenv file path used in dotenv/auto fallback (default: .env)
EOF
}

lane=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --lane)
      lane="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$lane" ]]; then
  usage
  exit 2
fi

prepare_environment() {
  mkdir -p "$STATE_DIR" "$RUNS_DIR" "$LOCK_DIR" "$SMOKE_DIR" "$WORK_DIR"
}

source_dotenv_file() {
  local env_path="$ENV_DOTENV_FILE"
  if [[ ! -f "$env_path" ]]; then
    echo "dotenv fallback requested but missing file: $env_path" >&2
    return 1
  fi
  set -a
  # shellcheck disable=SC1091
  source "$env_path"
  set +a
}

bootstrap_repo_env() {
  case "$ENV_BOOTSTRAP_MODE" in
    none)
      ENV_LOADED_WITH_DIRENV=0
      return 0
      ;;
    direnv)
      if ! command -v direnv >/dev/null 2>&1; then
        echo "missing required command: direnv (mode=direnv)" >&2
        return 1
      fi
      direnv allow . >/dev/null
      ENV_LOADED_WITH_DIRENV=1
      return 0
      ;;
    dotenv)
      source_dotenv_file
      ENV_LOADED_WITH_DIRENV=0
      return 0
      ;;
    auto)
      if command -v direnv >/dev/null 2>&1; then
        if direnv allow . >/dev/null 2>&1; then
          ENV_LOADED_WITH_DIRENV=1
          return 0
        fi
        if [[ "$ENV_REQUIRE_DIRENV" == "1" ]]; then
          echo "direnv allow failed and BRANDING_AUTOMATION_REQUIRE_DIRENV=1" >&2
          return 1
        fi
        echo "warning: direnv allow failed; falling back to dotenv (.env)" >&2
        source_dotenv_file
        ENV_LOADED_WITH_DIRENV=0
        return 0
      fi
      if [[ "$ENV_REQUIRE_DIRENV" == "1" ]]; then
        echo "missing required command: direnv (auto mode with BRANDING_AUTOMATION_REQUIRE_DIRENV=1)" >&2
        return 1
      fi
      source_dotenv_file
      ENV_LOADED_WITH_DIRENV=0
      ;;
    *)
      echo "invalid BRANDING_AUTOMATION_ENV_BOOTSTRAP_MODE: $ENV_BOOTSTRAP_MODE (expected auto|direnv|dotenv|none)" >&2
      return 1
      ;;
  esac
}

run_in_repo_env() {
  if [[ "$ENV_LOADED_WITH_DIRENV" == "1" ]]; then
    direnv exec . "$@"
    return $?
  fi
  "$@"
}

prepare_environment
bootstrap_repo_env

utc_now() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

epoch_now() {
  date -u +%s
}

run_id_for() {
  local lane_name="$1"
  printf "%s_%s_%s_s%s_a%s_%s_%s" \
    "$lane_name" \
    "$CAMPAIGN_ID" \
    "$(date -u +%Y%m%dT%H%M%SZ)" \
    "$SHARD_ID" \
    "$ATTEMPT_ID" \
    "$WORKER_ID" \
    "$$"
}

json_get() {
  local json_path="$1"
  local key="$2"
  python3 - "$json_path" "$key" <<'PY'
import json
import sys

path = sys.argv[1]
key = sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
value = data.get(key)
if value is None:
    sys.exit(3)
if isinstance(value, (dict, list)):
    print(json.dumps(value, ensure_ascii=True))
else:
    print(value)
PY
}

json_get_optional() {
  local json_path="$1"
  local key="$2"
  python3 - "$json_path" "$key" <<'PY'
import json
import sys

path = sys.argv[1]
key = sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
value = data.get(key)
if value is None:
    raise SystemExit(0)
if isinstance(value, (dict, list)):
    print(json.dumps(value, ensure_ascii=True))
else:
    print(value)
PY
}

write_manifest() {
  local out_path="$1"
  local lane_name="$2"
  local run_id="$3"
  local manifest_status="$4"
  local started_at="$5"
  local completed_at="$6"
  local source_commit="$7"
  local sync_state="$8"
  local details_json="$9"
  local upstream_run_ids_json="${10:-[]}"
  python3 - "$out_path" "$lane_name" "$run_id" "$manifest_status" "$started_at" "$completed_at" "$source_commit" "$sync_state" "$details_json" "$upstream_run_ids_json" "$CONTRACT_SCHEMA_VERSION" <<'PY'
import json
import sys

out_path, lane_name, run_id, manifest_status, started_at, completed_at, source_commit, sync_state, details_json, upstream_json, contract_schema_version = sys.argv[1:]
payload = {
    "lane": lane_name,
    "run_id": run_id,
    "status": manifest_status,
    "started_at": started_at,
    "completed_at": completed_at,
    "source_commit": source_commit,
    "sync_state": sync_state,
    "contract_schema_version": contract_schema_version,
    "upstream_run_ids": json.loads(upstream_json),
    "details": json.loads(details_json),
}
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, ensure_ascii=True)
    fh.write("\n")
PY
}

write_pointer() {
  local out_path="$1"
  local lane_name="$2"
  local run_id="$3"
  local manifest_path="$4"
  local completed_at="$5"
  local extra_json="$6"
  local tmp_path="${out_path}.tmp.$$"
  python3 - "$tmp_path" "$lane_name" "$run_id" "$manifest_path" "$completed_at" "$extra_json" "$CONTRACT_SCHEMA_VERSION" <<'PY'
import json
import sys

tmp_path, lane_name, run_id, manifest_path, completed_at, extra_json, contract_schema_version = sys.argv[1:]
payload = {
    "lane": lane_name,
    "run_id": run_id,
    "manifest_path": manifest_path,
    "completed_at": completed_at,
    "contract_schema_version": contract_schema_version,
}
payload.update(json.loads(extra_json))
with open(tmp_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, ensure_ascii=True)
    fh.write("\n")
PY
  mv "$tmp_path" "$out_path"
}

acquire_lock() {
  local start_ts elapsed
  start_ts="$(epoch_now)"
  while true; do
    if (set -o noclobber; printf '1' > "$LOCK_FILE") 2>/dev/null; then
      trap 'rm -f "$LOCK_FILE"' EXIT INT TERM
      return 0
    fi
    elapsed=$(( $(epoch_now) - start_ts ))
    if (( elapsed >= MAX_WAIT_S )); then
      echo "lock timeout after ${elapsed}s: $LOCK_FILE" >&2
      return 1
    fi
    sleep "$POLL_S"
  done
}

read_pointer_checked() {
  local pointer_path="$1"
  if [[ ! -f "$pointer_path" ]]; then
    echo "missing pointer: $pointer_path" >&2
    return 1
  fi
  local manifest_path
  manifest_path="$(json_get "$pointer_path" "manifest_path")"
  if [[ ! -f "$manifest_path" ]]; then
    echo "pointer manifest missing: $manifest_path" >&2
    return 1
  fi
  local manifest_status
  manifest_status="$(json_get "$manifest_path" "status")"
  if [[ "$manifest_status" != "success" ]]; then
    echo "upstream manifest not successful: $manifest_path (status=$manifest_status)" >&2
    return 1
  fi
  local ptr_schema manifest_schema
  ptr_schema="$(json_get_optional "$pointer_path" "contract_schema_version")"
  manifest_schema="$(json_get_optional "$manifest_path" "contract_schema_version")"
  if [[ -n "$ptr_schema" && "$ptr_schema" != "$CONTRACT_SCHEMA_VERSION" ]]; then
    echo "pointer schema mismatch: $pointer_path expected=$CONTRACT_SCHEMA_VERSION got=$ptr_schema" >&2
    return 1
  fi
  if [[ -n "$manifest_schema" && "$manifest_schema" != "$CONTRACT_SCHEMA_VERSION" ]]; then
    echo "manifest schema mismatch: $manifest_path expected=$CONTRACT_SCHEMA_VERSION got=$manifest_schema" >&2
    return 1
  fi
  echo "$manifest_path"
}

age_guard_from_manifest() {
  local manifest_path="$1"
  local max_age_s="$2"
  local completed_at
  completed_at="$(json_get "$manifest_path" "completed_at")"
  local completed_epoch
  completed_epoch="$(python3 - "$completed_at" <<'PY'
import datetime
import sys

ts = sys.argv[1]
dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
print(int(dt.timestamp()))
PY
)"
  local age_s
  age_s=$(( $(epoch_now) - completed_epoch ))
  if (( age_s > max_age_s )); then
    echo "artifact too old: ${age_s}s > ${max_age_s}s ($manifest_path)" >&2
    return 1
  fi
}

lane_generation() {
  local started_at completed_at run_id run_dir manifest_path commit_sha
  started_at="$(utc_now)"
  run_id="$(run_id_for generation)"
  run_dir="$RUNS_DIR/generation/$run_id"
  mkdir -p "$run_dir"
  manifest_path="$run_dir/manifest.json"
  commit_sha="$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")"

  write_manifest "$manifest_path" "generation" "$run_id" "running" "$started_at" "" "$commit_sha" "local_worktree" "{}" "[]"

  acquire_lock

  local quality_rc=0 remote_rc=0 quality_pid remote_pid
  run_in_repo_env zsh scripts/branding/run_openrouter_lane.sh --profile quality --lane 0 --shard-count 1 --bundle-a --max-runs 1 --no-live-progress --post-rank-top-n 30 --out-dir "$GEN_QUALITY_OUT" &
  quality_pid=$!
  run_in_repo_env zsh scripts/branding/run_openrouter_lane.sh --profile remote_quality --lane 0 --shard-count 1 --bundle-a --max-runs 1 --no-live-progress --post-rank-top-n 30 --out-dir "$GEN_REMOTE_OUT" &
  remote_pid=$!

  set +e
  wait "$quality_pid"
  quality_rc=$?
  wait "$remote_pid"
  remote_rc=$?
  set -e

  completed_at="$(utc_now)"
  if [[ "$quality_rc" -ne 0 || "$remote_rc" -ne 0 ]]; then
    write_manifest "$manifest_path" "generation" "$run_id" "failed" "$started_at" "$completed_at" "$commit_sha" "local_worktree" "{\"quality_rc\": $quality_rc, \"remote_quality_rc\": $remote_rc}" "[]"
    echo "generation failed: quality_rc=$quality_rc remote_quality_rc=$remote_rc" >&2
    return 1
  fi

  write_manifest "$manifest_path" "generation" "$run_id" "success" "$started_at" "$completed_at" "$commit_sha" "local_worktree" "{\"quality_out_dir\": \"$GEN_QUALITY_OUT\", \"remote_quality_out_dir\": \"$GEN_REMOTE_OUT\"}" "[]"
  write_pointer "$STATE_DIR/latest_generation.json" "generation" "$run_id" "$manifest_path" "$completed_at" "{}"
  echo "generation_success run_id=$run_id manifest=$manifest_path"
}

lane_fusion() {
  local started_at completed_at run_id run_dir manifest_path commit_sha
  started_at="$(utc_now)"
  run_id="$(run_id_for fusion)"
  run_dir="$RUNS_DIR/fusion/$run_id"
  mkdir -p "$run_dir"
  manifest_path="$run_dir/manifest.json"
  commit_sha="$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")"

  write_manifest "$manifest_path" "fusion" "$run_id" "running" "$started_at" "" "$commit_sha" "local_worktree" "{}" "[]"

  local gen_manifest
  gen_manifest="$(read_pointer_checked "$STATE_DIR/latest_generation.json")"
  age_guard_from_manifest "$gen_manifest" "$GEN_MAX_AGE_S"
  local consumed_generation_run_id
  consumed_generation_run_id="$(json_get "$gen_manifest" "run_id")"

  acquire_lock

  local fusion_rc=0
  BRANDING_AUTOMATION_QUALITY_OUT_DIR="$GEN_QUALITY_OUT" \
  BRANDING_AUTOMATION_REMOTE_OUT_DIR="$GEN_REMOTE_OUT" \
  BRANDING_AUTOMATION_FUSED_OUT_DIR="$FUSED_OUT" \
    run_in_repo_env zsh scripts/branding/run_openrouter_fusion_fail_closed.sh || fusion_rc=$?

  completed_at="$(utc_now)"
  if [[ "$fusion_rc" -ne 0 ]]; then
    write_manifest "$manifest_path" "fusion" "$run_id" "failed" "$started_at" "$completed_at" "$commit_sha" "local_worktree" "{\"fusion_rc\": $fusion_rc, \"consumed_generation_run_id\": \"$consumed_generation_run_id\"}" "[\"$consumed_generation_run_id\"]"
    echo "fusion failed rc=$fusion_rc" >&2
    return 1
  fi

  write_manifest "$manifest_path" "fusion" "$run_id" "success" "$started_at" "$completed_at" "$commit_sha" "local_worktree" "{\"fused_out_dir\": \"$FUSED_OUT\", \"consumed_generation_run_id\": \"$consumed_generation_run_id\"}" "[\"$consumed_generation_run_id\"]"
  write_pointer "$STATE_DIR/latest_fusion.json" "fusion" "$run_id" "$manifest_path" "$completed_at" "{\"consumed_generation_run_id\": \"$consumed_generation_run_id\"}"
  echo "fusion_success run_id=$run_id manifest=$manifest_path"
}

lane_validation() {
  local started_at completed_at run_id run_dir manifest_path provenance_path commit_sha
  started_at="$(utc_now)"
  run_id="$(run_id_for validation)"
  run_dir="$RUNS_DIR/validation/$run_id"
  mkdir -p "$run_dir"
  manifest_path="$run_dir/manifest.json"
  provenance_path="$run_dir/provenance.json"
  commit_sha="$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")"

  write_manifest "$manifest_path" "validation" "$run_id" "running" "$started_at" "" "$commit_sha" "local_worktree" "{}" "[]"

  local gen_manifest fus_manifest
  gen_manifest="$(read_pointer_checked "$STATE_DIR/latest_generation.json")"
  fus_manifest="$(read_pointer_checked "$STATE_DIR/latest_fusion.json")"
  age_guard_from_manifest "$gen_manifest" "$GEN_MAX_AGE_S"
  age_guard_from_manifest "$fus_manifest" "$FUS_MAX_AGE_S"

  local consumed_generation_run_id consumed_fusion_run_id
  consumed_generation_run_id="$(json_get "$gen_manifest" "run_id")"
  consumed_fusion_run_id="$(json_get "$fus_manifest" "run_id")"

  python3 - "$provenance_path" "$consumed_generation_run_id" "$consumed_fusion_run_id" <<'PY'
import json
import sys

out_path, gen_run, fus_run = sys.argv[1:]
payload = {"consumed_generation_run_id": gen_run, "consumed_fusion_run_id": fus_run}
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, ensure_ascii=True)
    fh.write("\n")
PY

  acquire_lock

  local fused_out_dir
  fused_out_dir="$(python3 - "$fus_manifest" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
print((data.get("details") or {}).get("fused_out_dir", ""))
PY
)"
  if [[ -z "$fused_out_dir" ]]; then
    echo "missing fused_out_dir in fusion manifest: $fus_manifest" >&2
    return 1
  fi

  local q_health_rc=0 r_health_rc=0 q_report_rc=0 r_report_rc=0 fused_integrity_rc=0
  run_in_repo_env python3 scripts/branding/check_campaign_health.py --out-dir "$GEN_QUALITY_OUT" || q_health_rc=$?
  run_in_repo_env python3 scripts/branding/check_campaign_health.py --out-dir "$GEN_REMOTE_OUT" || r_health_rc=$?
  run_in_repo_env zsh scripts/branding/report_campaign_progress.sh --out-dir "$GEN_QUALITY_OUT" --top-n 20 || q_report_rc=$?
  run_in_repo_env zsh scripts/branding/report_campaign_progress.sh --out-dir "$GEN_REMOTE_OUT" --top-n 20 || r_report_rc=$?
  python3 - "$fused_out_dir/postrank/fused_quality_remote_rank.csv" "$fused_out_dir/postrank/fused_quality_remote_summary.json" <<'PY' || fused_integrity_rc=$?
import csv
import json
import os
import sys

rank_csv, summary_json = sys.argv[1], sys.argv[2]
if not os.path.isfile(rank_csv):
    raise SystemExit(2)
if not os.path.isfile(summary_json):
    raise SystemExit(3)
with open(rank_csv, "r", encoding="utf-8", newline="") as fh:
    rows = list(csv.DictReader(fh))
if not rows:
    raise SystemExit(4)
with open(summary_json, "r", encoding="utf-8") as fh:
    payload = json.load(fh)
if not isinstance(payload, dict):
    raise SystemExit(5)
print(f"fused_integrity_ok rows={len(rows)} rank_csv={rank_csv}")
PY

  completed_at="$(utc_now)"
  local fatal=0
  if [[ "$q_report_rc" -ne 0 || "$r_report_rc" -ne 0 ]]; then
    fatal=1
  fi
  if [[ "$q_health_rc" -ne 0 && "$q_health_rc" -ne 3 ]]; then
    fatal=1
  fi
  if [[ "$r_health_rc" -ne 0 && "$r_health_rc" -ne 3 ]]; then
    fatal=1
  fi
  if [[ "$fused_integrity_rc" -ne 0 ]]; then
    fatal=1
  fi

  if [[ "$fatal" -ne 0 ]]; then
    write_manifest "$manifest_path" "validation" "$run_id" "failed" "$started_at" "$completed_at" "$commit_sha" "local_worktree" "{\"q_health_rc\": $q_health_rc, \"r_health_rc\": $r_health_rc, \"q_report_rc\": $q_report_rc, \"r_report_rc\": $r_report_rc, \"fused_integrity_rc\": $fused_integrity_rc, \"fused_out_dir\": \"$fused_out_dir\", \"provenance\": \"$provenance_path\"}" "[\"$consumed_generation_run_id\",\"$consumed_fusion_run_id\"]"
    echo "validation failed: qh=$q_health_rc rh=$r_health_rc qr=$q_report_rc rr=$r_report_rc fi=$fused_integrity_rc" >&2
    return 1
  fi

  local health_state="healthy"
  if [[ "$q_health_rc" -eq 3 || "$r_health_rc" -eq 3 ]]; then
    health_state="warning_unhealthy_campaign"
  fi
  write_manifest "$manifest_path" "validation" "$run_id" "success" "$started_at" "$completed_at" "$commit_sha" "local_worktree" "{\"q_health_rc\": $q_health_rc, \"r_health_rc\": $r_health_rc, \"q_report_rc\": $q_report_rc, \"r_report_rc\": $r_report_rc, \"fused_integrity_rc\": $fused_integrity_rc, \"fused_out_dir\": \"$fused_out_dir\", \"health_state\": \"$health_state\", \"provenance\": \"$provenance_path\"}" "[\"$consumed_generation_run_id\",\"$consumed_fusion_run_id\"]"
  echo "validation_success run_id=$run_id manifest=$manifest_path provenance=$provenance_path"
}

lane_smoke() {
  local ts report smoke_lock_file
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  report="$SMOKE_DIR/smoke_${ts}.txt"
  smoke_lock_file="$LOCK_DIR/branding_generation_queue_smoke.lock"
  {
    echo "timestamp=$(utc_now)"
    echo "root=$ROOT_DIR"
    echo "artifact_root=$ARTIFACT_ROOT"

    rm -f "$smoke_lock_file"
    if (set -o noclobber; printf '1' > "$smoke_lock_file") 2>/dev/null; then
      echo "lock_create=ok"
    else
      echo "lock_create=fail"
    fi
    rm -f "$smoke_lock_file"
    echo "lock_cleanup=$( [[ ! -e "$smoke_lock_file" ]] && echo ok || echo fail )"

    echo "syntax_lane=$(zsh -n scripts/branding/run_openrouter_lane.sh >/dev/null 2>&1 && echo ok || echo fail)"
    echo "syntax_fusion=$(zsh -n scripts/branding/run_openrouter_fusion_fail_closed.sh >/dev/null 2>&1 && echo ok || echo fail)"
    echo "syntax_contract_runner=$(zsh -n scripts/branding/run_automation_lane_with_contract.sh >/dev/null 2>&1 && echo ok || echo fail)"

    local http_models
    http_models="$(run_in_repo_env python3 - <<'PY'
import os
import urllib.request

key = os.environ.get("OPENROUTER_API_KEY", "").strip()
if not key:
    print("missing_key")
    raise SystemExit(0)
request = urllib.request.Request(
    "https://openrouter.ai/api/v1/models",
    headers={"Authorization": f"Bearer {key}"},
)
try:
    with urllib.request.urlopen(request, timeout=20) as response:
        print(response.status)
except Exception:
    print("error")
PY
)"
    echo "openrouter_models_http=${http_models:-none}"
  } | tee "$report"
  echo "smoke_report=$report"
}

case "$lane" in
  generation)
    lane_generation
    ;;
  fusion)
    lane_fusion
    ;;
  validation)
    lane_validation
    ;;
  smoke)
    lane_smoke
    ;;
  *)
    echo "Invalid lane: $lane" >&2
    usage
    exit 2
    ;;
esac
