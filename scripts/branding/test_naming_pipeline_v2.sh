#!/usr/bin/env zsh
set -euo pipefail

# One-command test runner for naming pipeline v2.
# Modes:
# - smoke (default): fast, degraded network, local temp outputs
# - full: writes canonical docs outputs
# - kill-switch: smoke profile + explicit v2 flag-off regression validator check

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MODE="${1:-smoke}"

if [[ "$MODE" != "smoke" && "$MODE" != "full" && "$MODE" != "kill-switch" ]]; then
  echo "Usage: $0 [smoke|full|kill-switch]"
  exit 1
fi

if [[ "$MODE" == "smoke" || "$MODE" == "kill-switch" ]]; then
  DB_PATH="/tmp/naming_pipeline_v2_smoke.db"
  CSV_OUT="/tmp/candidate_batch_v2_smoke.csv"
  JSON_OUT="/tmp/candidate_batch_v2_smoke.json"
  RUN_LOG="/tmp/name_generator_runs_v2_smoke.jsonl"
  VALIDATOR_LOG="/tmp/naming_validator_v2_killswitch.log"
  CHECK_LIMIT=30
  EXTRA_FLAGS=(
    --degraded-network-mode
    --no-domain-check
    --no-store-check
    --no-web-check
    --no-package-check
    --no-social-check
    --no-progress
  )
else
  DB_PATH="$ROOT_DIR/test_outputs/branding/naming_pipeline_v1.db"
  CSV_OUT="$ROOT_DIR/test_outputs/branding/candidate_batch_v2.csv"
  JSON_OUT="$ROOT_DIR/test_outputs/branding/candidate_batch_v2.json"
  RUN_LOG="$ROOT_DIR/test_outputs/branding/name_generator_runs.jsonl"
  CHECK_LIMIT=120
  EXTRA_FLAGS=()
fi

echo "[1/8] Python syntax compile checks..."
python3 -m py_compile \
  "$ROOT_DIR/scripts/branding/name_generator.py" \
  "$ROOT_DIR/scripts/branding/naming_db.py" \
  "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
  "$ROOT_DIR/scripts/branding/naming_validate_async.py"

if [[ "${USE_RUFF:-1}" == "1" ]]; then
  if command -v ruff >/dev/null 2>&1; then
    echo "[2/8] Ruff checks..."
    ruff check \
      "$ROOT_DIR/scripts/branding/name_generator.py" \
      "$ROOT_DIR/scripts/branding/naming_db.py" \
      "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
      "$ROOT_DIR/scripts/branding/naming_validate_async.py"
  else
    echo "[2/8] Ruff requested but not installed (skipping)."
  fi
else
  echo "[2/8] Ruff checks skipped (set USE_RUFF=0 to disable was used)."
fi

if [[ "${USE_BLACK:-0}" == "1" ]]; then
  if command -v black >/dev/null 2>&1; then
    echo "[3/8] Black format check..."
    black --check \
      "$ROOT_DIR/scripts/branding/name_generator.py" \
      "$ROOT_DIR/scripts/branding/naming_db.py" \
      "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
      "$ROOT_DIR/scripts/branding/naming_validate_async.py"
  else
    echo "[3/8] Black requested but not installed (skipping)."
  fi
else
  echo "[3/8] Black format check skipped (set USE_BLACK=1 to enable)."
fi

echo "[4/8] Preparing artifacts..."
if [[ "$MODE" == "smoke" || "$MODE" == "kill-switch" ]]; then
  rm -f "$DB_PATH" "$CSV_OUT" "$JSON_OUT" "$RUN_LOG" "$VALIDATOR_LOG"
fi

echo "[5/8] Ingesting curated source atoms..."
python3 "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
  --db "$DB_PATH" \
  --inputs "$ROOT_DIR/resources/branding/inputs/source_inputs_v2.csv" \
  --source-label=curated_lexicon_v2 \
  --scope=global \
  --gate=balanced \
  --derive-morphology \
  --also-candidates

echo "[6/8] Running generator..."
python3 "$ROOT_DIR/scripts/branding/name_generator.py" \
  --pipeline-version=v2 \
  --scope=global \
  --gate=balanced \
  --variation-profile=expanded \
  --generator-families=source_pool,blend,morphology,seed,suggestive,coined,expression \
  --family-quotas=coined:90,stem:120,suggestive:100,morphology:180,seed:110,expression:70,source_pool:220,blend:180 \
  --source-pool-db="$DB_PATH" \
  --source-pool-limit=600 \
  --source-min-confidence=0.58 \
  --false-friend-lexicon="$ROOT_DIR/resources/branding/lexicon/naming_false_friend_lexicon_v1.md" \
  --false-friend-fail-threshold=28 \
  --gibberish-fail-threshold=35 \
  --pool-size=500 \
  --check-limit="$CHECK_LIMIT" \
  --no-stage-events \
  --persist-db \
  --db="$DB_PATH" \
  --output="$CSV_OUT" \
  --json-output="$JSON_OUT" \
  --run-log="$RUN_LOG" \
  "${EXTRA_FLAGS[@]}"

echo "[7/8] Yield report..."
python3 - "$RUN_LOG" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print("yield_report=missing_run_log")
    raise SystemExit(0)
lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
if not lines:
    print("yield_report=empty_run_log")
    raise SystemExit(0)
entry = json.loads(lines[-1])
families = entry.get("generator_family_counts", {})
print(
    "yield_report "
    f"pipeline={entry.get('pipeline_version')} "
    f"candidates={entry.get('candidate_count')} "
    f"shortlist={entry.get('shortlist_selected_count')} "
    f"strong={entry.get('recommendation_counts', {}).get('strong', 0)} "
    f"consider={entry.get('recommendation_counts', {}).get('consider', 0)} "
    f"families={families}"
)
PY

echo "[8/8] Optional kill-switch regression..."
if [[ "$MODE" == "kill-switch" || "${KILL_SWITCH_CHECK:-0}" == "1" ]]; then
  python3 "$ROOT_DIR/scripts/branding/naming_validate_async.py" \
    --db "$DB_PATH" \
    --pipeline-version=v2 \
    --validation-tier=all \
    --checks=adversarial,psych,descriptive \
    --candidate-limit=20 \
    --concurrency=4 \
    --no-stage-events \
    > "$VALIDATOR_LOG"
  if ! rg -qi 'v3_enabled=?(false|False)' "$VALIDATOR_LOG"; then
    echo "kill_switch_regression=fail reason=v3_flag_not_off"
    exit 1
  fi
  echo "kill_switch_regression=pass log=$VALIDATOR_LOG"
else
  echo "kill_switch_regression=skipped"
fi

echo "Done."
echo "mode=$MODE"
echo "db=$DB_PATH"
echo "csv=$CSV_OUT"
echo "json=$JSON_OUT"
echo "run_log=$RUN_LOG"
echo "v3_runner=$ROOT_DIR/scripts/branding/test_naming_pipeline_v3.sh"
