#!/usr/bin/env zsh
set -euo pipefail

# One-command test runner for naming pipeline v2.
# Modes:
# - smoke (default): fast, degraded network, local temp outputs
# - full: writes canonical docs outputs

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MODE="${1:-smoke}"

if [[ "$MODE" != "smoke" && "$MODE" != "full" ]]; then
  echo "Usage: $0 [smoke|full]"
  exit 1
fi

if [[ "$MODE" == "smoke" ]]; then
  DB_PATH="/tmp/naming_pipeline_v2_smoke.db"
  CSV_OUT="/tmp/candidate_batch_v2_smoke.csv"
  JSON_OUT="/tmp/candidate_batch_v2_smoke.json"
  RUN_LOG="/tmp/name_generator_runs_v2_smoke.jsonl"
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
  DB_PATH="$ROOT_DIR/docs/branding/naming_pipeline_v1.db"
  CSV_OUT="$ROOT_DIR/docs/branding/candidate_batch_v2.csv"
  JSON_OUT="$ROOT_DIR/docs/branding/candidate_batch_v2.json"
  RUN_LOG="$ROOT_DIR/docs/branding/name_generator_runs.jsonl"
  CHECK_LIMIT=120
  EXTRA_FLAGS=()
fi

echo "[1/6] Python syntax compile checks..."
python3 -m py_compile \
  "$ROOT_DIR/scripts/branding/name_generator.py" \
  "$ROOT_DIR/scripts/branding/naming_db.py" \
  "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
  "$ROOT_DIR/scripts/branding/naming_validate_async.py"

if [[ "${USE_RUFF:-1}" == "1" ]]; then
  if command -v ruff >/dev/null 2>&1; then
    echo "[2/6] Ruff checks..."
    ruff check \
      "$ROOT_DIR/scripts/branding/name_generator.py" \
      "$ROOT_DIR/scripts/branding/naming_db.py" \
      "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
      "$ROOT_DIR/scripts/branding/naming_validate_async.py"
  else
    echo "[2/6] Ruff requested but not installed (skipping)."
  fi
else
  echo "[2/6] Ruff checks skipped (set USE_RUFF=0 to disable was used)."
fi

if [[ "${USE_BLACK:-0}" == "1" ]]; then
  if command -v black >/dev/null 2>&1; then
    echo "[3/6] Black format check..."
    black --check \
      "$ROOT_DIR/scripts/branding/name_generator.py" \
      "$ROOT_DIR/scripts/branding/naming_db.py" \
      "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
      "$ROOT_DIR/scripts/branding/naming_validate_async.py"
  else
    echo "[3/6] Black requested but not installed (skipping)."
  fi
else
  echo "[3/6] Black format check skipped (set USE_BLACK=1 to enable)."
fi

echo "[4/6] Preparing artifacts..."
if [[ "$MODE" == "smoke" ]]; then
  rm -f "$DB_PATH" "$CSV_OUT" "$JSON_OUT" "$RUN_LOG"
fi

echo "[5/6] Ingesting curated source atoms..."
python3 "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
  --db "$DB_PATH" \
  --inputs "$ROOT_DIR/docs/branding/source_inputs_v2.csv" \
  --source-label=curated_lexicon_v2 \
  --scope=global \
  --gate=balanced \
  --also-candidates

echo "[6/6] Running generator..."
python3 "$ROOT_DIR/scripts/branding/name_generator.py" \
  --scope=global \
  --gate=balanced \
  --variation-profile=expanded \
  --generator-families=source_pool,blend,seed,suggestive,coined,expression \
  --family-quotas=coined:100,stem:140,suggestive:110,seed:120,expression:80,source_pool:260,blend:240 \
  --source-pool-db="$DB_PATH" \
  --source-pool-limit=600 \
  --source-min-confidence=0.58 \
  --false-friend-lexicon="$ROOT_DIR/docs/branding/naming_false_friend_lexicon_v1.md" \
  --false-friend-fail-threshold=28 \
  --gibberish-fail-threshold=35 \
  --pool-size=500 \
  --check-limit="$CHECK_LIMIT" \
  --persist-db \
  --db="$DB_PATH" \
  --output="$CSV_OUT" \
  --json-output="$JSON_OUT" \
  --run-log="$RUN_LOG" \
  "${EXTRA_FLAGS[@]}"

echo "Done."
echo "mode=$MODE"
echo "db=$DB_PATH"
echo "csv=$CSV_OUT"
echo "json=$JSON_OUT"
echo "run_log=$RUN_LOG"
