#!/usr/bin/env zsh
set -euo pipefail

# Dedicated v3 pipeline runner.
# Modes:
# - smoke (default): deterministic, no external network checks
# - full: canonical docs outputs with full external checks enabled

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MODE="${1:-smoke}"

if [[ "$MODE" != "smoke" && "$MODE" != "full" ]]; then
  echo "Usage: $0 [smoke|full]"
  exit 1
fi

if [[ "$MODE" == "smoke" ]]; then
  DB_PATH="/tmp/naming_pipeline_v3_smoke.db"
  CSV_OUT="/tmp/candidate_batch_v3_smoke.csv"
  JSON_OUT="/tmp/candidate_batch_v3_smoke.json"
  RUN_LOG="/tmp/name_generator_runs_v3_smoke.jsonl"
  VALIDATOR_LOG="/tmp/naming_validator_v3_smoke.log"
  CHECK_LIMIT=36
  GENERATOR_EXTRA_FLAGS=(
    --degraded-network-mode
    --no-domain-check
    --no-store-check
    --no-web-check
    --no-package-check
    --no-social-check
    --no-progress
  )
  VALIDATOR_ARGS=(
    --checks=adversarial,psych,descriptive,tm_cheap
    --validation-tier=cheap
    --candidate-limit=40
    --concurrency=6
    --no-progress
  )
else
  DB_PATH="$ROOT_DIR/docs/branding/naming_pipeline_v1.db"
  CSV_OUT="$ROOT_DIR/docs/branding/candidate_batch_v3.csv"
  JSON_OUT="$ROOT_DIR/docs/branding/candidate_batch_v3.json"
  RUN_LOG="$ROOT_DIR/docs/branding/name_generator_runs_v3.jsonl"
  VALIDATOR_LOG="$ROOT_DIR/docs/branding/naming_validator_v3.log"
  CHECK_LIMIT=140
  GENERATOR_EXTRA_FLAGS=()
  VALIDATOR_ARGS=(
    --checks=adversarial,psych,descriptive,tm_cheap,domain,web,app_store,package,social
    --validation-tier=all
    --candidate-limit=100
    --concurrency=10
  )
fi

echo "[1/8] Python syntax compile checks..."
python3 -m py_compile \
  "$ROOT_DIR/scripts/branding/name_generator.py" \
  "$ROOT_DIR/scripts/branding/naming_db.py" \
  "$ROOT_DIR/scripts/branding/name_ideation_ingest.py" \
  "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
  "$ROOT_DIR/scripts/branding/naming_validate_async.py"

if command -v ruff >/dev/null 2>&1; then
  echo "[2/8] Ruff checks..."
  ruff check \
    "$ROOT_DIR/scripts/branding/name_generator.py" \
    "$ROOT_DIR/scripts/branding/naming_db.py" \
    "$ROOT_DIR/scripts/branding/name_ideation_ingest.py" \
    "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
    "$ROOT_DIR/scripts/branding/naming_validate_async.py"
else
  echo "[2/8] Ruff not installed (skipping)."
fi

echo "[3/8] Preparing artifacts..."
if [[ "$MODE" == "smoke" ]]; then
  rm -f "$DB_PATH" "$CSV_OUT" "$JSON_OUT" "$RUN_LOG" "$VALIDATOR_LOG"
fi

echo "[4/8] Ingesting curated + morphology source atoms..."
python3 "$ROOT_DIR/scripts/branding/name_input_ingest.py" \
  --db "$DB_PATH" \
  --inputs "$ROOT_DIR/docs/branding/source_inputs_v2.csv" \
  --source-label=curated_lexicon_v2 \
  --scope=global \
  --gate=balanced \
  --derive-morphology \
  --morph-confidence-scale=0.72 \
  --also-candidates

echo "[5/8] Running v3 generator..."
python3 "$ROOT_DIR/scripts/branding/name_generator.py" \
  --pipeline-version=v3 \
  --enable-v3 \
  --use-engine-interfaces \
  --use-tiered-validation \
  --scope=global \
  --gate=balanced \
  --variation-profile=expanded \
  --generator-families=source_pool,morphology,blend,seed,suggestive,coined,expression \
  --family-quotas=coined:90,stem:120,suggestive:100,morphology:220,seed:120,expression:70,source_pool:230,blend:200 \
  --source-pool-db="$DB_PATH" \
  --source-pool-limit=700 \
  --source-min-confidence=0.58 \
  --false-friend-lexicon="$ROOT_DIR/docs/branding/naming_false_friend_lexicon_v1.md" \
  --false-friend-fail-threshold=28 \
  --gibberish-fail-threshold=35 \
  --pool-size=520 \
  --check-limit="$CHECK_LIMIT" \
  --shortlist-size=50 \
  --shortlist-max-bucket=2 \
  --shortlist-max-prefix3=2 \
  --shortlist-max-phonetic=1 \
  --persist-db \
  --db="$DB_PATH" \
  --output="$CSV_OUT" \
  --json-output="$JSON_OUT" \
  --run-log="$RUN_LOG" \
  "${GENERATOR_EXTRA_FLAGS[@]}"

echo "[6/8] Running async validator..."
if [[ "$MODE" == "smoke" ]]; then
  python3 "$ROOT_DIR/scripts/branding/naming_validate_async.py" \
    --db "$DB_PATH" \
    --pipeline-version=v3 \
    --enable-v3 \
    --state-filter=new,checked \
    --scope=global \
    --gate=balanced \
    --expensive-finalist-limit=25 \
    --finalist-recommendations=strong,consider \
    "${VALIDATOR_ARGS[@]}" \
    > "$VALIDATOR_LOG"
  python3 "$ROOT_DIR/scripts/branding/naming_validate_async.py" \
    --db "$DB_PATH" \
    --pipeline-version=v3 \
    --enable-v3 \
    --state-filter=new,checked \
    --scope=global \
    --gate=balanced \
    --expensive-finalist-limit=25 \
    --finalist-recommendations=strong,consider \
    "${VALIDATOR_ARGS[@]}" \
    >> "$VALIDATOR_LOG"
else
  python3 "$ROOT_DIR/scripts/branding/naming_validate_async.py" \
    --db "$DB_PATH" \
    --pipeline-version=v3 \
    --enable-v3 \
    --state-filter=new,checked \
    --scope=global \
    --gate=balanced \
    --expensive-finalist-limit=25 \
    --finalist-recommendations=strong,consider \
    "${VALIDATOR_ARGS[@]}" \
    > "$VALIDATOR_LOG"
fi

echo "[7/8] Contract assertion (deterministic smoke gate)..."
python3 "$ROOT_DIR/scripts/branding/naming_db.py" \
  --db "$DB_PATH" \
  assert-contract \
  --min-candidates=10 \
  --require-shortlist

echo "[8/8] Yield + validator report..."
python3 - "$RUN_LOG" "$VALIDATOR_LOG" <<'PY'
import json
import re
import sys
from pathlib import Path

run_log = Path(sys.argv[1])
validator_log = Path(sys.argv[2])

if run_log.exists():
    lines = [line.strip() for line in run_log.read_text(encoding="utf-8").splitlines() if line.strip()]
    if lines:
        entry = json.loads(lines[-1])
        print(
            "v3_yield_report "
            f"pipeline={entry.get('pipeline_version')} "
            f"candidates={entry.get('candidate_count')} "
            f"shortlist={entry.get('shortlist_selected_count')} "
            f"families={entry.get('generator_family_counts', {})}"
        )

if validator_log.exists():
    text = validator_log.read_text(encoding="utf-8")
    matches = [
        line[len("run_summary="):]
        for line in text.splitlines()
        if line.startswith("run_summary=")
    ]
    if matches:
        payload = json.loads(matches[-1])
        print(
            "v3_validator_report "
            f"total_jobs={payload.get('total_jobs')} "
            f"status_counts={payload.get('status_counts', {})} "
            f"tier_result_counts={payload.get('tier_result_counts', {})} "
            f"cache_summary={payload.get('cache_summary', {})}"
        )
PY

echo "Done."
echo "mode=$MODE"
echo "db=$DB_PATH"
echo "csv=$CSV_OUT"
echo "json=$JSON_OUT"
echo "run_log=$RUN_LOG"
echo "validator_log=$VALIDATOR_LOG"
