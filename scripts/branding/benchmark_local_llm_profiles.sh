#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

LMSTUDIO_BASE_URL="${LMSTUDIO_BASE_URL:-http://127.0.0.1:1234/v1}"
OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://127.0.0.1:11434}"

LMSTUDIO_FAST_MODEL="${LMSTUDIO_FAST_MODEL:-llama-3.3-8b-instruct-omniwriter}"
LMSTUDIO_QUALITY_MODEL="${LMSTUDIO_QUALITY_MODEL:-qwen3-vl-30b-a3b-instruct-mlx}"
OLLAMA_MODEL="${OLLAMA_MODEL:-mistral-small3.1:latest}"

LMSTUDIO_TTL_S="${LMSTUDIO_TTL_S:-3600}"
OLLAMA_KEEP_ALIVE="${OLLAMA_KEEP_ALIVE:-30m}"

RUNS="${LOCAL_LLM_BENCH_RUNS:-5}"
GAP_S="${LOCAL_LLM_BENCH_GAP_S:-1}"
EVICTION_GAP_S="${LOCAL_LLM_BENCH_EVICTION_GAP_S:-70}"
TIMEOUT_S="${LOCAL_LLM_BENCH_TIMEOUT_S:-60}"
OUT_DIR="${LOCAL_LLM_BENCH_OUT_DIR:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --runs)
      RUNS="$2"
      shift 2
      ;;
    --gap-s)
      GAP_S="$2"
      shift 2
      ;;
    --eviction-gap-s)
      EVICTION_GAP_S="$2"
      shift 2
      ;;
    --timeout-s)
      TIMEOUT_S="$2"
      shift 2
      ;;
    --lmstudio-base-url)
      LMSTUDIO_BASE_URL="$2"
      shift 2
      ;;
    --ollama-base-url)
      OLLAMA_BASE_URL="$2"
      shift 2
      ;;
    --lmstudio-fast-model)
      LMSTUDIO_FAST_MODEL="$2"
      shift 2
      ;;
    --lmstudio-quality-model)
      LMSTUDIO_QUALITY_MODEL="$2"
      shift 2
      ;;
    --ollama-model)
      OLLAMA_MODEL="$2"
      shift 2
      ;;
    --lmstudio-ttl-s)
      LMSTUDIO_TTL_S="$2"
      shift 2
      ;;
    --ollama-keep-alive)
      OLLAMA_KEEP_ALIVE="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: scripts/branding/benchmark_local_llm_profiles.sh [options]

Runs standardized warm-cache probes for three local profiles:
1) LM Studio fast lane (default llama-3.3-8b-instruct-omniwriter)
2) Ollama balanced lane (default mistral-small3.1:latest)
3) LM Studio quality lane (default qwen3-vl-30b-a3b-instruct-mlx)

Options:
  --out-dir <path>                Output directory (default: /tmp/branding_local_llm_bench_<timestamp>)
  --runs <n>                      Probe main runs per profile (default: 5)
  --gap-s <seconds>               Gap between probe calls (default: 1)
  --eviction-gap-s <seconds>      Idle gap before post-idle probe (default: 70)
  --timeout-s <seconds>           Per-call timeout (default: 60)
  --lmstudio-base-url <url>       LM Studio base URL (default: http://127.0.0.1:1234/v1)
  --ollama-base-url <url>         Ollama native base URL (default: http://127.0.0.1:11434)
  --lmstudio-fast-model <id>      LM Studio fast model
  --lmstudio-quality-model <id>   LM Studio quality model
  --ollama-model <id>             Ollama model
  --lmstudio-ttl-s <seconds>      TTL hint for LM Studio probes (default: 3600)
  --ollama-keep-alive <value>     keep_alive for Ollama probes (default: 30m)
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="/tmp/branding_local_llm_bench_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$OUT_DIR"

RESULTS_TSV="$OUT_DIR/runs.tsv"
SUMMARY_JSON="$OUT_DIR/benchmark_summary.json"
SUMMARY_CSV="$OUT_DIR/benchmark_summary.csv"
echo -e "profile\tprovider\tmodel\tjson_path\tlog_path\texit_code" > "$RESULTS_TSV"

echo "[1/4] Running endpoint/model preflight checks..."
if ! zsh "$ROOT_DIR/scripts/branding/preflight_llm.sh" \
  --check-lmstudio \
  --lmstudio-base-url="$LMSTUDIO_BASE_URL" \
  --lmstudio-model="$LMSTUDIO_FAST_MODEL,$LMSTUDIO_QUALITY_MODEL" \
  --check-ollama \
  --ollama-base-url="$OLLAMA_BASE_URL" \
  --ollama-model="$OLLAMA_MODEL"; then
  echo "LOCAL_LLM_BENCH FAIL stage=preflight lmstudio_base_url=$LMSTUDIO_BASE_URL ollama_base_url=$OLLAMA_BASE_URL"
  exit 2
fi

run_probe() {
  local profile="$1"
  local provider="$2"
  local model="$3"
  local base_url="$4"
  local json_path="$OUT_DIR/${profile}.json"
  local log_path="$OUT_DIR/${profile}.log"

  local cmd=(
    python3
    "$ROOT_DIR/scripts/branding/test_local_llm_warm_cache.py"
    --provider="$provider"
    --base-url="$base_url"
    --model="$model"
    --runs="$RUNS"
    --gap-s="$GAP_S"
    --eviction-gap-s="$EVICTION_GAP_S"
    --timeout-s="$TIMEOUT_S"
    --output-json="$json_path"
  )
  if [[ "$provider" == "openai_compat" ]]; then
    cmd+=(--ttl-s="$LMSTUDIO_TTL_S")
  else
    cmd+=(--keep-alive="$OLLAMA_KEEP_ALIVE")
  fi

  echo "benchmark_probe_start profile=$profile provider=$provider model=$model base_url=$base_url"
  set +e
  "${cmd[@]}" > "$log_path" 2>&1
  local rc=$?
  set -e
  if [[ "$rc" -eq 0 ]]; then
    echo "benchmark_probe_done profile=$profile status=ok log=$log_path json=$json_path"
  else
    echo "benchmark_probe_done profile=$profile status=fail exit_code=$rc log=$log_path json=$json_path"
    tail -n 30 "$log_path" || true
  fi
  echo -e "${profile}\t${provider}\t${model}\t${json_path}\t${log_path}\t${rc}" >> "$RESULTS_TSV"
}

echo "local_llm_benchmark_config out_dir=$OUT_DIR runs=$RUNS gap_s=$GAP_S eviction_gap_s=$EVICTION_GAP_S timeout_s=$TIMEOUT_S lmstudio_ttl_s=$LMSTUDIO_TTL_S ollama_keep_alive=$OLLAMA_KEEP_ALIVE"

echo "[2/4] Running lmstudio_fast probe..."
run_probe "lmstudio_fast" "openai_compat" "$LMSTUDIO_FAST_MODEL" "$LMSTUDIO_BASE_URL"
echo "[3/4] Running ollama_balanced probe..."
run_probe "ollama_balanced" "ollama_native" "$OLLAMA_MODEL" "$OLLAMA_BASE_URL"
echo "[4/4] Running lmstudio_quality probe..."
run_probe "lmstudio_quality" "openai_compat" "$LMSTUDIO_QUALITY_MODEL" "$LMSTUDIO_BASE_URL"

python3 - "$RESULTS_TSV" "$SUMMARY_JSON" "$SUMMARY_CSV" <<'PY'
import csv
import json
import sys
from pathlib import Path

runs_tsv = Path(sys.argv[1])
summary_json = Path(sys.argv[2])
summary_csv = Path(sys.argv[3])

rows = []
with runs_tsv.open('r', encoding='utf-8') as handle:
    reader = csv.DictReader(handle, delimiter='\t')
    for row in reader:
        entry = {
            'profile': row['profile'],
            'provider': row['provider'],
            'model': row['model'],
            'json_path': row['json_path'],
            'log_path': row['log_path'],
            'exit_code': int(row['exit_code']),
            'status': 'fail',
            'cold_ms': None,
            'warm_median_ms': None,
            'post_idle_ms': None,
            'main_ok': 0,
            'main_total': 0,
        }
        json_path = Path(row['json_path'])
        if entry['exit_code'] == 0 and json_path.exists():
            try:
                payload = json.loads(json_path.read_text(encoding='utf-8'))
                summary = payload.get('summary', {})
                entry['status'] = 'ok'
                entry['cold_ms'] = summary.get('cold_elapsed_ms')
                entry['warm_median_ms'] = summary.get('warm_median_ms')
                entry['post_idle_ms'] = summary.get('post_idle_elapsed_ms')
                entry['main_ok'] = int(summary.get('ok_main_runs') or 0)
                entry['main_total'] = int(summary.get('total_main_runs') or 0)
            except Exception:
                entry['status'] = 'fail'
        rows.append(entry)

summary_json.write_text(json.dumps({'profiles': rows}, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

with summary_csv.open('w', newline='', encoding='utf-8') as handle:
    writer = csv.writer(handle)
    writer.writerow(['profile', 'provider', 'model', 'status', 'cold_ms', 'warm_median_ms', 'post_idle_ms', 'main_ok', 'main_total', 'json_path', 'log_path'])
    for row in rows:
        writer.writerow([
            row['profile'],
            row['provider'],
            row['model'],
            row['status'],
            row['cold_ms'],
            row['warm_median_ms'],
            row['post_idle_ms'],
            row['main_ok'],
            row['main_total'],
            row['json_path'],
            row['log_path'],
        ])

print('\nLocal LLM Benchmark')
print('profile            provider       model                                   status  cold_ms  warm_ms  post_idle_ms  main_ok/total')
for row in rows:
    cold = '-' if row['cold_ms'] is None else str(row['cold_ms'])
    warm = '-' if row['warm_median_ms'] is None else str(row['warm_median_ms'])
    post_idle = '-' if row['post_idle_ms'] is None else str(row['post_idle_ms'])
    print(
        f"{row['profile']:<18} "
        f"{row['provider']:<13} "
        f"{row['model']:<39} "
        f"{row['status']:<6} "
        f"{cold:<8} "
        f"{warm:<8} "
        f"{post_idle:<13} "
        f"{row['main_ok']}/{row['main_total']}"
    )

failed = [row for row in rows if row['status'] != 'ok']
print(f"\nbenchmark_summary_json={summary_json}")
print(f"benchmark_summary_csv={summary_csv}")
if failed:
    print(f"benchmark_status=fail failed_profiles={[row['profile'] for row in failed]}")
    raise SystemExit(1)
print("benchmark_status=ok")
PY
