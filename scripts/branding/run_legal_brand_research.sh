#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

PACK_DIR=""
INPUT_CSV=""
OUT_PREFIX=""
COUNTRIES="de,ch,it"
REGISTRY_TOP_N=8
WEB_TOP_N=8
PRINT_TOP=12
EUIPO_PROBE=1
EUIPO_TIMEOUT_MS=20000
EUIPO_SETTLE_MS=2500
EUIPO_HEADFUL=0
SWISSREG_UI_PROBE=1
SWISSREG_TIMEOUT_MS=20000
SWISSREG_SETTLE_MS=2500
SWISSREG_HEADFUL=0

usage() {
  cat <<'EOF'
Usage:
  zsh scripts/branding/run_legal_brand_research.sh \
    --pack-dir <decision_pack_dir> \
    [--input-csv <final_survivors_csv>] \
    [--out-prefix <prefix>] \
    [--countries de,ch,it] \
    [--registry-top-n 8] \
    [--web-top-n 8] \
    [--print-top 12] \
    [--no-euipo-probe] \
    [--euipo-timeout-ms 20000] \
    [--euipo-settle-ms 2500] \
    [--euipo-headful] \
    [--no-swissreg-ui-probe] \
    [--swissreg-timeout-ms 20000] \
    [--swissreg-settle-ms 2500] \
    [--swissreg-headful]

Defaults:
  --input-csv  <pack-dir>/acceptance_keep_only/final_survivors_8.csv
  --out-prefix <pack-dir>/acceptance_keep_only/legal_brand_research_final8

Outputs:
  <out-prefix>.csv
  <out-prefix>.json
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pack-dir)
      PACK_DIR="$2"; shift 2 ;;
    --input-csv)
      INPUT_CSV="$2"; shift 2 ;;
    --out-prefix)
      OUT_PREFIX="$2"; shift 2 ;;
    --countries)
      COUNTRIES="$2"; shift 2 ;;
    --registry-top-n)
      REGISTRY_TOP_N="$2"; shift 2 ;;
    --web-top-n)
      WEB_TOP_N="$2"; shift 2 ;;
    --print-top)
      PRINT_TOP="$2"; shift 2 ;;
    --no-euipo-probe)
      EUIPO_PROBE=0; shift 1 ;;
    --euipo-timeout-ms)
      EUIPO_TIMEOUT_MS="$2"; shift 2 ;;
    --euipo-settle-ms)
      EUIPO_SETTLE_MS="$2"; shift 2 ;;
    --euipo-headful)
      EUIPO_HEADFUL=1; shift 1 ;;
    --no-swissreg-ui-probe)
      SWISSREG_UI_PROBE=0; shift 1 ;;
    --swissreg-timeout-ms)
      SWISSREG_TIMEOUT_MS="$2"; shift 2 ;;
    --swissreg-settle-ms)
      SWISSREG_SETTLE_MS="$2"; shift 2 ;;
    --swissreg-headful)
      SWISSREG_HEADFUL=1; shift 1 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2 ;;
  esac
done

if [[ -z "$PACK_DIR" ]]; then
  echo "Missing required --pack-dir" >&2
  usage
  exit 2
fi

if [[ -z "$INPUT_CSV" ]]; then
  INPUT_CSV="$PACK_DIR/acceptance_keep_only/final_survivors_8.csv"
fi
if [[ -z "$OUT_PREFIX" ]]; then
  OUT_PREFIX="$PACK_DIR/acceptance_keep_only/legal_brand_research_final8"
fi

if [[ ! -f "$INPUT_CSV" ]]; then
  echo "Input CSV not found: $INPUT_CSV" >&2
  exit 1
fi

NAMES_FILE="${OUT_PREFIX}.names.txt"
mkdir -p "$(dirname "$OUT_PREFIX")"

awk -F, 'NR>1 {print $2}' "$INPUT_CSV" > "$NAMES_FILE"

cmd=(
  python3 "$ROOT_DIR/scripts/branding/legal_brand_research.py"
  --names-file "$NAMES_FILE"
  --countries "$COUNTRIES"
  --registry-top-n "$REGISTRY_TOP_N"
  --web-top-n "$WEB_TOP_N"
  --output-csv "${OUT_PREFIX}.csv"
  --output-json "${OUT_PREFIX}.json"
  --print-top "$PRINT_TOP"
  --euipo-timeout-ms "$EUIPO_TIMEOUT_MS"
  --euipo-settle-ms "$EUIPO_SETTLE_MS"
  --swissreg-timeout-ms "$SWISSREG_TIMEOUT_MS"
  --swissreg-settle-ms "$SWISSREG_SETTLE_MS"
)

if [[ "$EUIPO_PROBE" -eq 1 ]]; then
  cmd+=(--euipo-probe)
else
  cmd+=(--no-euipo-probe)
fi
if [[ "$EUIPO_HEADFUL" -eq 1 ]]; then
  cmd+=(--euipo-headful)
fi
if [[ "$SWISSREG_UI_PROBE" -eq 1 ]]; then
  cmd+=(--swissreg-ui-probe)
else
  cmd+=(--no-swissreg-ui-probe)
fi
if [[ "$SWISSREG_HEADFUL" -eq 1 ]]; then
  cmd+=(--swissreg-headful)
fi

"${cmd[@]}"

echo "legal_brand_research_done csv=${OUT_PREFIX}.csv json=${OUT_PREFIX}.json names=$NAMES_FILE"
