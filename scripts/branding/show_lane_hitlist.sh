#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
ARTIFACT_ROOT="${BRANDING_AUTOMATION_DATA_ROOT:-$ROOT_DIR/test_outputs/branding/automation-data}"
STATE_DIR="$ARTIFACT_ROOT/state"
RUNS_DIR="$ARTIFACT_ROOT/runs"
TOP_N="${HITLIST_TOP_N:-50}"
LANE="all"

usage() {
  cat <<'USAGE'
Show latest Top-N hitlists from automation lane artifacts.

Usage:
  scripts/branding/show_lane_hitlist.sh [options]

Options:
  --lane <all|generation|fusion|validation|smoke>
  --top-n <n>                  Number of names to print per list (default: 50)
  --artifact-root <path>       Override automation artifact root
  -h, --help

Notes:
- generation prints two hitlists: quality + remote_quality
- fusion prints fused hitlist
- validation prints fused hitlist from latest validation manifest context
- smoke has no names/hitlist output
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lane)
      LANE="${2:-}"
      shift 2
      ;;
    --top-n)
      TOP_N="${2:-}"
      shift 2
      ;;
    --artifact-root)
      ARTIFACT_ROOT="${2:-}"
      STATE_DIR="$ARTIFACT_ROOT/state"
      RUNS_DIR="$ARTIFACT_ROOT/runs"
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

if [[ -z "$TOP_N" || ! "$TOP_N" =~ '^[1-9][0-9]*$' ]]; then
  echo "--top-n must be a positive integer" >&2
  exit 2
fi

if [[ "$LANE" != "all" && "$LANE" != "generation" && "$LANE" != "fusion" && "$LANE" != "validation" && "$LANE" != "smoke" ]]; then
  echo "--lane must be one of: all, generation, fusion, validation, smoke" >&2
  exit 2
fi

json_get() {
  local json_path="$1"
  local key_path="$2"
  python3 - "$json_path" "$key_path" <<'PY'
import json
import sys

json_path = sys.argv[1]
key_path = sys.argv[2]
keys = [k for k in key_path.split('.') if k]

try:
    with open(json_path, 'r', encoding='utf-8') as fh:
        value = json.load(fh)
except (OSError, json.JSONDecodeError) as exc:
    print(f'Failed to read JSON from {json_path}: {exc}', file=sys.stderr)
    raise SystemExit(1)

for key in keys:
    if not isinstance(value, dict) or key not in value:
        raise SystemExit(3)
    value = value[key]

if value is None:
    raise SystemExit(4)

if isinstance(value, (dict, list)):
    print(json.dumps(value, ensure_ascii=True))
else:
    print(str(value))
PY
}

emit_csv_hitlist() {
  local title="$1"
  local csv_path="$2"
  local top_n="$3"

  if [[ ! -f "$csv_path" ]]; then
    echo "[$title] missing file: $csv_path"
    return 0
  fi

  echo
  echo "=== $title ==="
  echo "source: $csv_path"

  python3 - "$csv_path" "$top_n" <<'PY'
import csv
import sys

csv_path = sys.argv[1]
top_n = int(sys.argv[2])

with open(csv_path, 'r', encoding='utf-8', newline='') as fh:
    rows = list(csv.DictReader(fh))

if not rows:
    print('(no rows)')
    raise SystemExit(0)

rank_keys = ('rank', 'final_rank')
name_keys = ('name', 'name_display')
score_keys = ('fusion_score', 'total_score', 'raw_total_score', 'current_score', 'score')
rec_keys = ('recommendation', 'current_recommendation', 'source_recommendation')
extra_keys = ('source_profiles', 'reasons', 'quality_rank', 'remote_quality_rank')

def first_key(candidates):
    for key in candidates:
        if key in rows[0]:
            return key
    return None

rank_key = first_key(rank_keys)
name_key = first_key(name_keys)
score_key = first_key(score_keys)
rec_key = first_key(rec_keys)
extra_key = first_key(extra_keys)

if name_key is None:
    print('missing name column')
    raise SystemExit(0)

columns = []
if rank_key:
    columns.append(('rank', rank_key))
columns.append(('name', name_key))
if score_key:
    columns.append(('score', score_key))
if rec_key:
    columns.append(('rec', rec_key))
if extra_key:
    columns.append(('extra', extra_key))

subset = rows[:top_n]
headers = [h for h, _ in columns]
widths = {h: len(h) for h in headers}
for row in subset:
    for h, key in columns:
        widths[h] = max(widths[h], len(str(row.get(key, '') or '')))

line = ' | '.join(h.ljust(widths[h]) for h in headers)
sep = '-+-'.join('-' * widths[h] for h in headers)
print(line)
print(sep)
for row in subset:
    print(' | '.join(str(row.get(key, '') or '').ljust(widths[h]) for h, key in columns))
PY
}

latest_validation_manifest() {
  local latest_dir
  latest_dir="$(ls -1dt "$RUNS_DIR"/validation/* 2>/dev/null | head -n 1 || true)"
  if [[ -z "$latest_dir" ]]; then
    return 1
  fi
  echo "$latest_dir/manifest.json"
}

show_generation() {
  local pointer manifest quality_out remote_out
  pointer="$STATE_DIR/latest_generation.json"
  if [[ ! -f "$pointer" ]]; then
    echo "[generation] missing pointer: $pointer"
    return 0
  fi

  manifest="$(json_get "$pointer" manifest_path)" || {
    echo "[generation] invalid pointer: $pointer"
    return 0
  }

  quality_out="$(json_get "$manifest" details.quality_out_dir 2>/dev/null || true)"
  remote_out="$(json_get "$manifest" details.remote_quality_out_dir 2>/dev/null || true)"

  if [[ -n "$quality_out" ]]; then
    emit_csv_hitlist "generation/quality top $TOP_N" "$quality_out/postrank/deterministic_rubric_rank.csv" "$TOP_N"
  else
    echo "[generation] quality_out_dir missing in manifest: $manifest"
  fi

  if [[ -n "$remote_out" ]]; then
    emit_csv_hitlist "generation/remote_quality top $TOP_N" "$remote_out/postrank/deterministic_rubric_rank.csv" "$TOP_N"
  else
    echo "[generation] remote_quality_out_dir missing in manifest: $manifest"
  fi
}

show_fusion() {
  local pointer manifest fused_out
  pointer="$STATE_DIR/latest_fusion.json"
  if [[ ! -f "$pointer" ]]; then
    echo "[fusion] missing pointer: $pointer"
    return 0
  fi

  manifest="$(json_get "$pointer" manifest_path)" || {
    echo "[fusion] invalid pointer: $pointer"
    return 0
  }

  fused_out="$(json_get "$manifest" details.fused_out_dir 2>/dev/null || true)"
  if [[ -z "$fused_out" ]]; then
    echo "[fusion] fused_out_dir missing in manifest: $manifest"
    return 0
  fi

  emit_csv_hitlist "fusion top $TOP_N" "$fused_out/postrank/fused_quality_remote_rank.csv" "$TOP_N"
}

show_validation() {
  local manifest fused_out
  manifest="$(latest_validation_manifest || true)"
  if [[ -z "$manifest" || ! -f "$manifest" ]]; then
    echo "[validation] no validation manifest found under: $RUNS_DIR/validation"
    return 0
  fi

  fused_out="$(json_get "$manifest" details.fused_out_dir 2>/dev/null || true)"
  if [[ -z "$fused_out" ]]; then
    echo "[validation] fused_out_dir missing in manifest: $manifest"
    return 0
  fi

  emit_csv_hitlist "validation context (fused) top $TOP_N" "$fused_out/postrank/fused_quality_remote_rank.csv" "$TOP_N"
}

show_smoke() {
  echo "[smoke] no name hitlist output by design (smoke lane only checks infra/syntax/connectivity)."
}

echo "artifact_root=$ARTIFACT_ROOT"
echo "lane=$LANE top_n=$TOP_N"

case "$LANE" in
  all)
    show_generation
    show_fusion
    show_validation
    show_smoke
    ;;
  generation)
    show_generation
    ;;
  fusion)
    show_fusion
    ;;
  validation)
    show_validation
    ;;
  smoke)
    show_smoke
    ;;
esac
