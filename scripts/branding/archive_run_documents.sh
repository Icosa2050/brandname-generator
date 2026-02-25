#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="$ROOT_DIR/test_outputs/branding/continuous_hybrid"
ARCHIVE_ROOT="$ROOT_DIR/artifacts/branding/run_archives"
LABEL="manual"
PRUNE=0
DRY_RUN=0

usage() {
  cat <<'USAGE'
Archive non-review campaign run documents into a timestamped .tar.gz.

Usage:
  scripts/branding/archive_run_documents.sh [options]

Options:
  --out-dir <path>        Campaign output directory to archive from
  --archive-root <path>   Destination root for archives
  --label <name>          Label in archive filename (default: manual)
  --prune                 Remove archived files from out-dir after successful archive
  --dry-run               Print what would be archived without writing/removing files
  -h, --help              Show help

Archived by default (if present):
  continuous/logs/
  continuous/supervisor_heartbeat.log
  runs/campaign_heartbeat.jsonl
  runs/*.log

Review-critical files are intentionally not included:
  naming_campaign.db
  campaign_summary.json
  campaign_progress.csv
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --archive-root)
      ARCHIVE_ROOT="$2"
      shift 2
      ;;
    --label)
      LABEL="$2"
      shift 2
      ;;
    --prune)
      PRUNE=1
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
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ ! -d "$OUT_DIR" ]]; then
  echo "Output directory does not exist: $OUT_DIR" >&2
  exit 1
fi

files_to_archive=()

if [[ -d "$OUT_DIR/continuous/logs" ]]; then
  files_to_archive+=("$OUT_DIR/continuous/logs")
fi
if [[ -f "$OUT_DIR/continuous/supervisor_heartbeat.log" ]]; then
  files_to_archive+=("$OUT_DIR/continuous/supervisor_heartbeat.log")
fi
if [[ -f "$OUT_DIR/runs/campaign_heartbeat.jsonl" ]]; then
  files_to_archive+=("$OUT_DIR/runs/campaign_heartbeat.jsonl")
fi
for run_log in "$OUT_DIR"/runs/*.log(N); do
  files_to_archive+=("$run_log")
done

if (( ${#files_to_archive[@]} == 0 )); then
  echo "No archive-candidate run documents found in: $OUT_DIR"
  exit 0
fi

ts="$(date -u +%Y%m%dT%H%M%SZ)"
out_base="$(basename "$OUT_DIR")"
archive_dir="$ARCHIVE_ROOT/$out_base"
archive_name="${out_base}_run_docs_${LABEL}_${ts}.tar.gz"
archive_path="$archive_dir/$archive_name"
sha_path="$archive_path.sha256"

relative_paths=()
for abs_path in "${files_to_archive[@]}"; do
  if [[ "$abs_path" == "$OUT_DIR"/* ]]; then
    relative_paths+=("${abs_path#$OUT_DIR/}")
  fi
done

echo "Archive source: $OUT_DIR"
echo "Archive destination: $archive_path"
echo "Items:"
for rel in "${relative_paths[@]}"; do
  echo "  - $rel"
done

if (( DRY_RUN )); then
  echo "Dry-run only. No archive written."
  exit 0
fi

mkdir -p "$archive_dir"
tar -czf "$archive_path" -C "$OUT_DIR" "${relative_paths[@]}"
shasum -a 256 "$archive_path" > "$sha_path"

echo "Created archive: $archive_path"
echo "Created checksum: $sha_path"

if (( PRUNE )); then
  echo "Pruning archived source files..."
  for abs_path in "${files_to_archive[@]}"; do
    if [[ -d "$abs_path" ]]; then
      rm -rf "$abs_path"
    else
      rm -f "$abs_path"
    fi
  done

  # Remove empty run subdirs if possible.
  rmdir "$OUT_DIR/continuous" 2>/dev/null || true
  rmdir "$OUT_DIR/runs" 2>/dev/null || true
  echo "Prune completed."
else
  echo "Source files retained (use --prune to remove them after archiving)."
fi
