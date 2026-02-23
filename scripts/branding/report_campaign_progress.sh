#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${REPORT_OUT_DIR:-$ROOT_DIR/test_outputs/branding/continuous_hybrid}"
DB_PATH="${REPORT_DB_PATH:-$OUT_DIR/naming_campaign.db}"
TOP_N="${REPORT_TOP_N:-20}"

usage() {
  cat <<'EOF'
Show key quality/progress metrics from a campaign database.

Usage:
  scripts/branding/report_campaign_progress.sh [options]

Options:
  --out-dir <path>   Campaign out dir containing naming_campaign.db
  --db <path>        Explicit DB path (overrides --out-dir)
  --top-n <n>        Number of top strong names to print (default: 20)
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      DB_PATH="$OUT_DIR/naming_campaign.db"
      shift 2
      ;;
    --db)
      DB_PATH="$2"
      shift 2
      ;;
    --top-n)
      TOP_N="$2"
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

if [[ ! -f "$DB_PATH" ]]; then
  echo "DB not found: $DB_PATH" >&2
  exit 1
fi
if [[ "${TOP_N:-}" != <-> ]]; then
  echo "--top-n must be a non-negative integer" >&2
  exit 2
fi

scalar() {
  sqlite3 -cmd ".timeout 3000" -noheader "$DB_PATH" "$1" 2>/dev/null | tr -d '[:space:]'
}

runs="$(scalar "SELECT IFNULL(MAX(id),0) FROM naming_runs;")"
candidates_total="$(scalar "SELECT COUNT(*) FROM candidates;")"
checked_strong="$(scalar "SELECT COUNT(*) FROM candidates WHERE state='checked' AND current_recommendation='strong';")"
checked_consider="$(scalar "SELECT COUNT(*) FROM candidates WHERE state='checked' AND current_recommendation='consider';")"
checked_good=$(( ${checked_strong:-0} + ${checked_consider:-0} ))
shortlist_good="$(scalar "SELECT COUNT(DISTINCT c.id) FROM candidates c JOIN shortlist_decisions s ON s.candidate_id=c.id WHERE s.selected=1 AND c.state='checked' AND c.current_recommendation IN ('strong','consider');")"
shortlist_unique="$(scalar "SELECT COUNT(DISTINCT c.name_normalized) FROM candidates c JOIN shortlist_decisions s ON s.candidate_id=c.id WHERE s.selected=1;")"

echo "campaign_progress db=$DB_PATH runs=$runs candidates_total=$candidates_total checked_good=$checked_good checked_strong=$checked_strong checked_consider=$checked_consider shortlist_good=$shortlist_good shortlist_unique=$shortlist_unique"
echo
echo "Checked recommendation mix"
sqlite3 -cmd ".timeout 3000" -header -column "$DB_PATH" "
SELECT current_recommendation, COUNT(*) AS n
FROM candidates
WHERE state='checked'
GROUP BY current_recommendation
ORDER BY n DESC, current_recommendation ASC;
"

if (( TOP_N > 0 )); then
  echo
  echo "Top checked strong names (up to $TOP_N)"
  sqlite3 -cmd ".timeout 3000" -header -column "$DB_PATH" "
SELECT c.name_display,
       c.current_score AS score,
       c.current_risk  AS risk,
       c.state,
       c.current_recommendation
FROM candidates c
WHERE c.state='checked' AND c.current_recommendation='strong'
ORDER BY c.current_score DESC, c.current_risk ASC, c.id DESC
LIMIT $TOP_N;
"
fi
