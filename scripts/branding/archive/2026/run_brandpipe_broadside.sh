#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

PROFILE="balanced"
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: run_brandpipe_broadside.sh [--profile balanced|short|expressive|all] [--dry-run]

Runs the brandpipe broadside configs through the brandpipe CLI using direnv.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
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
      echo "unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

case "$PROFILE" in
  balanced)
    CONFIGS=("resources/brandpipe/openrouter_broadside_balanced.toml")
    ;;
  short)
    CONFIGS=("resources/brandpipe/openrouter_broadside_short.toml")
    ;;
  expressive)
    CONFIGS=("resources/brandpipe/openrouter_broadside_expressive.toml")
    ;;
  all)
    CONFIGS=(
      "resources/brandpipe/openrouter_broadside_short.toml"
      "resources/brandpipe/openrouter_broadside_balanced.toml"
      "resources/brandpipe/openrouter_broadside_expressive.toml"
    )
    ;;
  *)
    echo "invalid profile: $PROFILE" >&2
    usage >&2
    exit 1
    ;;
esac

for rel_config in "${CONFIGS[@]}"; do
  config_path="$ROOT_DIR/$rel_config"
  cmd=(direnv exec "$ROOT_DIR" env PYTHONPATH="$ROOT_DIR/src" python3 -m brandpipe.cli run --config "$config_path")
  echo "\$ ${cmd[*]}"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    "${cmd[@]}"
  fi
done
