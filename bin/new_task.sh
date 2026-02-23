#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $(basename "$0") <task-slug> [base-branch]"
  echo "Example: $(basename "$0") openrouter-diversity main"
}

die() { echo "Error: $*" >&2; exit 1; }

TASK="${1:-}"
BASE="${2:-main}"

[[ -n "$TASK" ]] || { usage; exit 1; }
[[ "$TASK" =~ ^[a-z0-9][a-z0-9._-]*$ ]] || die "Invalid task slug '$TASK' (allowed: a-z 0-9 . _ -)."

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"
WTROOT="${WTROOT:-$HOME/Development/brandname-generator-worktrees}"

BRANCH="codex/$TASK"
WTPATH="$WTROOT/$TASK"

git -C "$REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "REPO is not a git repo: $REPO"
mkdir -p "$WTROOT"

[[ ! -e "$WTPATH" ]] || die "Worktree path already exists: $WTPATH"

if git -C "$REPO" show-ref --verify --quiet "refs/heads/$BRANCH"; then
  die "Local branch already exists: $BRANCH"
fi
if git -C "$REPO" ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
  die "Remote branch already exists: $BRANCH"
fi

git -C "$REPO" fetch origin "$BASE" --prune
git -C "$REPO" show-ref --verify --quiet "refs/remotes/origin/$BASE" || die "Missing origin/$BASE"

git -C "$REPO" worktree add -b "$BRANCH" "$WTPATH" "origin/$BASE"

echo "Created:"
echo "  branch:   $BRANCH"
echo "  worktree: $WTPATH"
echo "Next:"
echo "  cd \"$WTPATH\""

