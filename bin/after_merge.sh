#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $(basename "$0") <task-slug> [--remote-clean]"
  echo "Example: $(basename "$0") openrouter-diversity --remote-clean"
}

die() { echo "Error: $*" >&2; exit 1; }

TASK="${1:-}"
REMOTE_CLEAN="${2:-}"

[[ -n "$TASK" ]] || { usage; exit 1; }
[[ "$TASK" =~ ^[a-z0-9][a-z0-9._-]*$ ]] || die "Invalid task slug '$TASK'."
[[ -z "$REMOTE_CLEAN" || "$REMOTE_CLEAN" == "--remote-clean" ]] || { usage; exit 1; }

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"
WTROOT="${WTROOT:-$HOME/Development/brandname-generator-worktrees}"

BRANCH="codex/$TASK"
EXPECTED_WTPATH="$WTROOT/$TASK"

git -C "$REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "REPO is not a git repo: $REPO"
git -C "$REPO" fetch origin main --prune

# Find actual worktree path for this branch (if any).
WTPATH="$(
  git -C "$REPO" worktree list --porcelain | awk -v want="refs/heads/$BRANCH" '
    $1=="worktree" { wt=$2 }
    $1=="branch" && $2==want { print wt; exit }
  '
)"

if [[ -n "$WTPATH" ]]; then
  case "$PWD/" in
    "$WTPATH/"* ) die "You are inside the task worktree ($WTPATH). cd elsewhere first." ;;
  esac
fi

if git -C "$REPO" show-ref --verify --quiet "refs/heads/$BRANCH"; then
  git -C "$REPO" merge-base --is-ancestor "$BRANCH" "origin/main" \
    || die "Branch $BRANCH is not merged into origin/main."
fi

if [[ -n "$WTPATH" ]]; then
  git -C "$REPO" worktree remove "$WTPATH"
elif [[ -d "$EXPECTED_WTPATH" ]]; then
  # Fallback if branch metadata was cleaned up but folder remains.
  git -C "$REPO" worktree remove "$EXPECTED_WTPATH" || true
fi

if git -C "$REPO" show-ref --verify --quiet "refs/heads/$BRANCH"; then
  git -C "$REPO" branch -d "$BRANCH"
fi

if [[ "$REMOTE_CLEAN" == "--remote-clean" ]]; then
  git -C "$REPO" push origin --delete "$BRANCH" || true
fi

git -C "$REPO" pull --ff-only origin main

echo "Cleanup complete for $BRANCH"

