#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
APPLY=0
FETCH=1
BASE_REF=""

usage() {
  cat <<'EOF'
Inspect and optionally clean local git worktree/branch hygiene.

Usage:
  scripts/branding/git_worktree_hygiene.sh [options]

Options:
  --apply             Apply safe cleanup actions (default: report-only)
  --no-fetch          Skip `git fetch origin --prune`
  --base-ref <ref>    Compare merge status against this ref (default: origin/main, fallback main)
  -h, --help

Cleanup actions when --apply is set:
  - remove clean detached worktrees
  - delete merged local codex/* branches not checked out in any worktree
  - run `git worktree prune`

Notes:
  - codex/rescue-* branches are never auto-deleted
  - dirty detached worktrees are reported but never removed
EOF
}

contains_item() {
  local needle="$1"
  shift || true
  local item
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    --no-fetch)
      FETCH=0
      shift
      ;;
    --base-ref)
      if [[ $# -lt 2 ]]; then
        echo "Error: --base-ref requires a value." >&2
        usage
        exit 2
      fi
      BASE_REF="$2"
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

cd "$ROOT_DIR"

if (( FETCH )); then
  if ! git fetch origin --prune >/dev/null 2>&1; then
    echo "warning: git fetch origin --prune failed; continuing with local refs" >&2
  fi
fi

if [[ -z "$BASE_REF" ]]; then
  if git show-ref --verify --quiet refs/remotes/origin/main; then
    BASE_REF="origin/main"
  else
    BASE_REF="main"
  fi
fi

if ! git rev-parse --verify "${BASE_REF}^{commit}" >/dev/null 2>&1; then
  echo "error: base ref not found: $BASE_REF" >&2
  exit 1
fi

typeset -a worktree_paths
typeset -a active_branches
typeset -a detached_clean_candidates
typeset -a detached_dirty_paths
typeset -a merged_branch_candidates

worktree_paths=("${(@f)$(git worktree list --porcelain | awk '/^worktree /{sub(/^worktree /,""); print}')}")

echo "base_ref=$BASE_REF"
echo "worktrees:"
for wt_path in "${worktree_paths[@]}"; do
  wt_branch="$(git -C "$wt_path" rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
  if [[ "$wt_branch" == "HEAD" ]]; then
    wt_branch="(detached)"
  else
    active_branches+=("$wt_branch")
  fi
  if [[ -n "$(git -C "$wt_path" status --porcelain --untracked-files=normal)" ]]; then
    wt_state="dirty"
  else
    wt_state="clean"
  fi

  echo "  - path=$wt_path branch=$wt_branch state=$wt_state"

  if [[ "$wt_branch" == "(detached)" ]]; then
    if [[ "$wt_state" == "clean" ]]; then
      detached_clean_candidates+=("$wt_path")
    else
      detached_dirty_paths+=("$wt_path")
    fi
  fi
done

for local_branch in ${(f)"$(git for-each-ref --format='%(refname:short)' refs/heads/codex)"}; do
  [[ -z "$local_branch" ]] && continue
  if [[ "$local_branch" == codex/rescue-* ]]; then
    continue
  fi
  if contains_item "$local_branch" "${active_branches[@]}"; then
    continue
  fi
  if git merge-base --is-ancestor "$local_branch" "$BASE_REF"; then
    merged_branch_candidates+=("$local_branch")
  fi
done

echo
echo "cleanup candidates:"
if (( ${#detached_clean_candidates[@]} > 0 )); then
  for wt_path in "${detached_clean_candidates[@]}"; do
    echo "  - clean detached worktree: $wt_path"
  done
else
  echo "  - clean detached worktree: none"
fi

if (( ${#detached_dirty_paths[@]} > 0 )); then
  for wt_path in "${detached_dirty_paths[@]}"; do
    echo "  - dirty detached worktree (manual action): $wt_path"
  done
else
  echo "  - dirty detached worktree: none"
fi

if (( ${#merged_branch_candidates[@]} > 0 )); then
  for local_branch in "${merged_branch_candidates[@]}"; do
    echo "  - merged local branch: $local_branch"
  done
else
  echo "  - merged local branch: none"
fi

if (( APPLY == 0 )); then
  echo
  echo "report-only mode. re-run with --apply to execute safe cleanup actions."
  exit 0
fi

echo
echo "applying cleanup..."
for wt_path in "${detached_clean_candidates[@]}"; do
  echo "  removing worktree: $wt_path"
  git worktree remove "$wt_path"
done

for local_branch in "${merged_branch_candidates[@]}"; do
  echo "  deleting branch: $local_branch"
  git branch -d "$local_branch"
done

echo "  pruning worktree metadata"
git worktree prune
echo "done"
