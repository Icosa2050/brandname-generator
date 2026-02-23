REPO=/Users/bernhard/Development/brandname-generator
WTROOT=/Users/bernhard/Development/brandname-generator-worktrees
TASK=<task-slug>

git -C "$REPO" pull --ff-only
git -C "$REPO" worktree add -b "codex/$TASK" "$WTROOT/$TASK" main

