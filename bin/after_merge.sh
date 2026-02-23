git -C "$REPO" worktree remove "$WTROOT/$TASK"
git -C "$REPO" branch -d "codex/$TASK"
