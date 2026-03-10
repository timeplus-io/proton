---
name: create-worktree
description: Create a Proton git worktree with local submodule reuse. Use when the user wants an isolated branch/worktree for feature work, bug fixing, or review.
---

# Create Worktree

## Arguments

- `$0` (required): branch name
- `$1` (optional): worktree path; default `.worktrees/<safe-branch>`

## Goal

Create a worktree without re-downloading the repo's submodule data. This repo has a large `contrib/` tree, so submodule reuse matters for both speed and disk usage.

## Procedure

### 1. Resolve paths

```bash
BRANCH=$0
MAIN_REPO=$(git rev-parse --show-toplevel)
SAFE_BRANCH=$(printf '%s' "$BRANCH" | tr '/' '-')
WORKTREE_PATH=${1:-"$MAIN_REPO/.worktrees/$SAFE_BRANCH"}
GIT_DIR=$(git -C "$MAIN_REPO" rev-parse --git-common-dir)
```

Stop if `WORKTREE_PATH` already exists.

### 2. Create the worktree

If the branch exists locally:

```bash
git -C "$MAIN_REPO" worktree add "$WORKTREE_PATH" "$BRANCH"
```

If the branch exists only on `origin`:

```bash
git -C "$MAIN_REPO" worktree add "$WORKTREE_PATH" -b "$BRANCH" "origin/$BRANCH"
```

Otherwise:

```bash
git -C "$MAIN_REPO" worktree add -b "$BRANCH" "$WORKTREE_PATH" origin/develop
```

### 3. Reuse submodule git data locally

Hardlink-copy the shared modules directory into the worktree metadata:

```bash
WORKTREE_GIT_DIR=$(git -C "$WORKTREE_PATH" rev-parse --git-dir)
WORKTREE_ADMIN_DIR=$(cd "$WORKTREE_GIT_DIR" && pwd)
cp -al "$GIT_DIR/modules" "$WORKTREE_ADMIN_DIR/modules"
```

Fix any copied `core.worktree` pointers so they reference the new worktree:

```bash
find "$WORKTREE_ADMIN_DIR/modules" -name config -exec \
    sed -i "s|worktree = .*/contrib/|worktree = $WORKTREE_PATH/contrib/|" {} +

find "$WORKTREE_ADMIN_DIR/modules" -name config.worktree -exec \
    sed -i "s|worktree = .*/contrib/|worktree = $WORKTREE_PATH/contrib/|" {} +
```

### 4. Materialize submodule working trees

```bash
git -C "$WORKTREE_PATH" submodule update || git -C "$WORKTREE_PATH" submodule init
git -C "$WORKTREE_PATH" submodule update
git -C "$WORKTREE_PATH" submodule foreach \
    '(git read-tree HEAD && git checkout -- .) 2>/dev/null || echo "SKIP: $name"'
```

### 5. Report back

Report:

- source repo
- branch
- worktree path
- whether submodule reuse succeeded

## Notes

- This is local-only and should not need network access if the main repo already has submodules initialized.
- Build artifacts are not shared; configure or build inside the worktree separately.
- Prefer this skill from [issue-workflow](../issue-workflow/SKILL.md) instead of inlining raw `git worktree add`.
