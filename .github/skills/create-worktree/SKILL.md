---
name: create-worktree
description: Create a Proton git worktree with local submodule reuse. Use when the user wants an isolated branch/worktree for feature work, bug fixing, or review.
---

# Create Worktree

## Default policy

Use a dedicated worktree by default for any task that writes code, creates a branch/commit, or needs isolated build/test output.

Do not pay the worktree cost for pure read-only tasks such as grep, architecture tracing, or review-only requests unless the user explicitly asks for isolation.

## Arguments

- `$0` (required): branch name
- `$1` (optional): worktree path; default `.worktrees/<safe-branch>`

## Goal

Create a worktree without re-downloading the repo's submodule data. This repo has a large `contrib/` tree, so submodule reuse matters for both speed and disk usage.

This skill isolates three things together:
- source-tree changes
- submodule working trees
- build/test artifacts that must stay bound to one source root

## Procedure

### 1. Resolve paths

```bash
BRANCH=$0
if [ -z "$BRANCH" ]; then
    echo "Branch name is required"
    exit 1
fi

MAIN_REPO=$(git rev-parse --show-toplevel) || {
    echo "Not inside a git repository"
    exit 1
}
SAFE_BRANCH=$(printf '%s' "$BRANCH" | tr '/' '-')
WORKTREE_PATH=${1:-"$MAIN_REPO/.worktrees/$SAFE_BRANCH"}
GIT_DIR=$(git -C "$MAIN_REPO" rev-parse --git-common-dir)

# Ensure WORKTREE_PATH is absolute.
case "$WORKTREE_PATH" in
    /*) ;;
    *)  WORKTREE_PATH="$MAIN_REPO/$WORKTREE_PATH" ;;
esac

if [ -e "$WORKTREE_PATH" ]; then
    echo "Worktree path already exists: $WORKTREE_PATH"
    exit 1
fi
```

### 2. Create the worktree

```bash
if git -C "$MAIN_REPO" show-ref --verify --quiet "refs/heads/$BRANCH"; then
    git -C "$MAIN_REPO" worktree add "$WORKTREE_PATH" "$BRANCH"
elif git -C "$MAIN_REPO" show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
    git -C "$MAIN_REPO" worktree add -b "$BRANCH" "$WORKTREE_PATH" "origin/$BRANCH"
else
    git -C "$MAIN_REPO" worktree add -b "$BRANCH" "$WORKTREE_PATH" origin/develop
fi
```

`origin/develop` is the standard base branch for Proton. Adjust if the repo uses a different default (e.g. `main`, `master`).

### 3. Reuse submodule git data locally

Hardlink-copy the shared modules directory into the worktree metadata:

```bash
if [ ! -d "$GIT_DIR/modules" ]; then
    echo "Main repo submodules are not initialized: $GIT_DIR/modules not found"
    exit 1
fi

WORKTREE_GIT_DIR=$(git -C "$WORKTREE_PATH" rev-parse --git-dir)
WORKTREE_ADMIN_DIR=$(cd "$WORKTREE_GIT_DIR" && pwd)
if cp -al "$GIT_DIR/modules" "$WORKTREE_ADMIN_DIR/modules" 2>/dev/null; then
    REUSE_MODE="hardlink (GNU cp -al)"
elif cp -RlpP "$GIT_DIR/modules" "$WORKTREE_ADMIN_DIR/modules" 2>/dev/null; then
    REUSE_MODE="hardlink (BSD cp -RlpP)"
else
    cp -R "$GIT_DIR/modules" "$WORKTREE_ADMIN_DIR/modules"
    REUSE_MODE="full copy (cp -R)"
fi
```

The first two paths create hardlinks, which is safe because git objects are content-addressed and immutable. The `sed -i` step that follows breaks the hardlink for any config file it modifies, so the original repo's configs are unaffected. `cp -al` is GNU-specific (`-a` is not available on BSD `cp`); `cp -RlpP` is the preferred BSD/macOS hardlink-copy path. The `cp -R` fallback is a recursive copy (slower, doubles disk) reached only when hardlinks are unavailable (cross-filesystem, restricted volume).

Fix any copied `core.worktree` pointers so they reference the new worktree:

```bash
find "$WORKTREE_ADMIN_DIR/modules" -name config -exec \
    sed -i'' -e "s|worktree = .*/contrib/|worktree = $WORKTREE_PATH/contrib/|" {} +

find "$WORKTREE_ADMIN_DIR/modules" -name config.worktree -exec \
    sed -i'' -e "s|worktree = .*/contrib/|worktree = $WORKTREE_PATH/contrib/|" {} +
```

### 4. Materialize submodule working trees

```bash
git -C "$WORKTREE_PATH" submodule update || {
    git -C "$WORKTREE_PATH" submodule init
    git -C "$WORKTREE_PATH" submodule update
}
git -C "$WORKTREE_PATH" submodule foreach \
    '(git read-tree HEAD && git checkout -- .) 2>/dev/null || echo "SKIP: $name"'
```

### 5. Validate submodule state before editing or building

```bash
SUB_STATUS=$(git -C "$WORKTREE_PATH" submodule status --recursive)
echo "$SUB_STATUS"

if echo "$SUB_STATUS" | grep -Eq '^[-+U]'; then
    echo "Submodule state mismatch in $WORKTREE_PATH"
    exit 1
fi
```

- `-` means not initialized
- `+` means checked out at a different commit than the superproject expects
- `U` means merge conflict

### 6. Initialize build directories inside the same source tree you will validate

Any worktree-local build directory name is valid (`build`, `build_release`, `build_debug`, etc.). Preferred default for Proton: create `build/` inside the worktree itself, because the standard helper scripts and validation commands resolve binaries under `build/...`.

```bash
mkdir -p "$WORKTREE_PATH/build"
(cd "$WORKTREE_PATH/build" && ../build.sh Debug)
```

If you need additional configurations such as `build_release/` or `build_debug/`, create them inside the same worktree. When you use non-default build directories, update the downstream run/test commands to point at that directory instead of assuming `build/...`.

Do not reuse `build*` directories from the main checkout to validate worktree changes. CMake/Ninja build directories embed absolute source paths, so reusing or retargeting them will either build the wrong tree or overwrite the original checkout's cache/configuration.

### 7. Report back

Report:

- source repo
- branch
- worktree path
- submodule reuse mode (`$REUSE_MODE` — hardlink or full copy)
- whether submodule validation passed
- which build directory convention should be used next (`$WORKTREE_PATH/build` by default; custom worktree-local build dirs are also fine if later commands are adjusted consistently)

## Examples

- `create-worktree bugfix/issue-3446-session-watermark-regression`
- `create-worktree chore/worktree-skill-build-isolation ../proton-enterprise-worktree-skill-build-isolation`

## Notes

- This is local-only and should not need network access if the main repo already has submodules initialized.
- Build artifacts are not shared across source roots. Reconfigure/build inside the worktree you are validating.
- Prefer this skill from [issue-workflow](../issue-workflow/SKILL.md) instead of inlining raw `git worktree add`.
- To remove a clean worktree later:

```bash
git -C "$MAIN_REPO" worktree remove "$WORKTREE_PATH"
```

- If removal is blocked by uncommitted or untracked files, inspect or back up that work first. Only use forced cleanup when you intentionally want to discard it:

```bash
git -C "$MAIN_REPO" worktree remove --force "$WORKTREE_PATH"
```
