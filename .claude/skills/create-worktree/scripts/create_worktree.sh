#!/usr/bin/env bash
# create_worktree.sh — Create a Proton git worktree with local submodule reuse.
#
# Usage: create_worktree.sh <branch> [worktree-path]
#
# Creates a git worktree for the given branch, reuses submodule git data via
# hardlinks when possible (safe because git objects are content-addressed and
# immutable), materializes submodule working trees, and validates state.
# Build directory setup is left as a manual next step (printed at the end).
#
# The default worktree path is .worktrees/<safe-branch> under the repo root.
# origin/develop is used as the base branch if the branch doesn't exist locally
# or on the remote.

set -euo pipefail

# ── 1. Resolve paths ────────────────────────────────────────────────────────

BRANCH="${1:-}"
if [ -z "$BRANCH" ]; then
    echo "Usage: create_worktree.sh <branch> [worktree-path]"
    exit 1
fi

MAIN_REPO=$(git rev-parse --show-toplevel) || {
    echo "Error: not inside a git repository"
    exit 1
}
SAFE_BRANCH=$(printf '%s' "$BRANCH" | tr '/' '-')
WORKTREE_PATH="${2:-$MAIN_REPO/.worktrees/$SAFE_BRANCH}"
GIT_DIR=$(git -C "$MAIN_REPO" rev-parse --git-common-dir)

# Ensure WORKTREE_PATH is absolute.
case "$WORKTREE_PATH" in
    /*) ;;
    *)  WORKTREE_PATH="$MAIN_REPO/$WORKTREE_PATH" ;;
esac

if [ -e "$WORKTREE_PATH" ]; then
    echo "Error: worktree path already exists: $WORKTREE_PATH"
    exit 1
fi

echo "Branch:        $BRANCH"
echo "Worktree path: $WORKTREE_PATH"
echo "Main repo:     $MAIN_REPO"
echo ""

# ── 2. Create the worktree ──────────────────────────────────────────────────

if git -C "$MAIN_REPO" show-ref --verify --quiet "refs/heads/$BRANCH"; then
    echo "Using existing local branch: $BRANCH"
    git -C "$MAIN_REPO" worktree add "$WORKTREE_PATH" "$BRANCH"
elif git -C "$MAIN_REPO" show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
    echo "Tracking remote branch: origin/$BRANCH"
    git -C "$MAIN_REPO" worktree add -b "$BRANCH" "$WORKTREE_PATH" "origin/$BRANCH"
else
    echo "Creating new branch from origin/develop"
    git -C "$MAIN_REPO" worktree add -b "$BRANCH" "$WORKTREE_PATH" origin/develop
fi

# ── 3. Reuse submodule git data via hardlinks ────────────────────────────────

WORKTREE_GIT_DIR=$(git -C "$WORKTREE_PATH" rev-parse --git-dir)
WORKTREE_ADMIN_DIR=$(cd "$WORKTREE_GIT_DIR" && pwd)

if [ ! -d "$GIT_DIR/modules" ]; then
    echo "Warning: main repo submodules not initialized ($GIT_DIR/modules not found)"
    echo "Skipping submodule reuse — initializing submodules from scratch in worktree"
    REUSE_MODE="none (fresh init)"
else
    # Try hardlink copy (fast, space-efficient), fall back to full copy.
    # Hardlinks are safe: git objects are content-addressed and immutable.
    # The sed step below breaks hardlinks for config files it modifies.
    if cp -al "$GIT_DIR/modules" "$WORKTREE_ADMIN_DIR/modules" 2>/dev/null; then
        REUSE_MODE="hardlink (GNU cp -al)"
    elif cp -RlpP "$GIT_DIR/modules" "$WORKTREE_ADMIN_DIR/modules" 2>/dev/null; then
        REUSE_MODE="hardlink (BSD cp -RlpP)"
    else
        cp -R "$GIT_DIR/modules" "$WORKTREE_ADMIN_DIR/modules"
        REUSE_MODE="full copy (cp -R)"
    fi

    echo "Submodule reuse: $REUSE_MODE"

    # Fix core.worktree pointers to reference the new worktree path.
    find "$WORKTREE_ADMIN_DIR/modules" -name config -exec \
        sed -i'' -e "s|worktree = .*/contrib/|worktree = $WORKTREE_PATH/contrib/|" {} +

    find "$WORKTREE_ADMIN_DIR/modules" -name config.worktree -exec \
        sed -i'' -e "s|worktree = .*/contrib/|worktree = $WORKTREE_PATH/contrib/|" {} +
fi

# ── 4. Materialize submodule working trees ───────────────────────────────────

echo "Materializing submodule working trees..."
git -C "$WORKTREE_PATH" submodule update || {
    git -C "$WORKTREE_PATH" submodule init
    git -C "$WORKTREE_PATH" submodule update
}
git -C "$WORKTREE_PATH" submodule foreach \
    '(git read-tree HEAD && git checkout -- .) 2>/dev/null || echo "SKIP: $name"'

# ── 5. Validate submodule state ─────────────────────────────────────────────

echo ""
echo "Validating submodule state..."
SUB_STATUS=$(git -C "$WORKTREE_PATH" submodule status --recursive)

if echo "$SUB_STATUS" | grep -Eq '^[-+U]'; then
    echo "ERROR: Submodule state mismatch in $WORKTREE_PATH"
    echo "$SUB_STATUS" | grep -E '^[-+U]'
    echo ""
    echo "  '-' = not initialized"
    echo "  '+' = wrong commit checked out"
    echo "  'U' = merge conflict"
    exit 1
fi

echo "All submodules OK"

# ── 6. Report ────────────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════"
echo "Worktree ready"
echo "  Source repo:     $MAIN_REPO"
echo "  Branch:          $BRANCH"
echo "  Worktree path:   $WORKTREE_PATH"
echo "  Submodule reuse: $REUSE_MODE"
echo "  Submodules:      OK"
echo ""
echo "Next steps:"
echo "  cd $WORKTREE_PATH"
echo "  mkdir -p build && cd build && ../build.sh Debug"
echo "══════════════════════════════════════════════════════════"
