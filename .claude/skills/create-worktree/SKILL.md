---
name: create-worktree
description: Create a Proton git worktree with local submodule reuse. Make sure to use this skill whenever the user wants to work on a separate branch in isolation, needs a clean working copy, or mentions worktrees, even if they just say 'I want to start working on issue X in a separate directory'.
---

# Create Worktree

## Default policy

Use a dedicated worktree by default for any task that writes code, creates a branch/commit, or needs isolated build/test output.

Do not pay the worktree cost for pure read-only tasks such as code search, documentation lookup, or review-only requests unless the user explicitly asks for isolation.

## What it does

Creates a worktree without re-downloading the repo's submodule data. This repo has a large `contrib/` tree, so submodule reuse matters for both speed and disk usage.

The script isolates three things together:
- source-tree changes
- submodule working trees
- build/test artifacts that must stay bound to one source root

## Usage

Run the bundled script:

```bash
bash .claude/skills/create-worktree/scripts/create_worktree.sh <branch> [worktree-path]
```

- `branch` (required): branch name (e.g. `bugfix/issue-3446-session-watermark-regression`)
- `worktree-path` (optional): defaults to `.worktrees/<safe-branch>` under the repo root

The script will:
1. Resolve paths and validate inputs
2. Create the worktree (using existing local branch, remote tracking branch, or `origin/develop` as base)
3. Reuse submodule git data via hardlinks when possible (falls back to full copy)
4. Fix `core.worktree` pointers to reference the new worktree
5. Materialize submodule working trees
6. Validate submodule state
7. Report the result with next steps

## After the script completes

Initialize a build directory inside the worktree:

```bash
cd <worktree-path>
mkdir -p build && cd build && ../build.sh Debug
```

Do not replace this with direct `cmake` configure commands unless the user explicitly asks for that.

Do not reuse `build*` directories from the main checkout — CMake/Ninja build directories embed absolute source paths, so reusing them will either build the wrong tree or overwrite the original checkout's configuration.

## Examples

```bash
# Feature branch with default worktree path
bash .claude/skills/create-worktree/scripts/create_worktree.sh bugfix/issue-3446-session-watermark-regression

# Custom worktree path
bash .claude/skills/create-worktree/scripts/create_worktree.sh chore/worktree-skill-build-isolation ../proton-worktree-skill-build-isolation
```

## Notes

- This is local-only and should not need network access if the main repo already has submodules initialized.
- Build artifacts are not shared across source roots. Reconfigure/build inside the worktree you are validating.
- Prefer this skill instead of inlining raw `git worktree add`.
- To remove a clean worktree later (run from the **main checkout**, not from inside the worktree):

```bash
git -C /path/to/main/repo worktree remove <worktree-path>
```

- If removal is blocked by uncommitted or untracked files, inspect or back up that work first. Only use forced cleanup when you intentionally want to discard it:

```bash
git worktree remove --force <worktree-path>
```
