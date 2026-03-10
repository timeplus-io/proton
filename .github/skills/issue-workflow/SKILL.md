---
name: issue-workflow
description: End-to-end GitHub issue workflow including fetch/triage issue, branch naming, implement, test, commit message, and draft PR. Use for issue/bug/feature handling or branch/PR commands.
---

# Issue Workflow

## Workflow steps

- [ ] **1. Create TODO list** — Write a short checklist for this issue and keep it updated.
- [ ] **2. Fetch issue context** — `gh issue view <id> --comments` (title, body, labels, acceptance hints).
- [ ] **3. Sync & create worktree** — Prefer the dedicated [create-worktree](../create-worktree/SKILL.md) skill. If doing it inline, create an isolated worktree for this issue; **all subsequent steps run inside the worktree directory**:
  ```bash
  git fetch origin develop
  git worktree add .worktrees/<type>-issue-<id>-<desc> -b <type>/issue-<id>-<desc> origin/develop
  cd .worktrees/<type>-issue-<id>-<desc>
  ```
  - Branch/worktree name type convention:
    - Bug fix: `bugfix/issue-<id>-<desc>`
    - Feature work: `feat/issue-<id>-<desc>`
    - Performance work: `perf/issue-<id>-<desc>`
    - Enhancement work: `enhancement/issue-<id>-<desc>`
    - Test-only change: `test/issue-<id>-<desc>`
    - Chore/refactor/docs: `chore/issue-<id>-<desc>`
- [ ] **4. Analyze & plan** — Identify affected code and risks; choose approach (use [architecture](../architecture/SKILL.md) when needed).
- [ ] **5. Implement minimal fix** — Follow [cpp-coding](../cpp-coding/SKILL.md) / [sql-usage](../sql-usage/SKILL.md), keep scope limited to issue intent.
- [ ] **6. Verify with tests** — Follow [build-and-verify](../build-and-verify/SKILL.md). For behavior changes, add/adjust tests; if no new tests are needed, state why and list tests run.
- [ ] **7. Format & self-review** — `git clang-format`, then `git clang-format --diff` and `git diff` to confirm no accidental changes.
- [ ] **8. Commit** — Use a clear commit message with `Co-Authored-By`.
- [ ] **9. Final review, push, draft PR** — Check `git diff origin/develop...HEAD`, then `git push -u origin HEAD` and `gh pr create --draft ...`

## gh commands quick reference

```bash
# Fetch issue details (title, body, labels, state)
gh issue view <id>

# Fetch with comments
gh issue view <id> --comments

# Create draft PR linked to issue (always use --draft)
gh pr create --draft --base develop \
  --title "<type>: <description> #<id>" \
  --body "$(cat <<'EOF'
## Summary
<bullets>

## Test plan
- [ ] ...

Closes #<id>
EOF
)"

# If there is no issue, omit `#<id>` from the title and drop the `Closes #<id>` line.
```

## Commit message convention

```
<Concise description (under 72 chars)>

Co-Authored-By: <agent name> <agent email>
```

- Codex: `chatgpt-codex-connector[bot] <199175422+chatgpt-codex-connector[bot]@users.noreply.github.com>`
- Claude: `Claude <noreply@anthropic.com>`

## References

- [Detailed workflow with examples](references/workflow-detail.md)
