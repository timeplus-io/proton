# Issue Workflow — Detailed Reference

## Full workflow example

Resolve issue #10835 (bug report: "Memory aggregation incorrect with multi-shards"):

```bash
# 0. Draft todo list (mirror steps 1-11 below)
# - Fetch issue context
# - Classify type
# - Create worktree
# - Analyze
# - Implement
# - Test
# - Format
# - Review (pre-commit)
# - Commit
# - Final review (post-commit)
# - Push & create draft PR

# 1. Fetch issue context
gh issue view 10835
gh issue view 10835 --comments

# 2. Classify → bug report → prefix: bugfix/

# 3. Create worktree from latest develop, then work inside it
git fetch origin develop
git worktree add .worktrees/bugfix-issue-10835-fix-multishards-memory-aggr \
  -b bugfix/issue-10835-fix-multishards-memory-aggr origin/develop
cd .worktrees/bugfix-issue-10835-fix-multishards-memory-aggr

# 4. Analyze: read issue, identify affected code
#    → src/Processors/Transforms/Streaming/MemoryAggregator.h
#    → src/Interpreters/Streaming/AggregatedDataVariants.h

# 5. Implement fix (follow cpp-coding skill)

# 6. Test (follow build-and-verify skill)
./start-local-proton.sh
cd build && ninja
./src/stripped/bin/unit_tests_dbms --gtest_filter="MemoryAggr*"
cd ../tests && CLICKHOUSE_PORT_TCP=8463 CLICKHOUSE_PORT_HTTP=8123 ./ported-clickhouse-test.py -b ../build/programs/stripped/bin/proton -q queries_ported 99091_emit_per_event

# 7. Format (changed blocks only)
git clang-format
git clang-format --diff  # verify no remaining diff

# 8. Review (pre-commit)
git diff

# 9. Commit
git add -A
git commit -m "$(cat <<'EOF'
Fix memory aggregation with multi-shards

to close #10835
Co-Authored-By: <agent name> <agent email>
EOF
)"

# 10. Final review (post-commit)
git log origin/develop...HEAD --oneline   # commit list
git diff origin/develop...HEAD --stat     # changed files summary
git diff origin/develop...HEAD            # full diff

# 11. Push & create draft PR
git push -u origin HEAD
gh pr create --draft --base develop \
  --title "Fix memory aggregation with multi-shards #10835" \
  --body "$(cat <<'EOF'
## Summary
- Fixed incorrect memory aggregation results when using multi-shard streams
- Consolidated MemoryAggregatedDataVariants type enum into MemoryAggregatedDataTable

## Test plan
- [ ] Unit tests pass: `MemoryAggr*`
- [ ] SQL test: `99091_emit_per_event` passes
- [ ] Manual streaming verification with multi-shard stream

Closes #10835
EOF
)"
```

## Branch naming examples

| Scenario | Branch name |
|----------|-------------|
| Bug: memory leak in stream join | `bugfix/issue-11234-fix-stream-join-memory-leak` |
| Feature: add ASOF join support | `feat/issue-11300-add-asof-join` |
| Enhancement: optimize window close | `enhancement/issue-11400-optimize-window-close` |
| Perf: reduce checkpoint overhead | `perf/issue-11500-reduce-checkpoint-overhead` |
| Chore: update CI docker image | `chore/issue-11600-update-ci-docker` |

Rules:
- Always kebab-case for description
- Keep description under 5 words
- Always include issue ID

## Edge cases

### Worktree already exists

```bash
# Remove the existing worktree, then recreate
git worktree remove .worktrees/bugfix-issue-10835-fix-memory-leak --force
git worktree add .worktrees/bugfix-issue-10835-fix-memory-leak \
  -B bugfix/issue-10835-fix-memory-leak origin/develop
```

### Branch already exists but is not attached to a worktree

```bash
# Reuse the existing branch in a new worktree
git worktree add .worktrees/bugfix-issue-10835-fix-memory-leak \
  bugfix/issue-10835-fix-memory-leak
```

### Develop moved ahead during work

```bash
# Rebase onto latest develop before PR
git fetch origin develop
git rebase origin/develop
# Resolve conflicts if any, then force-push
git push --force-with-lease
```

### Issue has no clear type

Default to `chore/` for ambiguous issues (e.g., refactoring, cleanup, documentation).

### Change has no issue

Use the same workflow, but omit `#<id>` from the PR title/body and do not include `Closes #<id>`.

## PR body template

```markdown
## Summary
- <1-3 bullet points describing the change>

## Test plan
- [ ] Unit tests pass: `<relevant filter>`
- [ ] SQL test: `<relevant test case>` passes
- [ ] Manual verification: <steps if needed>

Closes #<issue_id>
```

## Useful gh commands

```bash
# List open issues with labels
gh issue list --label "bug" --state open

# View PR checks status
gh pr checks

# Add labels to issue
gh issue edit <id> --add-label "bug,streaming"

# Link PR to issue (alternative to Closes #)
gh pr edit <pr_id> --add-issue <issue_id>

# View PR review comments
gh api repos/{owner}/{repo}/pulls/<pr_id>/comments
```
