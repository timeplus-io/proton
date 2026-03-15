---
name: review
description: Review a Proton pull request or diff for correctness, streaming semantics, checkpointing, compatibility, and performance. Use when the user wants a code review of a PR, branch, or diff.
---

# Review

## Arguments

- `$0` (required): PR number, branch name, or diff spec (for example `1234`, `my-branch`, `HEAD~3..HEAD`)

## Gather the change first

If `$0` is a PR number:

```bash
gh pr view "$0" --json title,body,baseRefName,headRefName,url
gh pr diff "$0"
```

If `$0` is a branch name:

```bash
git diff origin/develop..."$0"
git log --oneline origin/develop.."$0"
```

If `$0` is a diff spec:

```bash
git diff "$0"
git log --oneline "$0"
```

Read surrounding file context when the diff alone is insufficient.

## Review priorities

1. Correctness and safety
2. Streaming semantics and state recovery
3. Compatibility and user-visible behavior
4. Performance and resource usage
5. Maintainability

## Proton-specific checklist

- Streaming mode semantics are preserved:
  - `SELECT FROM stream` remains continuous.
  - `SELECT FROM table(stream)` remains historical/batch.
- Stateful processors and materialized views preserve `hasState()`, `checkpoint()`, and `recover()` invariants when touched.
- `_tp_time`, `_tp_delta`, window columns, and EMIT behavior stay internally consistent.
- Proton fences are used only in upstream-synced code and never in `src/Storages/Stream/` or `namespace DB::Streaming`.
- Cluster dependency boundaries remain acyclic; use [architecture](../architecture/SKILL.md) when touched code crosses modules.
- Parser, AST, serialization, metadata, and storage format changes include compatibility reasoning and tests.
- Query-plan or processor changes do not introduce hidden blocking work in `prepare()` paths.
- Join/window/aggregation changes do not create obvious unbounded state growth or watermark regressions.
- User-visible SQL behavior changes include tests and docs updates when appropriate.

## What to look for

### Correctness

- Wrong query results, dropped rows, duplicate rows, or broken changelog semantics
- Incorrect watermark or window-close behavior
- Missing cleanup on error paths
- Lifetime bugs, invalid references, iterator invalidation, or ownership confusion

### Concurrency and recovery

- Shared mutable state without synchronization
- Deadlock-prone lock ordering changes
- Checkpoint/recover mismatches
- State mutations hidden in code paths that should stay deterministic or replay-safe

### Performance

- Allocations or copies in hot streaming paths
- New per-event work in `prepare()`
- Unbounded maps/vectors keyed by user data
- Regressions in joins, windows, aggregation, or WAL/historical handoff

### Maintainability

- Logic duplicated across streaming and historical paths
- New magic constants that should be settings
- Heavy includes or non-trivial implementations pushed into widely included headers

## Tests expectation

For behavior changes, look for the smallest set that proves safety:

- Positive path test
- Negative/error-path test
- Recovery or restart test when state/checkpointing is involved
- Streaming SQL regression test for user-visible semantics

## Output format

### Findings

List findings first, ordered by severity.

- `Blocker` - correctness, corruption, crash, data loss, invalid recovery, major compatibility break
- `Major` - realistic bug risk, missing important tests, strong performance regression
- `Minor` - clarity or robustness issue that should be fixed but is not merge-blocking

Each finding should include:

- file and line
- concrete impact
- why it is a real problem
- smallest reasonable fix

### Then include

- Missing context
- Test coverage assessment
- Proton-specific compliance notes
- Final verdict: `Approve`, `Request changes`, or `Block`

## Review rules

- Be strict on real bugs, not style noise.
- Ignore pure formatting-only diffs.
- Prefer no comment over a weak or speculative comment.
- When no meaningful findings exist, state that explicitly and mention residual risk or missing evidence.
