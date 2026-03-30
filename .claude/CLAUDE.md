# Timeplus Proton — Claude Code Instructions

## Invariants (always apply)

- C++20 streaming SQL engine extending ClickHouse with real-time stream processing
- Always use stripped binaries: `build/programs/stripped/bin/proton`, `build/src/stripped/bin/unit_tests_dbms`
- Temp files → `./tmp/` under the current working tree, NEVER `/tmp`
- Proton fences (`/// proton: starts/ends`) ONLY in ClickHouse-inherited code
- NEVER fence in `src/Storages/Stream/` or `namespace DB::Streaming` (already Proton-specific)
- Dual query modes: `SELECT FROM stream` = streaming; `SELECT FROM table(stream)` = historical
- All streams auto-add `_tp_time datetime64(3, 'UTC')` for event-time semantics

## Working conventions

- Do not rebase or amend shared history; add new commits instead.
- Do not commit directly to `develop`; create a branch or worktree for each task.
- C++ formatting follows the repo `.clang-format`; opening braces normally go on their own line.
- Multiple local build directories are supported, including `build`, `build_asan`, `build_tsan`, `build_ubsan`, and `build_release`.
- Always configure builds via the repo's `build.sh` from inside the chosen build directory (for example `mkdir -p build && cd build && ../build.sh Debug`); do not replace it with ad-hoc direct `cmake` configure commands unless the user explicitly asks.
- If a task needs temporary logs, downloads, or scripts, use a `tmp/` directory under the current working tree, not `/tmp`.

## Tool shortcuts

- For CI failure or performance-report investigation, prefer `/ci-diagnostics` and the uploaded report URLs from commit statuses over raw GitHub Actions logs.

## Skill routing

| Task | Skill |
|------|-------|
| Build, compile, run server/client/cluster, execute tests, verify results, troubleshoot build/test failures | [build-and-verify](skills/build-and-verify/SKILL.md) |
| Write/review C++ code, C++ design/style discussions | [cpp-coding](skills/cpp-coding/SKILL.md) |
| Write/debug streaming SQL, SQL semantics/behavior questions (EMIT, windows, JOINs, UDFs) | [sql-usage](skills/sql-usage/SKILL.md) |
| Review a PR, branch, or diff for correctness, streaming semantics, and performance | [review](skills/review/SKILL.md) |
| Create an isolated git worktree with local submodule reuse | [create-worktree](skills/create-worktree/SKILL.md) |
| Analyze collapsed allocation profiles or jemalloc dumps | [alloc-profile](skills/alloc-profile/SKILL.md) |
| Diagnose CI failures or performance comparison reports | [ci-diagnostics](skills/ci-diagnostics/SKILL.md) |

> Any task that involves a GitHub issue number (e.g. `#1234`), bug report, or feature request and needs issue triage, bounded implementation, targeted verification, gate review, or end-to-end issue delivery → use Agent tool with `issue-workflow` agent (orchestrates `issue-implement`, `issue-verify`, and `issue-review`). Unless the user explicitly opts out of local commit or draft PR creation, it should continue through both. Use the normal git/gh flow for standalone branch naming, commit message, commit, push, or PR creation tasks that are not part of an `issue-workflow` run.

## Invoking skills

Skills are invoked with the `/` slash command syntax:

```
/build-and-verify
/cpp-coding
/sql-usage
/review
/create-worktree
/alloc-profile
/ci-diagnostics
```

Each skill's `SKILL.md` provides concise commands, checklists, and decision tables. Detailed references are in `skills/<skill>/references/`.

## Primary documentation

- Streaming SQL reference: https://docs.timeplus.com
- Docs repo (markdown source): https://github.com/timeplus-io/docs/tree/main/docs
  - SQL reference files: `sql-*.md` (e.g., `sql-create-stream.md`, `sql-create-view.md`)
  - Streaming topics: `streaming-*.md` (e.g., `streaming-aggregations.md`, `streaming-joins.md`)
  - Stream types: `append-stream.md`, `mutable-stream.md`, `external-stream.md`, etc.
- Offline summaries: `skills/*/references/` (synced from docs above)
