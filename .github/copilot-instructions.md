# Timeplus Proton - Agent Instructions

## Invariants (always apply)
- C++20 streaming SQL engine extending ClickHouse with real-time stream processing
- Always use stripped binaries: `build/programs/stripped/bin/proton`, `build/src/stripped/bin/unit_tests_dbms`
- Temp files → `./` (current dir), NEVER `/tmp`
- Proton fences (`/// proton: starts/ends`) ONLY in ClickHouse-inherited code
- NEVER fence in `src/Storages/Stream/` or `namespace DB::Streaming` (already Proton-specific)
- Dual query modes: `SELECT FROM stream` = streaming; `SELECT FROM table(stream)` = historical
- All streams auto-add `_tp_time datetime64(3, 'UTC')` for event-time semantics

## Skill routing
| Task | Skill |
|------|-------|
| Build, compile, run server and client, execute tests, verify results, troubleshoot build/test failures | [build-and-verify](skills/build-and-verify/SKILL.md) |
| Write/review C++ code, C++ design/style discussions | [cpp-coding](skills/cpp-coding/SKILL.md) |
| Write/debug streaming SQL, SQL semantics/behavior questions (EMIT, windows, JOINs, UDFs) | [sql-usage](skills/sql-usage/SKILL.md) |
| Understand codebase, architecture, design decisions, where code lives, component interactions, data flow | [architecture](skills/architecture/SKILL.md) |
| Resolve GitHub issues, triage bugs/features, create branches, commit messages, submit PRs | [issue-workflow](skills/issue-workflow/SKILL.md) |
| Review a PR, branch, or diff for correctness, streaming semantics, and performance | [review](skills/review/SKILL.md) |
| Create an isolated git worktree with submodule reuse | [create-worktree](skills/create-worktree/SKILL.md) |
| Analyze collapsed allocation profiles or jemalloc dumps | [alloc-profile](skills/alloc-profile/SKILL.md) |
| Diagnose CI failures or performance comparison reports | [ci-diagnostics](skills/ci-diagnostics/SKILL.md) |

> **Note:** Any task that involves a GitHub issue number (e.g. `#1234`), bug report, feature request, PR creation, branch naming, or commit message → always use `issue-workflow`. Any request to review a PR/diff should use `review` in addition to `cpp-coding` when the change is non-trivial.

## Primary documentation
- Streaming SQL reference: https://docs.timeplus.com
- Docs repo (markdown source): https://github.com/timeplus-io/docs/tree/main/docs
  - SQL reference files: `sql-*.md` (e.g., `sql-create-stream.md`, `sql-create-view.md`)
  - Streaming topics: `streaming-*.md` (e.g., `streaming-aggregations.md`, `streaming-joins.md`)
  - Stream types: `append-stream.md`, `versioned-stream.md`, `changelog-stream.md`, `external-stream.md`, etc.
- Offline summaries: `.github/skills/*/references/` (synced from docs above)
