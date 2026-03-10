# Timeplus Proton — Claude Code Instructions

## Invariants (always apply)

- C++20 streaming SQL engine extending ClickHouse with real-time stream processing
- Always use stripped binaries: `build/programs/stripped/bin/proton`, `build/src/stripped/bin/unit_tests_dbms`
- Temp files → `./` (current dir), NEVER `/tmp`
- Proton fences (`/// proton: starts/ends`) ONLY in ClickHouse-inherited code
- NEVER fence in `src/Storages/Stream/` or `namespace DB::Streaming` (already Proton-specific)
- Dual query modes: `SELECT FROM stream` = streaming; `SELECT FROM table(stream)` = historical
- All streams auto-add `_tp_time datetime64(3, 'UTC')` for event-time semantics

## Working conventions

- Do not rebase or amend shared history; add new commits instead.
- Do not commit directly to `develop`; create a branch or worktree for each task.
- C++ formatting follows the repo `.clang-format`; opening braces normally go on their own line.
- Multiple local build directories are supported, including `build`, `build_asan`, `build_tsan`, `build_ubsan`, and `build_release`.
- If a task needs temporary logs, downloads, or scripts, use a `tmp/` directory under the current working tree, not `/tmp`.

## Tool shortcuts

- For CI failure or performance-report investigation, prefer `/ci-diagnostics` and the uploaded report URLs from commit statuses over raw GitHub Actions logs.

## Skill routing

| Task | Skill |
|------|-------|
| Build, compile, run server/client/cluster, execute tests, verify results, troubleshoot build/test failures | [build-and-verify](../.github/skills/build-and-verify/SKILL.md) |
| Write/review C++ code, C++ design/style discussions | [cpp-coding](../.github/skills/cpp-coding/SKILL.md) |
| Write/debug streaming SQL, SQL semantics/behavior questions (EMIT, windows, JOINs, UDFs) | [sql-usage](../.github/skills/sql-usage/SKILL.md) |
| Understand codebase, architecture, design decisions, where code lives, component interactions, data flow | [architecture](../.github/skills/architecture/SKILL.md) |
| Resolve GitHub issues, triage bugs/features, create branches, commit messages, submit PRs | [issue-workflow](../.github/skills/issue-workflow/SKILL.md) |
| Review a PR, branch, or diff for correctness, streaming semantics, and performance | [review](../.github/skills/review/SKILL.md) |
| Create an isolated git worktree with local submodule reuse | [create-worktree](../.github/skills/create-worktree/SKILL.md) |
| Analyze collapsed allocation profiles or jemalloc dumps | [alloc-profile](../.github/skills/alloc-profile/SKILL.md) |
| Diagnose CI failures or performance comparison reports | [ci-diagnostics](../.github/skills/ci-diagnostics/SKILL.md) |

> Any task that involves a GitHub issue number (e.g. `#1234`), bug report, feature request, PR creation, branch naming, or commit message → always use `issue-workflow`.

## Invoking skills

Skills are invoked with the `/` slash command syntax:

```
/build-and-verify
/cpp-coding
/sql-usage
/architecture
/issue-workflow
/review
/create-worktree
/alloc-profile
/ci-diagnostics
```

Each skill's `SKILL.md` provides concise commands, checklists, and decision tables. Detailed references are in `../.github/skills/<skill>/references/`.

## Primary documentation

- Streaming SQL reference: https://docs.timeplus.com
- Docs repo (markdown source): https://github.com/timeplus-io/docs/tree/main/docs
  - SQL reference files: `sql-*.md` (e.g., `sql-create-stream.md`, `sql-create-view.md`)
  - Streaming topics: `streaming-*.md` (e.g., `streaming-aggregations.md`, `streaming-joins.md`)
  - Stream types: `append-stream.md`, `versioned-stream.md`, `changelog-stream.md`, `external-stream.md`, etc.
- Offline summaries: `../.github/skills/*/references/` (synced from docs above)
