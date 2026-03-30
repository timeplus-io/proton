# Timeplus Proton - Agent Instructions

Canonical source: `.claude/CLAUDE.md`.

Keep this compatibility file aligned with `.claude/CLAUDE.md` when the shared rules change.

## Core invariants

- C++20 streaming SQL engine extending ClickHouse with real-time stream processing
- Always use stripped binaries: `build/programs/stripped/bin/proton`, `build/src/stripped/bin/unit_tests_dbms`
- Temp files go under `./tmp/` in the current working tree, never `/tmp`
- Proton fences (`/// proton: starts/ends`) only belong in ClickHouse-inherited code
- Never fence code in `src/Storages/Stream/` or `namespace DB::Streaming`
- `SELECT FROM stream` is streaming mode; `SELECT FROM table(stream)` is historical mode
- All streams auto-add `_tp_time datetime64(3, 'UTC')`

## Working conventions

- Do not rebase or amend shared history; add new commits instead
- Do not commit directly to `develop`; use a branch or worktree
- Follow the repo `.clang-format`
- If a task needs temporary logs, downloads, or scripts, keep them in `tmp/` under the current working tree

## Routing

- Use `.claude/skills/build-and-verify/SKILL.md` for build, compile, run, and test tasks
- Use `.claude/skills/cpp-coding/SKILL.md` for C++ code changes and reviews
- Use `.claude/skills/sql-usage/SKILL.md` for streaming SQL work
- Use `.claude/skills/review/SKILL.md` for PR or diff review
- Use `.claude/skills/create-worktree/SKILL.md` for isolated worktrees
- Use `.claude/skills/alloc-profile/SKILL.md` for allocation-profile analysis
- Use `.claude/skills/ci-diagnostics/SKILL.md` for CI and perf-report diagnosis

If a task involves a GitHub issue number, bug report, or feature request and needs issue triage, bounded implementation, targeted verification, gate review, or end-to-end issue delivery, use the `issue-workflow` agent in `.claude/agents/issue-workflow.md`. Unless the user explicitly opts out, continue through local commit and draft PR creation.
