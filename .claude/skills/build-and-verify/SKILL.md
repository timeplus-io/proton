---
name: build-and-verify
description: Build, compile, run server/client/cluster, execute unit/stateless/SQL tests, verify results, and troubleshoot build/test failures. Make sure to use this skill whenever the user mentions building, compiling, ninja, cmake, running tests, test failures, starting the server, or pre-commit checks, even if they just say 'it doesn't build' or 'the test broke'.
---

# Build & Verify

## Binaries

| Binary | Path (stripped, preferred) | Fallback |
|--------|---------------------------|---------|
| Server / Client | `build/programs/stripped/bin/proton` | `build/programs/proton` |
| Unit tests | `build/src/stripped/bin/unit_tests_dbms` | `build/src/unit_tests_dbms` |

Stripped = 90 % smaller, faster startup, matches CI. Always prefer stripped; fall back only if the file does not exist.

> **Source-root rule** — all `build*` dirs embed absolute source paths. Use each worktree's own `build*`; never share across checkouts.

---

## Part 1 — Build

Always configure from inside the chosen `build*` directory with the repo's `build.sh`.
Do not substitute direct `cmake -S/-B` configure commands unless the user explicitly requests that workflow.

```bash
cd build && ../build.sh Debug   # configure (once)
cd build && ninja                # incremental rebuild
```

---

## Part 2 — Test

### Full run (pre-commit)

```bash
./run-all-tests.sh                # unit → SQL → smoke (all three)
./run-all-tests.sh --skip-smoke   # faster pass: unit + SQL only
./run-all-tests.sh --only unit    # single suite: unit | sql | smoke
./run-all-tests.sh --port <tcp>   # reuse running server for SQL + smoke
```

### Targeted run (during issue work)

| What changed | Run this |
|--------------|----------|
| C++ data structures / algorithms | `./run-unit-tests.sh <SuiteName>` |
| SQL parsing / planner / execution | `./run-sql-tests.sh <test_id>` |
| Streaming end-to-end behaviour | `./run-smoke-tests.sh -s <suite>` |
| Unsure | `./run-all-tests.sh --skip-smoke` then add smoke if behaviour changed |

### Individual scripts

All scripts auto-manage server lifecycle and accept `--help`.

```bash
# Unit (no server needed)
./run-unit-tests.sh                          # all
./run-unit-tests.sh StorageStream            # prefix filter → StorageStream*
./run-unit-tests.sh "ParserStream.ParseDDL"  # exact case
./run-unit-tests.sh -l StorageStream         # list, don't run

# Stateless SQL
./run-sql-tests.sh                           # all
./run-sql-tests.sh 00001_select_1            # single test
./run-sql-tests.sh '0003_*'                  # wildcard prefix
./run-sql-tests.sh --port <tcp> 0003_*       # reuse running server

# Smoke
./run-smoke-tests.sh                         # all suites
./run-smoke-tests.sh -s tumble_window        # single suite
./run-smoke-tests.sh -s tumble_window -k 00_tumble_window_max
./run-smoke-tests.sh --collect-only          # list, don't run
./run-smoke-tests.sh --port <tcp>            # reuse running server
```

### Writing tests

| Change type | Add this test | Guide |
|-------------|--------------|-------|
| C++ logic / data structures | gtest case in `src/` | [references/unit-testing.md](references/unit-testing.md) |
| SQL behavior / streaming semantics | stateless `.sql` + `.reference` | [references/sql-testing.md](references/sql-testing.md) |
| End-to-end streaming scenario | smoke YAML case | [references/smoke-testing.md](references/smoke-testing.md) |

---

## Server (manual start/stop)

Only needed when running SQL or smoke tests with `--port` / `--reuse-server`.

```bash
./start-local-proton.sh             # prints tcp: and table_http: ports
build/programs/stripped/bin/proton client --port <tcp>
kill $(cat tmp_data_<tcp>/proton.pid)      # stop by port
```

Runtime data and logs: `./tmp_data_<tcp>/` — removed automatically on exit **when the test script started the server**. When using `--port`/`--reuse-server`, the directory is left untouched.
Test reports: `tmp_data_<tcp>/logs/sql/` and `tmp_data_<tcp>/logs/smoke/` (server-owned mode); `./tmp/sql_reports/` and `./tmp/smoke_logs/` (reuse mode).

> **Parallel-safe** — each `start-local-proton.sh` picks a free port automatically;
> multiple instances (or concurrent agents) coexist without conflicts.

---

## Debugging

```bash
# Running queries / stream state
build/programs/stripped/bin/proton client --port <tcp> \
  --query "SELECT * FROM system.processes"
build/programs/stripped/bin/proton client --port <tcp> --query "SHOW STREAMS"
build/programs/stripped/bin/proton client --port <tcp> --query "SHOW VIEWS"

# Server logs
tail -f ./tmp_data_<tcp>/data/var/log/proton-server/proton-server.log
tail -f ./tmp_data_<tcp>/data/var/log/proton-server/proton-server.err.log
```

---

## References

| Topic | File |
|-------|------|
| Unit tests: options, writing gtest cases | [references/unit-testing.md](references/unit-testing.md) |
| SQL tests: run, write, streaming patterns | [references/sql-testing.md](references/sql-testing.md) |
| Smoke tests: probe setup, options, writing YAML cases | [references/smoke-testing.md](references/smoke-testing.md) |
| Materialized view testing | [references/mv-testing.md](references/mv-testing.md) |
| JOIN testing patterns | [references/join-testing.md](references/join-testing.md) |
