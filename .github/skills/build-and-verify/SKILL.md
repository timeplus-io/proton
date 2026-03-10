---
name: build-and-verify
description: Build, compile, run server and client, execute unit/stateless/SQL tests, verify results, and troubleshoot build/test failures. Use when user asks to build or compile, run tests, diagnose compile/link errors, fix failing tests, or run pre-commit checks.
---

# Build & Verify

## Binaries (use stripped by default)

| Binary | Path |
|--------|------|
| Server/Client | `build/programs/stripped/bin/proton` |
| Unit Tests | `build/src/stripped/bin/unit_tests_dbms` |

Use stripped binaries by default; fall back only if stripped binaries do not exist:
`build/programs/proton`, `build/src/unit_tests_dbms`

Why stripped: 90% smaller, faster startup, matches production/CI.

## Source-root rule

All commands in this skill assume you are already in the source tree whose code you want to validate.

If you are working inside a git worktree:
- create/use that worktree's own `build*` directories
- do not run builds from the main checkout's `build*` directory unless it was configured for that exact same source root

Reason: CMake/Ninja build directories embed absolute source paths, so a reused `build_release` from another checkout may silently compile the wrong tree or require a reconfigure that disrupts the original checkout.

## Build

```bash
cd build && ../build.sh Debug                            # configure (once)
cd build && ninja                                         # incremental rebuild
```

## Run server

```bash
# Single node
./start-local-proton.sh
```

Single-node runtime artifacts are written to `./tmp_data_<tcp_port>/...` (no `/tmp` usage).

## Client

Default native port base: **8463** (actual port may auto-increment)
Use the `tcp:` / `table_http:` values printed by `./start-local-proton.sh` when connecting client or running SQL tests.

```bash
build/programs/stripped/bin/proton client                          # connects to localhost:8463
build/programs/stripped/bin/proton client --port 8463              # explicit port

# Non-interactive single query
build/programs/stripped/bin/proton client --query "SELECT 1"
```

## Pre-commit checklist

- [ ] `ninja` succeeds without error
- [ ] Unit tests pass: `./build/src/stripped/bin/unit_tests_dbms --gtest_filter="<Relevant>*"`
- [ ] SQL tests pass: `cd tests && CLICKHOUSE_PORT_TCP=<tcp_port> CLICKHOUSE_PORT_HTTP=<table_http_port> ./ported-clickhouse-test.py -b ../build/programs/stripped/bin/proton -q queries_ported <case>`
- [ ] Streaming query verified → see [references/streaming-verify.md](references/streaming-verify.md)
- [ ] `git clang-format --staged` executed on changed code blocks
- [ ] Proton fences correct (only in upstream code, no nesting)

## Writing new SQL test checklist

- [ ] Create `tests/queries_ported/0_stateless/<id>_<name>.sql`
- [ ] Create `tests/queries_ported/0_stateless/<id>_<name>.reference` (expected output)
- [ ] All resources named `<id>_*` (e.g., `99119_stream`, `99119_mv`)
- [ ] Stateless test default: use MatView without `INTO` + `table(mv)` pattern → see [references/streaming-verify.md](references/streaming-verify.md)
- [ ] Use `INTO target` only when test intent requires an extra target stream (e.g., validating downstream stream schema/data)
- [ ] Cleanup at end: `DROP VIEW` before `DROP STREAM`
- [ ] Test passes: `cd tests && CLICKHOUSE_PORT_TCP=<tcp_port> CLICKHOUSE_PORT_HTTP=<table_http_port> ./ported-clickhouse-test.py -b ../build/programs/stripped/bin/proton -q queries_ported <id>_<name>`

## Test commands quick reference

```bash
# Unit test - filter by pattern
./build/src/stripped/bin/unit_tests_dbms --gtest_filter="ParserSuite*"

# Unit test - specific class
./build/src/stripped/bin/unit_tests_dbms --gtest_filter="StorageStream*"

# Unit test - single case
./build/src/stripped/bin/unit_tests_dbms --gtest_filter="ParserStream.CreateStream"

# SQL test - single case
cd tests && CLICKHOUSE_PORT_TCP=8463 CLICKHOUSE_PORT_HTTP=8123 ./ported-clickhouse-test.py -b ../build/programs/stripped/bin/proton -q queries_ported 0001_simple_stream

# SQL test - wildcard
cd tests && CLICKHOUSE_PORT_TCP=8463 CLICKHOUSE_PORT_HTTP=8123 ./ported-clickhouse-test.py -b ../build/programs/stripped/bin/proton -q queries_ported 0001_*
```

## Debugging tips

```bash
# Check server status
build/programs/stripped/bin/proton client --query "SELECT * FROM system.processes"

# Show streams
build/programs/stripped/bin/proton client --query "SHOW STREAMS"

# Show materialized views
build/programs/stripped/bin/proton client --query "SHOW VIEWS"

# Server logs (path printed by start-local-proton.sh)
tail -f ./tmp_data_8463/data/var/log/proton-server/proton-server.log
```

## References

- [Streaming query verification pattern](references/streaming-verify.md)
- [Materialized view testing](references/mv-testing.md)
- [JOIN testing patterns](references/join-testing.md)
