# Smoke Testing Reference (probe)

Smoke tests validate end-to-end streaming SQL behaviour using the `probe` tool against live test cases in `tests/cluster/smoke/`.

---

## Get probe

```bash
# Check if already available
probe --version

# Install: extract once from Docker image
docker pull timeplus/probe:latest
docker run --rm --entrypoint cat timeplus/probe:latest /usr/local/bin/probe > ~/bin/probe
chmod +x ~/bin/probe
```

`run-smoke-tests.sh` also auto-extracts probe from Docker at runtime if it is not in PATH.

---

## Run smoke tests

```bash
# All suites — server starts and stops automatically
./run-smoke-tests.sh

# Limit to a suite
./run-smoke-tests.sh -s tumble_window

# Limit to a single case
./run-smoke-tests.sh -s tumble_window -k 00_tumble_window_max

# List matching cases without executing
./run-smoke-tests.sh --collect-only

# Reuse an already-running server (skip start/stop)
./run-smoke-tests.sh --reuse-server

# Reuse server on an explicit port
./run-smoke-tests.sh --port <tcp>
```

### Full options

| Option | Default | Purpose |
|--------|---------|---------|
| `-s SUITE` | all | Filter to a named suite |
| `-k CASE` | all | Filter to a named case |
| `-b BLACKLIST` | `bug,todo,skip` | Comma-separated tag blacklist |
| `--nodes N` | `1` | probe `nodes` variable (p1k1 = 1) |
| `--timeout N` | `120` | Per-case timeout in seconds |
| `--collect-only` | — | List cases, skip execution |
| `--reuse-server` | — | Skip server start/stop |
| `--port TCP` | auto | Explicit TCP port (implies `--reuse-server`) |

Environment overrides: `PROBE_BIN`, `CLICKHOUSE_BINARY`, `SMOKE_LOG_DIR`.

Logs: `tmp_data_<tcp>/logs/smoke/probe_smoke_results.log` (co-located with server; parallel-safe)

---

## How run-smoke-tests.sh works

1. Resolves `probe` binary: `$PROBE_BIN` → PATH → Docker extract.
2. Starts `proton` via `./start-local-proton.sh` (auto-selects free ports).
3. Generates a dynamic `p1k1.yaml` deployment file pointing at the actual ports
   (the static `tests/cluster/deployment/local/p1k1.yaml` hardcodes 8463 and is not used directly).
4. Runs `probe smoke tests/cluster/smoke -d <deploy_dir> -c p1k1 …`.
5. On exit (including Ctrl-C): stops server, removes tmp files.

---

## Suite / case structure

> **`-s` suite name** = directory name **without** the `NNNN_` numeric prefix.
> E.g. directory `0000_tumble_window` → `-s tumble_window`

```
tests/cluster/smoke/
  <NNNN>_<suite_name>/          # one directory per suite
    setup.yaml (or config.yaml) # shared setup: create streams (run once per suite)
    <NN>_<case_name>.yaml       # individual test cases

tests/cluster/deployment/local/
    p1k1.yaml                   # reference port layout (tcp=8463) — do not edit directly
    p3k1.yaml                   # 3-node reference layout
```

### Case YAML anatomy

```yaml
tags:
  - local-run          # required for local filtering
  - tumble window
description: "…"
cluster:
  - p1k1               # single-node; add p3k1 for cluster tests
steps:
  - type: stream       # open a streaming query (runs in background)
    name: "101"
    query: "SELECT … FROM tumble(…) EMIT STREAM"
    schema: [ {name: col, type: type} … ]
  - type: wait
    time: 3
  - type: query        # one-shot SQL (INSERT, DDL, etc.)
    sql: "INSERT INTO … VALUES"
    schema: [ … ]
    inputs: [ ['v1', 'v2'] ]
  - type: check        # assert streaming results
    target_name: "101"
    mode: sequence     # or "contain"
    expected_result:
      - ['val1', 'val2']
```

---

## Writing a new smoke test case

### Checklist

- [ ] Identify the target suite under `tests/cluster/smoke/` (or create `<NNNN>_<suite>/`)
- [ ] If creating a new suite, add a `setup.yaml` (creates shared streams used by all cases)
- [ ] Create `<NN>_<case_name>.yaml` with at minimum: `tags`, `cluster: [p1k1]`, `steps`
- [ ] Include `local-run` tag so the case is picked up locally
- [ ] Use `type: stream` for queries that emit continuously; `type: query` for one-shot SQL
- [ ] Place `type: wait` between stream start and data insert (allow query to initialise)
- [ ] Use `type: check` with `target_name` matching the stream step's `name`
- [ ] Verify locally: `./run-smoke-tests.sh -s <suite> -k <case>`

### Minimal new case template

```yaml
tags:
  - local-run
  - <feature tag>
description: "<one-line description>"
cluster:
  - p1k1
steps:
  # 1. Open streaming query
  - type: stream
    name: "101"
    query: "SELECT col1, col2 FROM tumble(test_smoke, timestamp, 5s)
            GROUP BY col1, window_start, window_end EMIT STREAM"
    schema:
      - {name: col1, type: string}
      - {name: col2, type: float32}

  # 2. Wait for query to initialise
  - type: wait
    time: 2

  # 3. Insert test data
  - type: query
    sql: "INSERT INTO test_smoke(id, value, timestamp) VALUES"
    schema:
      - {name: id,        type: string}
      - {name: value,     type: float32}
      - {name: timestamp, type: datetime64(3)}
    inputs:
      - ['dev1', 10.0, '2020-01-01 00:00:01']
      - ['dev1', 20.0, '2020-01-01 00:00:02']

  # 4. Assert results
  - type: check
    target_name: "101"
    mode: sequence        # or "contain" for unordered subset
    expected_result:
      - ['dev1', '30.0']
```

### Notes on `setup.yaml`

The suite's `setup.yaml` runs once before all cases and creates the shared streams (e.g. `test_smoke`). Cases should **not** create or drop these streams themselves — that's the setup's responsibility. Only add streams to setup that are reused across multiple cases.

---

## Probe binary resolution in CI vs local

| Environment | How probe is obtained |
|-------------|----------------------|
| Local (probe in PATH) | Used directly |
| Local (no probe, Docker available) | Extracted from `timeplus/probe:latest` |
| CI | probe pre-installed in runner image |
