# SQL Testing Reference

## Running stateless tests

```bash
# Prerequisites: server running (./start-local-proton.sh)
# Use tcp: and table_http: values from its output

cd tests

# Single case by exact name
CLICKHOUSE_PORT_TCP=<tcp> CLICKHOUSE_PORT_HTTP=<table_http> \
  ./ported-clickhouse-test.py \
  -b ../build/programs/stripped/bin/proton \
  -q queries_ported <id>_<name>

# Wildcard prefix
CLICKHOUSE_PORT_TCP=<tcp> CLICKHOUSE_PORT_HTTP=<table_http> \
  ./ported-clickhouse-test.py \
  -b ../build/programs/stripped/bin/proton \
  -q queries_ported '<id>_*'
```

Test files live in `tests/queries_ported/0_stateless/`.

---

## Writing a new stateless SQL test

### Checklist

- [ ] Create `tests/queries_ported/0_stateless/<id>_<name>.sql`
- [ ] Create `tests/queries_ported/0_stateless/<id>_<name>.reference` (exact expected output)
- [ ] All resources named `<id>_*` (e.g. `99119_stream`, `99119_mv`)
- [ ] Use MatView **without** `INTO` as default pattern (auto-creates internal target stream)
- [ ] Use `INTO target` only when the test explicitly validates a separate target stream
- [ ] Cleanup at end: `DROP VIEW` before `DROP STREAM`
- [ ] Test passes locally before committing

### MatView without INTO (default pattern)

```sql
-- Setup
DROP VIEW IF EXISTS <id>_mv;
DROP STREAM IF EXISTS <id>_stream;
CREATE STREAM <id>_stream(id int, value float64);

-- MV with internal storage (no INTO)
CREATE MATERIALIZED VIEW <id>_mv AS
  SELECT id, count() AS cnt, round(sum(value), 1) AS total
  FROM <id>_stream
  GROUP BY id
  EMIT PERIODIC 500ms;

SELECT sleep(2) FORMAT Null;

-- Insert (use explicit _tp_time for reproducibility)
-- VALUES (v1)(v2) without commas — required in --multiquery mode
INSERT INTO <id>_stream(id, value, _tp_time)
  VALUES (1, 10.5, '2025-01-01 00:00:00')(2, 20.3, '2025-01-01 00:00:01');

SELECT sleep(3) FORMAT Null;

-- Verify via historical scan
SELECT id, cnt, total FROM table(<id>_mv) ORDER BY id;

-- Cleanup
DROP VIEW <id>_mv;
DROP STREAM <id>_stream;
```

### Key rules for streaming SQL tests

| Rule | Detail |
|------|--------|
| `window_start` / `window_end` reserved | Alias in MV: `window_start AS win_start` |
| Tumble window needs a closing event | Insert a later-timestamped row to advance watermark |
| Sleep between steps | Use `SELECT sleep(N) FORMAT Null;` — not bash `sleep` |
| Non-deterministic columns | Use `SELECT * EXCEPT _tp_time FROM table(mv) ORDER BY ...` |
| Reference file format | Exact output, one row per line, columns tab-separated |

---

## Ad-hoc streaming verification (quick debug, not for committed tests)

```bash
# 1. Start streaming query in background
build/programs/stripped/bin/proton client --port <tcp> \
  --query "SELECT * FROM <stream>" > output.log 2>&1 &
QUERY_PID=$!

# 2. Insert data
build/programs/stripped/bin/proton client --port <tcp> \
  --query "INSERT INTO <stream> VALUES ..."

# 3. Wait, stop, inspect
sleep 3
kill $QUERY_PID
cat output.log

# 4. Cleanup
build/programs/stripped/bin/proton client --port <tcp> \
  --query "DROP STREAM IF EXISTS <stream>"
```

## Historical verification via table()

```sql
-- Full scan
SELECT * FROM table(<stream>) ORDER BY _tp_time;

-- Windowed aggregation on historical data
SELECT window_start, window_end, sum(value)
FROM tumble(table(<stream>), 2s)
GROUP BY window_start, window_end
ORDER BY window_start;
```

`table()` = historical scan (returns once, no streaming semantics). Preferred for assertions in tests.

---

## Example test

[`tests/queries_ported/0_stateless/99030_pause_resume_mv.sql`](../../../../tests/queries_ported/0_stateless/99030_pause_resume_mv.sql)
