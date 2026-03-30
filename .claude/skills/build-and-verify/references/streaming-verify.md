# Streaming Query Verification Pattern

Two approaches: ad-hoc (background process) or **stateless test** (preferred).

## Stateless test via MatView (preferred)

Create a MatView **without** `INTO` (auto-creates internal target stream), insert data, query `table(mv)` for historical results. This is deterministic, scriptable, and integrates with the test runner.

```sql
-- Setup
DROP VIEW IF EXISTS <id>_mv;
DROP STREAM IF EXISTS <id>_stream;
CREATE STREAM <id>_stream(id int, value float64);

-- MV without target stream (internal storage)
CREATE MATERIALIZED VIEW <id>_mv AS
  SELECT id, count() AS cnt, round(sum(value), 1) AS total
  FROM <id>_stream GROUP BY id EMIT PERIODIC 500ms;

SELECT sleep(2) FORMAT Null;

-- Insert test data (use explicit _tp_time for reproducibility)
-- Note: Proton/ClickHouse supports VALUES (v1)(v2) without commas; this is required in --multiquery mode
INSERT INTO <id>_stream(id, value, _tp_time) VALUES (1, 10.5, '2025-01-01 00:00:00')(2, 20.3, '2025-01-01 00:00:01');

SELECT sleep(3) FORMAT Null;

-- Verify via historical query on MV
SELECT id, cnt, total FROM table(<id>_mv) ORDER BY id;

-- Cleanup
DROP VIEW <id>_mv;
DROP STREAM <id>_stream;
```

Key rules:
- `window_start`/`window_end` are reserved — alias them in MV: `window_start AS win_start`
- Tumble windows need a later event to advance watermark and close the window
- Use `SELECT sleep(N) FORMAT Null;` between steps (not bash `sleep`)
- Use `SELECT * except _tp_time FROM table(mv) ORDER BY ...` to exclude non-deterministic timestamps
- Reference file: exact expected output, one value per line, tab-separated columns

Run: `cd tests && CLICKHOUSE_PORT_TCP=<tcp_port> CLICKHOUSE_PORT_HTTP=<table_http_port> ./ported-clickhouse-test.py -b ../build/programs/stripped/bin/proton -q queries_ported <id>_<name>`

Example: [99030_pause_resume_mv.sql](../../../../tests/queries_ported/0_stateless/99030_pause_resume_mv.sql)

## Ad-hoc verification (background process)

```bash
# 1. Start streaming query in background
build/programs/stripped/bin/proton client \
  --query "SELECT * FROM <stream>" > output.log 2>&1 &
QUERY_PID=$!

# 2. Insert test data
build/programs/stripped/bin/proton client \
  --query "INSERT INTO <stream> VALUES (1, 'a'), (2, 'b')"

# 3. Wait for processing
sleep 3

# 4. Stop the streaming query
kill $QUERY_PID

# 5. Verify results
cat output.log
```

## Historical verification (simpler, preferred when applicable)

Use `table()` for immediate, complete results without background processes:

```sql
-- Count total rows
SELECT count(*) FROM table(<stream>);

-- Replay all data ordered by time
SELECT * FROM table(<stream>) ORDER BY _tp_time;

-- Verify windowed aggregation
SELECT window_start, window_end, sum(value)
FROM tumble(table(<stream>), 2s)
GROUP BY window_start, window_end
ORDER BY window_start;
```

`table()` = historical scan (not streaming). Better for test assertions because it returns once and completes.

## Cleanup

Always clean up test resources after verification:

```sql
-- Drop views first, then streams
DROP VIEW IF EXISTS <view>;
DROP STREAM IF EXISTS <stream>;
```
