# Materialized View Testing

## Steps

```sql
-- 1. Create target stream (explicit target is required for production)
CREATE STREAM <id>_mv_target(
    window_start datetime64(3),
    total int64
);

-- 2. Create materialized view
CREATE MATERIALIZED VIEW <id>_mv INTO <id>_mv_target AS
SELECT window_start, count(*) AS total
FROM tumble(<id>_stream, timestamp_col, 1m)
GROUP BY window_start;

-- 3. Insert test data
INSERT INTO <id>_stream VALUES (...);

-- 4. Verify via historical scan
SELECT * FROM table(<id>_mv_target) ORDER BY window_start;

-- 5. Cleanup (order matters: view first, then streams)
DROP VIEW <id>_mv;
DROP STREAM <id>_mv_target;
DROP STREAM <id>_stream;
```

## Checklist

- [ ] Target stream created BEFORE materialized view
- [ ] MV uses `INTO <target>` (explicit target)
- [ ] Verify with `table()` for deterministic results
- [ ] Cleanup order: DROP VIEW → DROP target STREAM → DROP source STREAM
- [ ] All resources prefixed with test ID
