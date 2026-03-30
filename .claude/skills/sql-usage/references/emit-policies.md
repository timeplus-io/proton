# EMIT Policies

## Formal syntax

### Global aggregation EMIT

```
EMIT [STREAM|CHANGELOG]
     [PERIODIC <interval> [REPEAT]]
     [ON UPDATE [WITH BATCH <interval>]]
     [AFTER KEY EXPIRE [IDENTIFIED BY <col>] WITH [ONLY] MAXSPAN <interval> [AND TIMEOUT <interval>]]
```

Default: `EMIT STREAM PERIODIC 2s`

### Time-window aggregation EMIT

```
EMIT [AFTER WINDOW CLOSE [WITH DELAY <interval> [AND TIMEOUT <interval>]]]
     [PERIODIC <interval> [REPEAT] [WITH DELAY <interval> [AND TIMEOUT <interval>]]]
     [ON UPDATE [WITH BATCH <interval>] [WITH DELAY <interval> [AND TIMEOUT <interval>]]]
```

Default: `EMIT AFTER WINDOW CLOSE`

Interval units: `ms`, `s`, `m`, `h`, `d`

---

## Window aggregation policies

### AFTER WINDOW CLOSE (default)

Emits when watermark advances past window end.

```sql
SELECT window_start, device_id, avg(temperature)
FROM tumble(device_metrics, 5m)
GROUP BY window_start, device_id
EMIT AFTER WINDOW CLOSE;
```

### WITH DELAY

Allows grace period for late-arriving events.

```sql
EMIT AFTER WINDOW CLOSE WITH DELAY 2s;
```

### WITH DELAY AND TIMEOUT

Grace period + force-close if no events arrive within timeout.

```sql
EMIT AFTER WINDOW CLOSE WITH DELAY 1s AND TIMEOUT 3s;
```

### ON UPDATE (window)

Emits when aggregation value changes for a key, even before window closes.

```sql
SELECT window_start, cid, count() AS cnt
FROM tumble(car_live_data, 5s)
WHERE cid IN ('c00033', 'c00022')
GROUP BY window_start, cid
EMIT ON UPDATE;
```

### ON UPDATE WITH BATCH (window)

Batches update checks at specified interval.

```sql
EMIT ON UPDATE WITH BATCH 2s;
```

---

## Global aggregation policies

### PERIODIC (default)

Emits at fixed interval. Default is 2 seconds. Only emits if new events arrived.

```sql
SELECT device_id, count(*) AS cnt
FROM device_metrics
WHERE cpu_usage > 99
GROUP BY device_id
EMIT PERIODIC 5s;
```

### PERIODIC REPEAT

Emits at fixed interval even without new events.

```sql
EMIT PERIODIC 5s REPEAT;
```

### ON UPDATE (global)

Emits immediately when any aggregation value changes. High volume.

```sql
EMIT ON UPDATE;
```

### ON UPDATE WITH BATCH (global)

Batches update detection.

```sql
EMIT ON UPDATE WITH BATCH 1s;
```

### CHANGELOG

Outputs `_tp_delta` column (+1 for insert, -1 for retract) for global and non-aggregation queries.

```sql
SELECT device_id, count(*) AS cnt
FROM device_metrics
GROUP BY device_id
EMIT CHANGELOG;
```

### PER EVENT

Emits for every incoming event. Debug/low-volume only. Does not support parallel processing.

```sql
SELECT count() FROM market_data EMIT PER EVENT;
```

### AFTER KEY EXPIRE

For tracing/span aggregation. Emits when keys expire based on a time column.

```sql
SELECT trace_id, count() AS span_count, max(duration) AS max_duration
FROM spans
GROUP BY trace_id
EMIT AFTER KEY EXPIRE IDENTIFIED BY end_time WITH MAXSPAN 500ms AND TIMEOUT 2s;
```

---

## Decision guide

| Need | Policy |
|------|--------|
| Standard window aggregation | `EMIT AFTER WINDOW CLOSE` (default) |
| Handle late events | add `WITH DELAY <duration>` |
| Force-close idle windows | add `AND TIMEOUT <duration>` |
| Low latency window updates | `EMIT ON UPDATE` |
| Periodic global output | `EMIT PERIODIC <interval>` |
| Continuous output even when idle | add `REPEAT` |
| Real-time per-change output | `EMIT ON UPDATE` (global) |
| CDC-style delta output | `EMIT CHANGELOG` |
| Debug/testing only | `EMIT PER EVENT` |
| Span/trace aggregation | `EMIT AFTER KEY EXPIRE` |
