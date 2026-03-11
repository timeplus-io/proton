# Production Materialized View Configuration

## Creation syntax

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] <view_name>
[INTO <target_stream_or_table>]
AS <SELECT ...>
[SETTINGS <key>=<value>, ...]
[COMMENT '<comments>'];
```

Starts executing immediately unless `pause_on_start=true`.

---

## Standard pattern

```sql
-- 1. Create target stream with explicit schema
CREATE STREAM device_summary_target(
    window_start datetime64(3),
    device_id string,
    avg_temp float64
) ORDER BY window_start, device_id;

-- 2. Create materialized view with explicit target
CREATE MATERIALIZED VIEW device_summary
INTO device_summary_target AS
SELECT
    window_start,
    device_id,
    avg(temperature) AS avg_temp
FROM tumble(device_metrics, timestamp, 5m)
GROUP BY window_start, device_id;
```

---

## All SETTINGS parameters

### Checkpointing

| Setting | Purpose |
|---------|---------|
| `checkpoint_interval` | Interval in seconds; < 0 disables checkpointing |
| `checkpoint_settings` | Semicolon-separated string (see below) |

`checkpoint_settings` sub-keys:

| Key | Values | Default |
|-----|--------|---------|
| `replication_type` | `auto`, `nativelog`, `shared`, `local_file_system` | `auto` |
| `interval` | Seconds (dynamically adjusted by query weight) | — |
| `async` | `true`/`false` — dump to local disk, then replicate async | `true` |
| `incremental` | `true`/`false` — persist only updated state | — |
| `shared_disk` | Write checkpoints to shared storage (e.g., S3) | — |

```sql
CREATE MATERIALIZED VIEW mv_name
INTO target_stream
SETTINGS
    checkpoint_interval=30,
    checkpoint_settings='replication_type=auto;async=true;incremental=true'
AS SELECT ...;
```

### Hash table / aggregation

| Setting | Purpose |
|---------|---------|
| `default_hash_table` | `memory` or `hybrid` (hot keys in memory, cold spill to RocksDB) |
| `default_hash_join` | Hash table implementation for joins |
| `max_hot_keys` | Max in-memory keys before LRU spill to disk |
| `memory_weight` | Indicator of memory consumption for workload distribution |

```sql
CREATE MATERIALIZED VIEW high_cardinality_agg
INTO target_stream
SETTINGS
    default_hash_table='hybrid',
    max_hot_keys=10000
AS
SELECT unique_id, count(*) AS cnt
FROM high_volume_stream
GROUP BY unique_id
EMIT PERIODIC 10s;
```

### Execution control

| Setting | Purpose |
|---------|---------|
| `preferred_exec_node` | Node affinity for execution |
| `pause_on_start` | `true` = start paused; resume with `SYSTEM RESUME` |

### Error handling

| Setting | Default | Purpose |
|---------|---------|---------|
| `enable_dlq` | false | Send poison events to `system.mat_view_dlq` |
| `dlq_max_message_batch_size` | 10 | Max poison events per batch |
| `dlq_consecutive_failures_limit` | 100 | Max logged failures before stopping |
| `recovery_policy` | strict | `strict` or `best_effort` for failure recovery |
| `recovery_retry_for_same_error` | — | Max retries for same error |
| `input_format_ignore_parsing_errors` | false | Skip parsing errors |

---

## Schema evolution

```sql
-- 1. Add column to target stream
ALTER STREAM aggr_results ADD COLUMN max_i int;

-- 2. Modify MV query to populate new column
ALTER VIEW tumble_aggr_mv MODIFY QUERY SELECT
    window_start AS win_start,
    s,
    sum(i) AS total,
    max(i) AS max_i
FROM tumble(random_source, 2s)
GROUP BY window_start, s;
```

## Modify settings at runtime

```sql
ALTER VIEW tumble_aggr_mv MODIFY QUERY SETTING
    checkpoint_interval=-1, enable_dlq=true;
```

## Modify comment

```sql
ALTER VIEW tumble_aggr_mv MODIFY COMMENT 'new comment';
```

---

## Target types

| Target | Description |
|--------|-------------|
| Append stream | Standard time-series target |
| versioned_kv stream | KV target with latest-version tracking |
| External stream (Kafka) | Write results to Kafka topic |
| External table (ClickHouse, S3) | Write to external systems |

When `INTO` is omitted, an internal append stream is auto-created. **Not recommended** in production (limited customization, no schema evolution).

---

## Lifecycle

| Operation | Command |
|-----------|---------|
| Create | `CREATE MATERIALIZED VIEW ...` |
| Pause | `pause_on_start=true` at creation |
| Resume | `SYSTEM RESUME VIEW <name>` |
| Query | `SELECT * FROM <view>` (behavior depends on target type) |
| Alter query | `ALTER VIEW ... MODIFY QUERY ...` |
| Alter settings | `ALTER VIEW ... MODIFY QUERY SETTING ...` |
| Drop | `DROP VIEW [IF EXISTS] <name>` |

---

## Production checklist

- [ ] Target stream created with explicit schema (before MV)
- [ ] MV uses `INTO target` (never implicit)
- [ ] Checkpointing configured for fault tolerance
- [ ] High-cardinality groups use `default_hash_table='hybrid'`
- [ ] EMIT policy matches latency/throughput requirements
- [ ] DLQ enabled for poison event handling
- [ ] Recovery policy set appropriately (`strict` or `best_effort`)
- [ ] Cleanup order: `DROP VIEW` → `DROP STREAM` (target) → `DROP STREAM` (source)
