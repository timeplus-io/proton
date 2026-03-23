# Stream Types

## Append stream (default)

Columnar, immutable. Optimized for range scans via sorting key. Two-layer storage per shard: WAL (streaming store) + historical store.

```sql
CREATE STREAM device_metrics(
    device_id string,
    temperature float64,
    timestamp datetime64(3)
) ORDER BY device_id;
```

### Storage types (`storage_type` setting)

| Type | Description |
|------|-------------|
| `hybrid` (default) | WAL + historical store |
| `streaming` | WAL only, no historical store |
| `inmemory` | WAL in-memory, single-instance only |

### Key settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `shards` | 1 | Parallel ingestion/query distribution |
| `replication_factor` | 1 | HA replicas (single-instance only) |
| `logstore_retention_bytes` | 1 | WAL size threshold before GC |
| `logstore_retention_ms` | 86400000 (1 day) | WAL age threshold before GC |
| `logstore_codec` | none | WAL compression: `lz4`, `zstd`, `none` |
| `flush_threshold_count` | 100000 | Rows before flush to historical |
| `flush_threshold_ms` | 2000 | Time before flush to historical |
| `flush_threshold_bytes` | 16777216 (16MB) | Bytes before flush to historical |
| `ingest_batch_max_bytes` | 67108864 (64MB) | Max ingestion batch size |
| `ingest_batch_timeout_ms` | 500 | Ingestion batch timeout |

### Advanced features

- `ORDER BY` (required): determines physical row order in historical store
- `PRIMARY KEY`: must be prefix of ORDER BY, supports non-unique keys
- `PARTITION BY`: optional, month-level granularity recommended
- `TTL`: automatic data expiration
- Column compression codecs: configurable per column
- Zero-replication WAL: `shared_disk='s3_disk'` for S3-compatible storage

### Multi-shard example

```sql
CREATE STREAM elastic_serving_stream(
    p string, id uint64, p2 uint32,
    c1 string, c2 int, v datetime64(3)
) SETTINGS shards=3, shared_disk='s3_disk', logstore_codec='zstd';
```

---

## Versioned stream (versioned_kv)

Columnar with version tracking. Only latest version shown in queries; multiple versions retained for JOINs.

```sql
CREATE STREAM dim_products(
    product_id string,
    price float32
) PRIMARY KEY (product_id)
SETTINGS mode='versioned_kv';
```

### Key settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `keep_versions` | 3 | Versions retained in memory for ASOF JOIN |

### ASOF JOIN usage

```sql
-- Automatically picks closest version at or before event time
SELECT orders._tp_time, order_id, product_id, quantity,
       price * quantity AS revenue
FROM orders ASOF JOIN dim_products
    ON orders.product_id = dim_products.product_id
    AND orders._tp_time >= dim_products._tp_time;
```

Note: Do not set TTL on historical storage for versioned streams.

---

## Changelog stream (changelog_kv)

Columnar with CDC semantics. `_tp_delta`: 1=insert, -1=delete.

```sql
CREATE STREAM user_changes(
    user_id int64,
    name string,
    action string
) PRIMARY KEY user_id
SETTINGS mode='changelog_kv';
```

### CDC semantics

- Query returns only latest version per primary key (auto-compacted)
- Updates require two rows: `_tp_delta=-1` (old) then `_tp_delta=1` (new)
- Aggregations (count, sum) reflect current state after compaction
- Can be used on either side of a JOIN; latest version auto-selected

### CDC integration (Debezium example)

Map CDC operations to `_tp_delta`:
- `c` (create) / `r` (read/snapshot) → `_tp_delta=1`
- `d` (delete) → `_tp_delta=-1`
- `u` (update) → paired: `-1` (before) then `1` (after)

---

## External stream

References outside data sources without internal storage.

```sql
CREATE EXTERNAL STREAM kafka_events(raw string)
SETTINGS type='kafka', brokers='localhost:9092', topic='events';
```

Types: `kafka`, `pulsar`, `log` (experimental).
See [external-streams.md](external-streams.md) for full details.

---

## Decision guide

| Need | Recommended type |
|------|-----------------|
| Immutable event log, time-series | append |
| Updates/upserts, KV workload, version history, ASOF JOINs | versioned_kv |
| CDC, track deletes | changelog_kv |
| External data (Kafka, etc.) | external |
