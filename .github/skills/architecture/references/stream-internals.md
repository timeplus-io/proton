# Stream Internals

## Two-layer storage architecture

Each shard maintains two storage layers:

```
Client writes → [ Streaming Store (WAL) ] → async batch compaction → [ Historical Store (Columnar) ]
```

| Layer | Role | Implementation |
|-------|------|---------------|
| **Streaming Store** | Write-Ahead Log for ingestion, incremental processing, replication | NativeLog (or KafkaLog) |
| **Historical Store** | Asynchronous compaction of WAL data into compressed columnar format | MergeTree-based |

### Storage types (`storage_type` setting)

| Type | WAL | Historical | Use case |
|------|-----|-----------|----------|
| `hybrid` (default) | Yes | Yes | Standard — streaming + analytical queries |
| `streaming` | Yes | No | Streaming-only, no historical backfill |
| `inmemory` | In-memory | No | Single-instance, volatile |

### Data flow

1. **Ingestion**: Data written to WAL (NativeLog)
2. **Streaming queries**: Read directly from WAL for incremental processing
3. **Compaction**: Asynchronous batch flush from WAL to historical store
4. **Historical queries**: `table(stream)` reads from historical store
5. **Replication**: WAL replication across nodes (not applicable in single-instance mode)

### Flush thresholds (WAL → Historical)

| Setting | Default | Purpose |
|---------|---------|---------|
| `flush_threshold_count` | 100000 | Row count trigger |
| `flush_threshold_ms` | 2000 | Time trigger (ms) |
| `flush_threshold_bytes` | 16777216 (16MB) | Byte size trigger |

### WAL retention

| Setting | Default | Purpose |
|---------|---------|---------|
| `logstore_retention_bytes` | 1 | Size-based GC threshold |
| `logstore_retention_ms` | 86400000 (1 day) | Age-based GC threshold |
| `logstore_codec` | none | Compression: `lz4`, `zstd`, `none` |

---

## Four stream types

### Append stream (default)
- **Encoding**: Columnar
- **Mutation**: Immutable (append-only)
- **Storage**: hybrid (WAL + historical), streaming-only, or in-memory
- **Key feature**: ORDER BY determines physical row order in historical store; optimized for range scans
- **Use case**: Time-series, event logs, high-throughput analytics

### Versioned stream (versioned_kv)
- **Encoding**: Columnar with version tracking
- **Mutation**: Latest version shown in queries; multiple versions retained for JOINs
- **Settings**: `mode='versioned_kv'`, `keep_versions=3`
- **Key feature**: ASOF JOIN picks closest version automatically
- **Use case**: KV workloads, lookup tables with temporal history, dimension tables

### Changelog stream (changelog_kv)
- **Encoding**: Columnar with CDC semantics
- **Mutation**: Updates via paired `_tp_delta` rows (-1 then +1)
- **Settings**: `mode='changelog_kv'`
- **Key feature**: Auto-compaction per primary key; aggregations reflect current state
- **Use case**: Change Data Capture from external systems (Debezium, etc.)

### External stream
- **Storage**: None (references external sources)
- **Types**: `kafka`, `pulsar`, `log` (experimental)
- **Key feature**: Real-time streaming queries on external data without copying

---

## Implicit columns

All stream types automatically have:
- `_tp_time datetime64(3, 'UTC') DEFAULT now64(3, 'UTC')` — Event timestamp
- `_tp_delta int8` — Change type (changelog_kv only): 1=insert, -1=delete

---

## IProcessor pattern

Every streaming transform in `src/Processors/Transforms/Streaming/` follows:

```cpp
class IProcessor {
    virtual Status prepare();    // O(1), non-blocking readiness check
    virtual void work();         // Actual processing
    enum Status { NeedData, PortFull, Ready, Finished, Async, ... };
};
```

Stateful processors MUST additionally implement:
- `hasState()` — Whether processor maintains state
- `checkpoint(CheckpointContextPtr)` — Serialize state for exactly-once semantics
- `recover(CheckpointContextPtr)` — Restore from checkpoint

**ManyAggregatedData pattern**: Variant-based parallelism allows multiple threads to aggregate independently, merging later.

---

## Watermark mechanics

- Advances monotonically per stream
- Based on event-time progress (not wall-clock)
- Determines when time windows can be safely closed
- Critical for late-event handling and exactly-once guarantees

Three streaming query trigger types:

| Query type | Trigger | Memory |
|-----------|---------|--------|
| Non-aggregation | When events arrive | Minimal |
| Window aggregation | Window close (watermark) | Per-window state, recyclable |
| Global aggregation | Fixed interval (default 2s) | Unbounded (cannot recycle states) |
