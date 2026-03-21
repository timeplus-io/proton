---
name: sql-usage
description: Timeplus streaming SQL covering stream types, EMIT policies, window functions, JOINs, materialized views, external streams, and UDFs. Use when user asks to write or debug streaming SQL, explain SQL semantics or behavior, or design materialized views.
---

# Streaming SQL

Source of truth: https://docs.timeplus.com | Raw markdown: https://github.com/timeplus-io/docs/tree/main/docs

## Naming rules

| Element | Style | Example |
|---------|-------|---------|
| Keywords | `UPPERCASE` | `SELECT`, `CREATE STREAM`, `EMIT`, `JOIN` |
| Functions | `lowercase` | `count()`, `tumble()`, `date_diff_within()` |
| Data types | `lowercase` | `int64`, `float64`, `string`, `datetime64` |
| Identifiers | `lowercase_with_underscores` | `event_time`, `user_id` |
| Reserved fields | `_tp_` prefix | `_tp_time` (event timestamp), `_tp_delta` (changelog) |

## Stream type decision table

| Need | Type | Syntax |
|------|------|--------|
| Immutable events, time-series, high throughput | append (default) | `CREATE STREAM ... ORDER BY` |
| Updates/upserts, KV workload, version history, ASOF JOINs | versioned_kv | `CREATE STREAM ... PRIMARY KEY ... SETTINGS mode='versioned_kv'` |
| CDC semantics, track deletes via `_tp_delta` | changelog_kv | `CREATE STREAM ... PRIMARY KEY ... SETTINGS mode='changelog_kv'` |
| External source (Kafka, Pulsar, etc.) | external | `CREATE EXTERNAL STREAM ... SETTINGS type='kafka'` |

Full details → [references/stream-types.md](references/stream-types.md)

## Query modes

- `SELECT FROM stream` → streaming (continuous, future events)
- `SELECT FROM table(stream)` → historical (batch scan, returns once)

Three trigger types:
| Query type | Trigger |
|-----------|---------|
| Non-aggregation (tail/filter/transform) | When events arrive |
| Window aggregation | Window end + watermark |
| Global aggregation | Fixed interval (default 2s if `EMIT PERIODIC` omitted) |

## Window functions quick reference

| Function | Signature | Use case |
|----------|-----------|----------|
| `tumble` | `tumble(stream, [time_col], interval, [tz])` | Fixed non-overlapping windows |
| `hop` | `hop(stream, [time_col], slide, size, [tz])` | Sliding/overlapping windows |
| `session` | `session(stream, [time_col], MAXSPAN x AND TIMEOUT y)` | Inactivity-based windows |

- `time_col` defaults to `_tp_time` if omitted
- Intervals: `1s`, `5m`, `2h`, `3d`, `1w`, `1M`, `1q`, `1y`
- `window_start`, `window_end` auto-generated (left-closed, right-open `[)`)
- Hop: slide and size must use same unit; slide > size is unsupported
- Window nesting: max 2 levels; window-over-global is unsupported

## EMIT policy quick reference

| Context | Policy | Effect |
|---------|--------|--------|
| Window | `EMIT AFTER WINDOW CLOSE` | Default for windowed agg |
| Window | `EMIT AFTER WINDOW CLOSE WITH DELAY 2s` | Allow late events |
| Window | `EMIT AFTER WINDOW CLOSE WITH DELAY 1s AND TIMEOUT 3s` | Late events + force-close |
| Window | `EMIT ON UPDATE` | Emit when agg value changes per key |
| Window | `EMIT ON UPDATE WITH BATCH 2s` | Batched update detection |
| Global | `EMIT PERIODIC 5s` | Default (2s), batch periodic output |
| Global | `EMIT PERIODIC 5s REPEAT` | Emit even without new events |
| Global | `EMIT ON UPDATE` | Immediate on every change |
| Global | `EMIT CHANGELOG` | With `_tp_delta` (+1/-1) |
| Global | `EMIT PER EVENT` | Per-event (debug only, no parallelism) |
| Global | `EMIT AFTER KEY EXPIRE ... WITH MAXSPAN x AND TIMEOUT y` | Tracing/span aggregation |

Full formal syntax → [references/emit-policies.md](references/emit-policies.md)

## JOIN quick reference

| Pattern | Syntax key | Use case |
|---------|-----------|----------|
| Static enrichment | `stream JOIN table(lookup)` | Enrich with historical data |
| Dynamic enrichment | `append JOIN versioned_kv USING(k)` | Latest version auto-picked |
| Bidirectional | `versioned_kv JOIN versioned_kv` | Both sides updatable |
| Range (time-bounded) | `stream JOIN stream ... AND date_diff_within(2m)` | Bounded stream-to-stream |
| ASOF | `append ASOF JOIN versioned_kv ON ... AND t1 >= t2` | Closest version match |
| LATEST | `append LATEST JOIN versioned_kv ON ...` | Latest value only |
| Direct lookup | `stream JOIN versioned_kv ... SETTINGS join_algorithm='direct'` | PK/index lookup, no full load |
| Dictionary | `stream JOIN dict ... SETTINGS join_algorithm='direct'` | External source lookup |

Supported: INNER, LEFT, FULL. Unsupported: RIGHT, CROSS.
Strictness: ALL (default), ASOF, LATEST.

Full examples → [references/join-patterns.md](references/join-patterns.md)

## Materialized view checklist

- [ ] Stateless test default: use MatView without `INTO`, then verify with `table(mv)`
- [ ] Create target stream FIRST only when you need an extra sink stream
- [ ] Use explicit `INTO target` only when a target stream is required by the scenario
- [ ] Configure checkpointing: `SETTINGS checkpoint_interval=30`
- [ ] High-cardinality: `SETTINGS default_hash_table='hybrid', max_hot_keys=10000`
- [ ] Schema evolution (with target stream): `ALTER STREAM` target + `ALTER VIEW ... MODIFY QUERY`
- [ ] Cleanup order: `DROP VIEW` → (if created) `DROP STREAM` target → `DROP STREAM` source

Full config → [references/mv-production.md](references/mv-production.md)

## External stream (Kafka) quick reference

```sql
CREATE EXTERNAL STREAM events(raw string)
SETTINGS type='kafka', brokers='host:9092', topic='events';
```

Key settings: `data_format`, `security_protocol`, `sasl_mechanism`, `kafka_schema_registry_url`
Virtual columns: `_tp_message_key`, `_tp_message_headers`, `_tp_sn` (offset), `_tp_shard` (partition)
Query options: `SETTINGS shards='0,2'`, `seek_to='earliest'`

Full details → [references/external-streams.md](references/external-streams.md)

## UDF quick reference

```sql
CREATE FUNCTION udf_name(param type) RETURNS type LANGUAGE JAVASCRIPT AS $$ ... $$;
```

Scalar: receives array of values (batched), returns array.
UDAF: implement `initialize`, `process`, `finalize`, `serialize`, `deserialize`, `merge`.

Full details → [references/udf.md](references/udf.md)

## References

- [Stream type details and examples](references/stream-types.md)
- [All EMIT policies with formal syntax](references/emit-policies.md)
- [JOIN patterns with full examples](references/join-patterns.md)
- [Production materialized view configuration](references/mv-production.md)
- [External streams (Kafka, Pulsar)](references/external-streams.md)
- [User-defined functions (UDF/UDAF)](references/udf.md)
