# JOIN Patterns

## Supported types and strictness

| Type | Supported |
|------|-----------|
| INNER JOIN | Yes |
| LEFT JOIN | Yes |
| FULL/OUTER JOIN | Yes |
| RIGHT JOIN | No |
| CROSS JOIN | No |

| Strictness | Behavior |
|-----------|----------|
| ALL (default) | One left row can produce multiple results if right has multiple matches |
| ASOF | Closest match on unequal condition; at most one joined row per left row |
| LATEST | Only most recent key/value pairs from right side |

---

## 1. Static enrichment (stream + historical table)

Enrich streaming data with a static/historical dimension table.

```sql
SELECT device, vendor, cpu_usage, timestamp
FROM device_utils
INNER JOIN table(device_products_info) AS dim
    ON device_utils.product_id = dim.id;
```

`table()` on the right side makes it a bounded historical scan.

---

## 2. Dynamic enrichment (append + versioned_kv / changelog_kv)

Right side is a versioned/changelog dimension; latest version auto-selected.

```sql
-- Using versioned_kv
SELECT * FROM append_stream JOIN versioned_kv USING(k);

-- Using changelog_kv (latest version auto-selected)
SELECT * FROM append_stream LEFT JOIN changelog_kv USING(k);
```

---

## 3. ASOF JOIN (version-aware temporal match)

Matches on regular keys, then finds closest match on unequal condition.

```sql
SELECT orders._tp_time, order_id, product_id, quantity,
       price * quantity AS revenue
FROM orders ASOF JOIN dim_products
    ON orders.product_id = dim_products.product_id
    AND orders._tp_time >= dim_products._tp_time
SETTINGS keep_versions = 5;
```

`keep_versions` controls how many versions are retained in memory (default: 3).

---

## 4. LATEST JOIN

Only the most recent key/value pair from the right side.

```sql
SELECT * FROM append_stream
ASOF LATEST JOIN versioned_kv
    ON append_stream.k = versioned_kv.k;
```

---

## 5. Bidirectional JOIN (versioned_kv + versioned_kv)

Both sides receive real-time updates. Hash tables built for both sides.

```sql
SELECT k, count(*), min(i), max(i), avg(i), min(ii), max(ii), avg(ii)
FROM left_versioned JOIN right_versioned
    ON left_versioned.k = right_versioned.kk
GROUP BY k;
```

Production-ready. Supports INNER, LEFT, FULL.

---

## 6. Range JOIN (time-bounded, append + append)

Uses `date_diff_within()` to bound the join window and prevent unbounded state.

```sql
-- Default: uses _tp_time from both sides
SELECT l.*, r.*
FROM left_stream AS l
INNER JOIN right_stream AS r
    ON l.key = r.key
    AND date_diff_within(2m);

-- Explicit columns
SELECT l.*, r.*
FROM left_stream AS l
INNER JOIN right_stream AS r
    ON l.key = r.key
    AND date_diff_within(2m, l.event_time, r.event_time);

-- Numeric range (non-time)
SELECT *
FROM left_stream AS l
INNER JOIN right_stream AS r
    ON l.key = r.key
    AND l.sequence_number < r.sequence_number + 10;
```

Bidirectional ALL JOINs on append streams buffer both sides; control with `join_max_buffered_bytes`.

---

## 7. Self-JOIN (pattern detection in same stream)

```sql
SELECT *
FROM stream1
INNER JOIN stream1 AS stream2
    ON stream1.id = stream2.id
    AND date_diff_within(1m);
```

---

## 8. Direct JOIN (PK/index lookup, no full table load)

Avoids loading the entire right side into memory. Uses primary key or secondary index for targeted lookups.

```sql
-- Direct JOIN with versioned_kv stream
SELECT *
FROM test_left LEFT JOIN test_right
    ON test_left.k1 = test_right.k1 AND test_left.k2 = test_right.k2
SETTINGS join_algorithm = 'direct';
```

---

## 9. Dictionary JOIN (external source lookup)

```sql
CREATE DICTIONARY mysql_dict(id string, name string)
    PRIMARY KEY id
    SOURCE(MYSQL(DB 'testdb' TABLE 'products' ...))
    LAYOUT(HYBRID_HASH_CACHE(TTL 3600 ...));

SELECT *
FROM orders
JOIN mysql_dict AS products ON orders.product_id = products.id
SETTINGS join_algorithm = 'direct';
```

Triggers remote source requests when keys lack TTL validity.

---

## Compatibility matrix

| Left | Right | Join types | Strictness | Constraint |
|------|-------|-----------|------------|------------|
| append | `table(stream)` | INNER, LEFT | ALL | None (static lookup) |
| append | versioned_kv | INNER, LEFT | ALL, ASOF, LATEST | `keep_versions` for ASOF |
| append | changelog_kv | INNER, LEFT | ALL | Latest version auto-selected |
| append | append | INNER, LEFT | ALL | `date_diff_within()` required |
| versioned_kv | versioned_kv | INNER, LEFT, FULL | ALL | Both sides build hash tables |
| stream | stream (self) | INNER | ALL | `date_diff_within()` required |
| any | versioned_kv | LEFT, INNER | ALL | `join_algorithm='direct'` optional |
| any | dictionary | INNER | ALL | `join_algorithm='direct'` required |
