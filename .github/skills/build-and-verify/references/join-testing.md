# JOIN Testing Patterns

## Bidirectional versioned_kv JOIN

```bash
# Setup (versioned_kv — latest version auto-selected per key)
proton client --query "CREATE STREAM <id>_left(id int, value string) PRIMARY KEY id SETTINGS mode='versioned_kv'"
proton client --query "CREATE STREAM <id>_right(id int, name string) PRIMARY KEY id SETTINGS mode='versioned_kv'"

# Start streaming JOIN in background
proton client --query "
  SELECT l.id, l.value, r.name
  FROM <id>_left AS l
  JOIN <id>_right AS r ON l.id = r.id
" > join.log 2>&1 &
JOIN_PID=$!

# Insert to both sides
proton client --query "INSERT INTO <id>_left VALUES (1, 'a'), (2, 'b')"
proton client --query "INSERT INTO <id>_right VALUES (1, 'foo'), (2, 'bar')"

# Wait and verify
sleep 2
kill $JOIN_PID
cat join.log

# Cleanup
proton client --query "DROP STREAM <id>_left"
proton client --query "DROP STREAM <id>_right"
```

Note: `proton` refers to `build/programs/stripped/bin/proton`.

## Range JOIN (time-constrained, append streams)

```sql
SELECT l.*, r.*
FROM <id>_left_stream AS l
INNER JOIN <id>_right_stream AS r
    ON l.key = r.key
    AND date_diff_within(2m);
```

Range JOINs require `date_diff_within()` to bound the join window.

## Static enrichment JOIN

```sql
SELECT o.order_id, o.product_id, p.product_name, p.price
FROM <id>_orders AS o
LEFT JOIN table(<id>_products) AS p ON o.product_id = p.id;
```

Use `table()` on the lookup side for static/historical enrichment.
