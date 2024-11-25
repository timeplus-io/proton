
# Demo for broken stream monitoring using global aggregation

This demo shows how to leverage the new feature of stream SQL to monitor a broken stream, traditionally using watermark, the stream processing will generally stop processing when there is no new event which pushes the watermark farward.

In this demo, we show how to use the new feature `EMIT PERIODIC time_interval REPEAT` to emit existing result to help monitor stream status.

here are the steps

1. create a simulated data source using random stream

```sql
CREATE RANDOM STREAM device
(
 `sensor` string DEFAULT ['A', 'B', 'C'][(int_div(rand(), 1000000000) % 3) + 1],
 `temperature` float DEFAULT round(rand_normal(10, 0.5), 2)
)
SETTINGS eps = 1
```

2. create a target stream which simulated the data ingested to proton, and use a mv to read the data from random stream into it.

```sql
CREATE STREAM device_reader
(
 `sensor` string,
 `temperature` float32
);

CREATE MATERIALIZED VIEW mv_device_reader INTO device_reader
AS
SELECT
 *
FROM
 device;
```

3. now user can monitor the stream using following query

```sql
WITH accumulate_count AS (
    SELECT count(*) AS count FROM device_reader EMIT PERIODIC 2s REPEAT
)
SELECT count, lag(count) AS previous_count FROM accumulate_count
```

the accumulate_count will emit current observed total count every 2 second, even when there is no new event, the query will keep emitting existing aggregation result
and then the query will return the current count and pervious observed count.

4. user can create an alert when the accumulation count does not change, which means something is wrong and the stream might be broken.

```sql
WITH accumulate_count AS (
    SELECT count(*) AS count FROM device_reader EMIT PERIODIC 2s REPEAT
)
SELECT count, lag(count) AS previous_count FROM accumulate_count
WHERE count = previous_count
```

5. now, we can drop the mv and the previouse query should emit the alert

```sql
DROP VIEW mv_device_reader
```


