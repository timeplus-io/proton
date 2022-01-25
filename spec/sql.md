# Proton SQL

This document describes the specification and semantics of Streaming SQL in Proton in an unofficial way with examples.

**Note** that we are still actively working on the SQL, so some of them are subject to change.

**Note** this document is for internal use only.

**Note** parameters in `[]` are optional in the above SQL spec and in this document.

# Database CRUD

## Database Provision

```sql
CREATE DATABASE [IF NOT EXISTS] <db_name> [COMMENT <Comments>]
```

Examples

```sql
CREATE DATABASE IF NOT EXISTS my_db COMMENT 'this is my first db';
```

Database name can be any utf-8 characters. If there is any space in the name, then it needs to be backtick quoted. For example, \`my db\`.

**Note** it is not working via Proton CLI in a cluster env yet, but we can do it via REST API (Please check [REST API spec](rest-api/ddl-service.yaml) for details).

**Note** database was reserved as `namespace` quite a while ago. We will need more discussion if we need support database concept
and provisioning and how it is related  to tenant.

**Note** database provisioning is an async process. In a single instance, it usually happens really quickly. In a cluster env (via REST API for now), Proton sequentially
sends the database provision request to each Proton instance, so it may take a while depending on how many Proton instance there are.
Proton provides a REST API to check the database provision status (Please check `/statuses` [REST API spec](rest-api/ingest.yaml) for more details).

## Database Deletion

```sql
DROP DATABASE [IF EXISTS] <db_name>;
```

**Note** like database provision, database deletion is an async process.

# Table CRUD

## Table Provision
```sql
CREATE TABLE [IF NOT EXISTS] [db.]<table_name>
(
    <col_name1> <col_type_1> [DEFAULT <col_expr_1>] [compression_codec_1],
    <col_name1> <col_type_2> [DEFAULT <col_expr_2>] [compression_codec_2]
)
SETTINGS <key1>=<value1>, <key2>=<value2>, ...
```

Table name can be any utf-8 characters and needs backtick quoted if there are spaces in between.
Column name can be any utf-8 characters and needs backtick quoted if there are spaces in between.

Proton supports the following column types
1. Int8/16/32/64/128/256
2. UInt8/16/32/64/128/256
3. Boolean
4. Decimal(precision, scale) : valid range for precision is [1: 76], valid range for scale is [0: precision]
5. Float32/64
6. Date
7. DateTime
8. DateTime64(precision, [time_zone])
9. String
10. FixedString(N)
11. Array(T)
12. UUID

When defining the columns, columns can have default value or column expression from which Proton can calculate a default value if user doesn't
provide an explicit one during ingestion. The column expression can be any valid Proton SQL statements which produces the correct type of result.

Each column can have an associated compression codec to store the data on disk more efficiently. Proton supports the following codecs.
1. NONE - No compression
2. LZ4
3. ZSTD
4. DoubleDelta
5. Gorilla

When defining a table, users also can specify `SETTINGS` which contains a list of key value paris. Proton supports the following key/value pairs to fine tune
the table provisioning.
1. shards=\<integer\>, defines number of shards a table can have in distributed env. By default, it is 1
2. replicas=\<integer\>, defines number of replicas for each shard in distributed env. By default, it is 1
3. sharding_expr=\<sql_expr\>, defines how to route data to shards during data ingestion in distributed env. By default, it is `rand()` which means randomly routes data to shards. The result of the sharding expression shall be an integer.
4. event_time_column=\<sql_expr\>, defines table event time column if users like to change the default behavior. It can be any sql expression which results in Datetime64 type.

Examples

```sql
CREATE TABLE default.my_table
(
    my_int_col Int64 DEFAULT 10 CODEC(NONE),
    my_decimal_col Decimal(10, 3) DEFAULT 0.0,
    my_datetime_col Datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta),
    my_float_col Float64 DEFAULT 1.1 CODEC(Gorilla),
    my_string_col String CODEC(LZ4),
    my_array_col Array(Int32),
    my_uuid_col UUID,
    my_int_col_with_complex_expr Int64 DEFAULT (my_int_col + 1000) / 3
)
SETTINGS shards=3, replicas=4, sharding_expr='hash(my_uuid_col)', event_time_column='my_datetime_col';
```

**Note** users can provision a table via REST API as well, which provides the following tuning parameters via URL parameters (?param_k=param_v). If provisioning table via Proton CLI / TCP, for now Proton doesn't
support fine tune these parameters directly, so it will use the default values for these settings.
1. streaming_storage_cluster_id: which stream storage cluster to use. Proton supports multiple streaming storage cluster.
2. streaming_storage_subscription_mode: Mode of subscribing / polling data from streaming storage. `shared` means sharing poll threads across different tables. `dedicated` means dedicated poll thread for the table. By default, it is `shared`.
3. streaming_storage_auto_offset_reset: Default offset to consume from streaming store. By default, `earliest`.
4. streaming_storage_request_required_acks: Waited ack during data ingestion to the backend write-ahead log. By default, it is `1`.
5. streaming_storage_request_timeout_ms: Time out value for an ingest request to the backend write-ahead log. By default, it is `30 seconds`.
6. streaming_storage_retention_bytes: When this threshold reaches, streaming storage deletes old data. By default, it is `-1` meaning no data retention.
7. streaming_storage_retention_ms: When this threshold reaches, streaming storage delete old data. By default, it is `-1` meaning no data retention.
8. streaming_storage_flush_messages: Tell streaming storage to call fsync per flush messages. By default, it is `1000`.
9. streaming_storage_flush_ms: Tell streaming storage to call fsync every flush_ms interval. By default, it is `-1` meaning time based fsync is disabled.
10. distributed_ingest_mode: Data ingestion mode for DistributedMergeTree. By default, it is `async` and supports `sync`, `fire_and_forget`.
11. distributed_flush_threshold_ms: Time threshold for DistributedMergeTree to flush consumed data from write-ahead log. By default, it is every `1` second.
12. distributed_flush_threshold_count: Row count threshold for DistributedMergeTree to flush consumed data from write-ahead log. By default, it is every `1000000`.
13. distributed_flush_threshold_bytes: Data size threshold for DistributedMergeTree to flush consumed data from write-ahead log. By default, it is every `10` MB.

**Note** table provisioning is an async process. In a single instance, it usually happens really quickly. In a cluster env (via REST API for now), Proton sequentially
sends the table provision request to each Proton instance, so it may take a while depending on how big `shards * replicas` is.
Proton provides a REST API to check the database provision status (Please check `/statuses` [REST API spec](rest-api/ingest.yaml) for more details).

## Table Altering
**Note** although Proton supports altering table schema (probably without data integrity / consistency guarantee in a cluster env), it requires more discussion if we should do this.
Skipping the table altering SQL spec for now.

## Table Deletion

```sql
DROP TABLE [IF EXISTS] db.<db_name>;
```

**Note** like table provision, table deletion is an async process and dropping table will delete the underlying streaming store storage as well.

# Data Ingestion

```sql
INSERT INTO <table_name> (<col_name_1>, <col_name_2>, ...)
VALUES
(<col_value_1>, <col_value_2>, ...), (<col_value_11>, <col_value_22, ...), ...
```

When appending data to target table, users can specify all column names in any order or some column names of the table to insert data.
If only some of the column names of the target table are specified, Proton will use default values for unspecified ones.

Examples:

```sql
INSERT INTO test(i, s) VALUES (1, 'hello'), (2, 'world'), (3, 'more');
```

**Note** Proton internally supports different ingest modes: `async, sync, ordered, fire_and_forget` and users can pick an ingestion mode
per ingest in Proton REST API. However in Proton CLI or ingest via TCP, users can't pick a mode instead `async` mode is used internally.

As async indicates, the ingestion is an async process. Proton provides a REST API to poll the ingestion status.

# Query Processing
In Proton, every SQL query is streaming by default which is different from traditional databases which are snapshot based query processing.
Streaming processing means once the query is established, it continuously calculates new arrived data incrementally (unbounded data) and emits the results
according to emit policy. Streaming query is always long running unless there is some exception happening. Snapshot based query processing on
the other hand, scans a snapshot of data which is bounded, processes the data snapshots, emits the results and then the query exits.

Proton also supports snapshot based query processing like traditional databases do, which we call `history` mode and also supports
a combination of streaming query processing and snapshot query processing (See the related sections below for more details).

**Node** in Proton, time is a first class citizen, every table in it has 2 physical timestamp columns: `_tp_time and _tp_index_time` which have DateTime64(3, 'UTC') type.
We call `_tp_time` event timestamp which is when an event happened. We call `_tp_index_time` data index timestamp which is when the data is indexed in Proton historical store
for fast snapshot data processing.

**Note** Internally Proton has other virtual timestamp columns: `_tp_ingest_time, _tp_append_time, _tp_consume_time, _tp_processing_time and _tp_emit_time`. Most of these timestamps
are for internally use only like performance troubleshooting or latency calculation. End users usually don't need worry about them.

## Streaming Processing

### Streaming SQL Syntax

Proton introduces several SQL extensions to support streaming processing. The overall syntax looks like

```sql
SELECT <expr, columns, aggr>
FROM <streaming_window_function>(<table_name>, [<time_column>], [<window_size>], ...)
[WHERE clause]
[GROUP BY clause]
EMIT [stream] <window_emit_policy>
SETTINGS <key1>=<value1>, <key2>=<value2>, ...
```

Overall a streaming query in Proton establishes a long HTTP/TCP connection to clients and continuously evaluates the query and streams back the results according to the `EMIT` policy until end clients
abort the query or there is some exception happening.
Proton supports some internal SETTINGS to fine tune the streaming query processing behaviors. Here is an exhaustive list of them. We will come back to them in sections below.
1. **query_mode=\<hist|streaming\>**. A general setting which decides if the overall query is historical data processing or streaming data processing. By default, it is `streaming`.
2. **keep_windows=\<window_count\>**. A setting which tells Proton to retain number of projected windows in memory. By default, it is `0` which means don't keep around any projected windows.
3. **seek_to=\<timestamp\>**. A setting which tells Proton to seek to old data in streaming store by a timestamp. It can be relative timestamp or an absolute timestamp. By default, it is `latest` which means don't seek old data.
4. **record_consume_batch_count=\<batch_count\>**: A setting tuning the latency and throughput. Proton tries to wait for this batch count before it processes the data.
5. **record_consume_time=\<timeout_ms\>**: A setting tuning the latency and throughput. Proton tries to wait for this amount of time for more data before it processes the data.

**Note** `stream` in `EMIT [stream]` clause is reserved for future use.

The following sections describe different scenarios with examples in detail and the `device_utils` table in the following sections
has this table structure.

```sql
CREATE TABLE device_utils
(
    device String,
    location String,
    cpu_usage Float32,
    timestamp DateTime64(3),
    _tp_time DateTime64(3, 'UTC')
    _index_time DateTime64(3, 'UTC')
);
```

### Streaming Tailing

```sql
SELECT <expr>, <columns>
FROM <table_name>
[WHERE clause]
```

Examples

```sql
SELECT device, cpu_usage
FROM devices_utils
WHERE cpu_usage >= 99
```

The above example continuously evaluates the filter expression on the new events in the table `device_utils` to filter out events
which have `cpu_usage` less than 99. The final events will be streamed to clients.

## Global Streaming Aggregation

In Proton, we define global aggregation as an aggregation query without using streaming window like tumble, hop. Unlike streaming window aggregation, global streaming aggregation doesn't slice
the unbound streaming data into windows according to timestamp, instead it processes the unbounded streaming data as one huge big global window. Due to this property, Proton for now can't
recycle in-memory aggregation states / results according to timestamp for global aggregation.

```sql
SELECT <column_name1>, <column_name2>, <aggr_function>
FROM <table_name>
[WHERE clause]
EMIT PERIODIC [INTERVAL <n> <UNIT>];
```

`PERIODIC INTERVAL <n> <UNIT>` tells Proton to emit the aggregation periodically. `UNIT` can be `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `QUARTER`, `MONTH`, `YEAR`.
`<n>` shall be an integer greater than 0.

Examples
```sql
SELECT device, count(*)
FROM device_utils
WHERE cpu_usage > 99
EMIT PERIODIC INTERVAL 5 SECOND;
```

Like in `Streaming Tail`, Proton continuously monitors new events in table `device_utils`, does the filtering and then
continuously does **incremental** count aggregation. Whenever the specified delay interval is up, project the current aggregation result
to clients.

**Note** everytime it fires, it projects a `snapshot` of the aggregation instead of a `delta` of the aggregation
comparing to last projection.

**Note** Proton also supports shortcuts for specifying the intervals, like `5s` means `INTERVAL 5 SECOND`. So the above query can be written as

```sql
SELECT device, count(*)
FROM device_utils
WHERE cpu_usage > 99
EMIT PERIODIC 5s;
```

The exhaustive list of shortcuts for interval is
1. s : seconds. Example `5s` => `INTERVAL 5 SECOND`
2. m : minutes. Example `5m` => `INTERVAL 5 MINUTE`
3. h : hours. Example `5h` => `INTERVAL 5 HOUR`
4. d : days. Example `5d` => `INTERVAL 5 DAY`
5. w : weeks. Example `5w` => `INTERVAL 5 WEEK`
6. M : months. Example `5M` => `INTERVAL 5 MONTH`
8. q : quarters. Example `5q` => `INTERVAL 5 QUARTER`
7. y : years. Example `5y` => `INTERVAL 5 YEAR`


## Tumble Streaming Window Aggregation

Tumble slices the unbounded data into different windows according to its parameters. Internally, Proton observes the data streaming and automatically decides when to
close a sliced window and emit the final results for that window.

```sql
SELECT <column_name1>, <column_name2>, <aggr_function>
FROM tumble(<table_name>, [<timestamp_column>], <tumble_window_size>, [<time_zone>])
[WHERE clause]
GROUP BY [window_start | window_end], ...
EMIT <window_emit_policy>
SETTINGS <key1>=<value1>, <key2>=<value2>, ...
```

Tumble window means a fixed non-overlapped time window. Here is one example for a 5 seconds tumble window:

```
["2020-01-01 00:00:00", "2020-01-01 00:00:05]
["2020-01-01 00:00:05", "2020-01-01 00:00:10]
["2020-01-01 00:00:10", "2020-01-01 00:00:15]
...
```

`tumble` window in Proton is left closed and right open `[)` meaning it includes all events which have timestamps
**greater or equal** to the **lower bound** of the window, but **less** than the **upper bound** of the window.

`tumble` in the above SQL spec is a table function whose core responsibility is assigning tumble window to each event in
a streaming way. The `tumble` table function will generate 2 new columns: `window_start, window_end` which correspond to the low and high
bounds of a tumble window.

`tumble` table function accepts 4 parameters: `<timestamp_column>` and `<time-zone>` are optional, the others are mandatory.

When `<timestamp_column>` parameter is omitted from the query, the table's default event timestamp column which is `_tp_time` will be used.

When `<time_zone>` parameter is omitted the system's default timezone will be used. `<time_zone>` is a string type parameter, for example `UTC`.

`<tumble_window_size>` is an interval parameter: `INTERVAL <n> <UNIT>` where `<UNIT>` supports `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`.
It doesn't yet support `MONTH`, `QUARTER`, `YEAR`. Like above section mentioned, users can use shortcuts for these intervals. For example, `tumble(my_table, 5s)`.

Proton supports 2 emit policies for tumble window, so `<window_emit_policy>` can be:
1. `AFTER WATERMARK`: aggregation results will be emitted and pushed to clients right after watermark is observed. This is the default behavior when this clause is omitted.
2. `AFTER WATERMARK AND DELAY <internval>`: aggregation results will be held after watermark is observed until the specified delay reaches. Users can use interval shortcuts for the delay. For example, `DELAY 5s`.

**Note** `watermark` is an internal timestamp which is observed and calculated and emitted by Proton and is used to indicate when a streaming window shall close. It is guaranteed to be increased monotonically per stream query.

Examples

```sql
SELECT device, max(cpu_usage)
FROM tumble(device_utils, 5s)
GROUP BY device, window_end
```

The above example SQL continuously aggregates max cpu usage per device per tumble window for table `devices_utils`. Every time a window
is closed, Proton emits the aggregation results.

```sql
SELECT device, max(cpu_usage)
FROM tumble(device_utils, 5s)
GROUP BY device, widnow_end
EMIT AFTER WATERMARK DELAY 2s;
```

The above example SQL continuously aggregates max cpu usage per device per tumble window for table `device_utils`. Every time a window
is closed, Proton waits for another 2 seconds and then emits the aggregation results.

```sql
SELECT device, max(cpu_usage)
FROM tumble(devices, timestamp, 5s)
GROUP BY device, window_end
EMIT AFTER WATERMARK DELAY 2s;
```

Same as the above delayed tumble window aggregation, except in this query, user specifies a **specific time column** `timestamp` for tumble windowing.

The example below is so called processing time processing which uses wall clock time to assign windows. Proton internally processes `now/now64` in a streaming way.

```sql
SELECT device, max(cpu_usage)
FROM tumble(devices, now64(3, 'UTC'), 5s)
GROUP BY device, window_end
EMIT AFTER WATERMARK DELAY 2s;
```

## Hop Streaming Window Aggregation

Like Tumble, Hop also slices the unbounded streaming data into smaller windows, and it has an addition sliding step.

```sql
SELECT <column_name1>, <column_name2>, <aggr_function>
FROM hop(<table_name>, [<timestamp_column>], <hop_slide_size>, [hop_windows_size], [<time_zone>])
[WHERE clause]
GROUP BY [<window_start | window_end>], ...
EMIT <window_emit_policy>
SETTINGS <key1>=<value1>, <key2>=<value2>, ...
```

Hop window is a more generalized window comparing to tumble window. Hop window has an additional
parameter called `<hop_slide_size>` which means window progress this slide size every time. There are 3 cases:

1. `<hop_slide_size>` is less than `<hop_window_size>`. Hop windows have overlaps meaning an event can fall into several hop windows.
2. `<hop_slide_size>` is equal to `<hop_window_size>`. Degenerated to a tumble window.
3. `<hop_slide_size>` is greater than `<hop_window_size>`. Windows has gap in between. Usually not useful, hence not supported so far.

Here is one hop window example which has 2 seconds slide and 5 seconds hop window.
```
["2020-01-01 00:00:00", "2020-01-01 00:00:05]
["2020-01-01 00:00:02", "2020-01-01 00:00:07]
["2020-01-01 00:00:04", "2020-01-01 00:00:09]
["2020-01-01 00:00:06", "2020-01-01 00:00:11]
...
```

Except that hop window can have overlaps, other semantics are identical to the tumble window.

```sql
SELECT device, max(cpu_usage)
FROM hop(device_utils, 2s, 5s)
GROUP BY device, window_end
EMIT AFTER WATERMARK;
```

The above example SQL continuously aggregates max cpu usage per device per hop window for table `device_utils`. Every time a window
is closed, Proton emits the aggregation results.

### Last X Streaming Processing

In streaming processing, there is one typical query which is processing last X seconds / minutes / hours of data. For example, show
me the cpu usage per device in the last 1 hour. We called this type of processing `Last X Streaming Processing` in Proton and
Proton provides specialized SQL extension for easy of use: `EMIT LAST INTERVAL <n> <UNIT>`. As in other part of streaming queries, users can
use interval shortcuts here.

**Note** For now, last X streaming processing is process time processing by default and Proton will seek streaming storage to backfill data in last X time range and it is using wall clock time to do the seek.
Event time based last X processing is still under development. When event based last X processing is ready, the by default last X processing will be changed to event time.

#### Last X Tail

Tailing events whose event timestamps are in last X range.

```sql
SELECT <column_name1>, <column_name2>, ...
FROM <table_name>
WHERE <clause>
EMIT LAST INTERVAL <n> <UNIT>;
```

Examples

```sql
SELECT *
FROM device_utils
WHERE cpu_usage > 80
EMIT LAST 5m
```

The above example filters events in `device_utils` table where `cpu_usage` greater than 80% and events are appended in the last 5 minutes.
Internally, Proton seeks streaming storage back to 5 minutes (wall-clock time from now) and tailing the data from there.

#### Last X Global Aggregation

```sql
SELECT <column_name1>, <column_name2>, <aggr_function>
FROM <table_name>
[WHERE clause]
GROUP BY ...
EMIT LAST INTERVAL <n> <UNIT>
SETTINGS max_keep_windows=<window_count>
```

**Note** Internally Proton chops streaming data into small windows and does the aggregation in each small window and as time goes, it slides out old small windows to keep the
overall time window fixed and keep the incremental aggregation efficient. By default, the maximum keeping windows is 100. If the last X interval is very big and the periodic emit interval is small,
then users will need explicitly setup a bigger max windows : `last_x_interval / periodic_emit_interval`.

Examples

```sql
SELECT device, count(*)
FROM device_utils
WHERE cpu_usage > 80
GROUP BY device
EMIT LAST 1h AND PERIODIC 5s
SETTINGS max_keep_windows=720;
```

#### Last X Windowed Aggregation

```sql
SELECT <column_name1>, <column_name2>, <aggr_function>
FROM <streaming_window_function>(<table_name>, [<time_column>], [<window_size>], ...)
[WHERE clause]
GROUP BY ...
EMIT LAST INTERVAL <n> <UNIT>
SETTINGS max_keep_windows=<window_count>
```

Examples

```sql
SELECT device, window_end, count(*)
FROM tumble(device_utils, 5s)
WHERE cpu_usage > 80
GROUP BY device, window_end
EMIT LAST 1h
SETTINGS max_keep_windows=720;
```

Similarly, we can apply last X on hopping window.

### Subquery

#### Vanilla Subquery

A vanilla subquery doesn't have any aggregation (this is a recursive definition), but can have arbitrary number of filter predicates, transformation functions.
Some systems call this `flat map`.

Examples

```sql
SELECT device, max(cpu_usage)
FROM (
    SELECT * FROM device_utils WHERE cpu_usage > 80 -- vanilla subquery
) GROUP BY device;
```

Vanilla subquery can be arbitrary nested until Proton's system limit is hit. The outer parent query can be any normal vanilla query or windows aggregation or global aggregation.

User can also writes the query by using Common Table Expression (CTE) style.

```sql
WITH (
    SELECT * FROM device_utils WHERE cpu_usage > 80 -- vanilla subquery
) AS filtered
SELECT device, max(cpu_usage) FROM filteed GROUP BY device;
```

#### Streaming Window Aggregated Subquery

A window aggregate subquery contains windowed aggregation. There are some limitations users can do with this type of subquery.
1. Proton supports window aggregation parent query over windowed aggregation subquery (hop over hop, tumble over tumble etc), but it only supports 2 levels. When laying window aggregation over window aggregation, please pay attention to the window size: the window
2. Proton supports multiple outer global aggregations over a windowed subquery. (Not working for now).
3. Proton allows arbitrary flat transformation (vanilla query) over a windows subquery until a system limit is hit.

Examples

```sql
-- tumble over tumble
WITH (
    SELECT device, avg(cpu_usage) AS avg_usage, any(window_start) AS window_start -- tumble subquery
    FROM
      tumble(device_utils, 5s)
    GROUP BY device, window_start
) AS avg_5_second
SELECT device, max(avg_usage), window_end -- outer tumble aggregation query
FROM tumble(avg_5_second, window_start, 10s)
GROUP BY device, window_end;
```

```sql
-- global over tumble
SELECT device, max(avg_usage) -- outer global aggregation query
FROM
(
    SELECT device, avg(cpu_usage) AS avg_usage -- tumble subquery
    FROM
        tumble(device_utils, 5s)
    GROUP BY device, window_start
) AS avg_5_second
GROUP BY device;
```

#### Global Aggregated Subquery

A global aggregated subquery contains global aggregation. There are some limitations users can do with global aggregated subquery:
1. Proton supports global over global aggregation and they can be multiple levels until a system limit is hit.
2. Flat transformation over global aggregation can be multiple levels until a system limit is hit.
3. Window aggregation over global aggregation is not supported.

Examples

```sql
SELECT device, maxK(5)(avg_usage) -- outer global aggregation query
FROM
(
    SELECT device, avg(cpu_usage) AS avg_usage -- global aggregation subquery
    FROM device_utils
    GROUP BY device
) AS avg_5_second;
```

### Streaming and Dimension Table Join
In Proton, every table has 2 modes: streaming and historical. Streaming mode focus on latest real-time tail data which is suitable for streaming processing.
On the other hand, historical focuses old indexed data in the past and optimized for big batch processing like terabytes large scan.
Streaming is the by default mode when a query is running against it. To query the historical data of a table, `hist()` function can be used.

There are typical cases that an unbounded data streaming needs enrichment by connecting to a relative static dimension table. Proton can do this in one single engine
by storing streaming data and dimension table in it via a streaming to dimension table join.

Examples

```sql
SELECT device, vendor, cpu_usage, timestamp
FROM device_utils
INNER JOIN hist(device_products_info)
ON device_utils.product_id = device_products_info.id
WHERE device_products_info._tp_time > '2020-01-01T01:01:01';
```

In the above example, data from `device_utils` is a stream and data from `device_products_info` is historical data since it is tagged with `hist()` function.
For every (new) row from `device_utils`, it is continuously (hashed) joined with rows from dimension table `device_products_info` and enrich the streaming data with product
vendor information.

Streaming to dimension table join has some limitations
1. The left table in the join needs to be streaming.
2. Only INNER / LEFT join are supported which means, users can only do
   1. `single streaming table` INNER JOIN `single or multiple dimension tables`
   2. `single streaming table` LEFT [OUTER] JOIN `single or multiple dimension tables`

### Streaming and Streaming Table Join
Under construction

### Materialized Streaming View

Proton introduces a new feature called materialized streaming view which works like traditional materialized view and CQP (Continuous Query Processing)
but Proton run the query in a streaming way by only applying the new data to the existing calculate states / results which is way more efficient.

To create a materialized streaming view, user can use this SQL spec

```sql
CREATE STREAMING VIEW [IF NOT EIXSTS] <streaming_view_name> AS <normal Proton streaming select query >
SETTINGS key1=value1, key2=value2, ...
```

Once the streaming view is created, internally Proton run the query in background continuously and incrementally and emit the calculated
results according to the semantics of its underlying streaming select. The results will be materialized into 2 places
1. An in-memory table
2. An internal physical table

```
                                 ------>  In-memory table
                                /
Underlying streaming SELECT -> |
                                \
                                 ------> Internal physical table
```

The materialized data gets stored in in-memory table and internal physical depends on the underlying streaming select query:
1. If the underlying streaming SELECT is a global aggregation, Proton does
   1. Only store the latest results of the underlying query to the in-memory table and there is a timestamp version `__tp_version` to every row.
   2. Always appends the results of the underlying query to the internal physical table and there is a `__tp_version` is attached to every row.

2. If the underlying streaming SELECT is a windowed aggregation or a flat transformation like tail, Proton does
   1. Append the latest results of the underlying query to the in-memory table until a configured memory limit (by default 100 MB) or block count limit (100 blocks) has reached. If a limit has reached, Proton will roll out old results.
   2. Always appends the results of the underlying query to the internal physical table

There are 2 modes to query streaming view
1. Streaming mode: like `SELECT * FROM streaming_view`. In this mode, the query is against the underlying inner physical table.
2. Historical mode: like `SELECT * FROM hist(streaming_view)`. In this mode, the query is aginst the in-memory table and it always returns the latest snapshot of data.

**Note** Users have a chance to tune the in-memory table size by setting up the following 2 settings
1. `max_streaming_view_cached_block_count`: By default it is 100 blocks
2. `max_streaming_view_cached_block_bytes`: By default it is 100 MB

**Note** For now, Proton doesn't implement checkpoint yet, so materialized streaming view may miss new updates / data across system reboot or process crashes etc.
The in-memory data will be discarded as well and will not be replayed across system reboot or process crashes.

Examples

```sql
CREATE STREAMING VIEW count_sv AS
SELECT device, count(*) FROM device_utils GROUP BY device
SETTINGS max_streaming_view_cached_block_count=1000,  max_streaming_view_cached_block_bytes=10000000;
```

And query the latest results
```sql
SELECT * FROM hist(count_cv);
```

User can do further streaming processing or create streaming views on top of existing streaming query (Streaming processing cascading / fanout) by querying
the existing streaming view in the streaming mode.
```sql
SELECT count(*) FROM count_cv;
```

To delete a streaming view, users can run the following SQL. Dropping the streaming view drops everything including the inner physical table.

```sql
DROP VIEW [IF EXISTS] <streaming_view_name>;
```

### Streaming Analytic Functions

Proton aims to provide common and easy of use streaming analytic functions out of box. The current list is:

1. min/max/avg/count/sum
2. topK/minK/maxK
3. unique / uniqueExact
4. streamingNeighbor

#### min/max/avg/count/sum Function Signatures

**min(\<column_name\>)**: minimum value of a column. For String column, the comparison is lexicographic order.

**max(\<column_name\>)**: maximum value of a column. For String column, the comparison is lexicographic order.

**avg(\<column_name\>)**: average value of a column (sum(column) / count(column)). Only works for numeric column.

**count()**: total number of rows

**sum(\<column_name\>)**: sum of the columns. Only works for numerics.

#### topK/minK/maxK Function Signatures

**topK(N)(\<column_name\>)**: Top frequent N items in column_name

**minK(N)(\<column_name\>)**: The least N items in column_name

**maxK(N)(\<column_name\>)**: The greatest N items in column_name

#### unique / uniqueExact Function Signatures

**unique(\<column_name1\>[, \<column_name2\>, ...])**: Calculates the approximate number of different values of the columns.

**uniqueExact(\<column_name1\>[, \<column_name2\>, ...])**: Calculates the exact number of different values of the columns.

#### streamingNeighbor

**streamingNeighbor(\<column_name\>, \<offset\>[, \<default_value\>])**: Access to a row at a specified offset which comes before the current row of a given column. \<offset\> must be a negative number.

## Vanilla View

Vanilla view in Proton is just like view in traditional database which is just a static query definition.

To create a vanilla view
```sql
CREATE VIEW [IF NOT EXISTS] <view_name> AS <SELECT ...>
```

To delete a vanilla view

```sql
DROP VIEW [IF EXISTS] <view_name>
```

## Historical Data Processing

As previous section indicates, Proton supports pure historical data query processing. There are 2 ways to do it
1. Run historical query to the whole query by setting `query_mode='hist'`. This mode is useful if there are multiple tables in the query and users like to do historical data processing as whole in the query.
2. Run historical query per table by wrapping table with `hist()` function. This mode is flexible and sometimes required in some scenarios like streaming and dimension table join.

Examples

```sql
SELECT * FROM device_utils SETTINGS query_mod='hist';
```

```sql
SELECT * FROM hist(device_utils);
```

Proton also supports MySQL / PostgresSQL protocols. If users access via these protocols, `hist` mode is the default mode.
