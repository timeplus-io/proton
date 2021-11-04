# Proton Streaming Processing

This document describes the specification and semantics of streaming processing in proton in an un-official way with examples.
Please note we are still in progress to implement all of them and the streaming processing syntax / semantics are not even final.

## Streaming SQL Syntax

Proton introduces several SQL extensions to support streaming processing. The overall syntax looks like
```sql
SELECT <expr, columns, aggr>
FROM <streaming_window_function>(<table>, [<time-column>], [<window-size>], ...)
[WHERE clause]
[GROUP BY clause]
EMIT STREAM <window-emit-policy>
...
```

**Note**, parameters in `[]` are optional in the above SQL spec and in this document.

Overall a streaming query in Proton establishes a long HTTP/TCP connection to clients and continuously
evaluates the query and streams back the results according to the `EMIT` policy until end clients
abort the query.

The following sections describes different scenarios with examples in detail and the `devices` table in the following sections
has the following structure.

```sql
CREATE TABLE devices
(
    device String,
    location String,
    cpu_usage Float32,
    timestamp DateTime64(3),
    _time DateTime64(3)
);
```

## Streaming Tailing

```sql
SELECT <expr>, <columns>
FROM <table>
[WHERE clause]
EMIT STREAM;
```

`EMIT STREAM` here tells Proton this is a streaming query. It does a streaming live tail of the table `devices`.
The semantic of this tailing query is Proton continuously monitors if there are new events / rows in
the table. If there are, do the filtering in realtime and if after filtering, there are still events left, project
the events to end users continuously.

Examples

```sql
SELECT device, cpu_usage
FROM devices
WHERE cpu_usage >= 99
EMIT STREAM;
```

The above example continuously evaluates the filter expression on new events in the table `devices` to filter out events
which have `cpu_usage` less than 99. The final events will be streamed to clients.

## Global Streaming Aggregation

```sql
SELECT <aggr-function>
FROM <table>
[WHERE clause]
EMIT STREAM PERIODIC INTERVAL <n> <UNIT>;
```

`EMIT STREAM` has the same semantic as above which tells Proton this is a streaming query.

`PERIODIC INTERVAL <n> <UNIT>` tells Proton to emit the aggregation periodically. `UNIT` can be
`SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `QUARTER`, `YEAR`.

Examples
```sql
SELECT device, count(*)
FROM devices
WHERE cpu_usage > 99
EMIT STREAM PERIODIC INTERVAL 5 SECOND;
```

Like in `Streaming Tail`, Proton continuously monitors new events in table `devices`, does the filtering and then
continuously does **incremental** count aggregation. Whenever the specified delay interval is up, project the current aggregation result
to clients.

Please note everytime it fires, it projects a `snapshot` of the aggregation instead of a `delta` of the aggregation
comparing to last projection.

## Tumble Streaming Window Aggregation

```sql
SELECT <aggr-function>
FROM TUMBLE(<table>, [<timestamp-column>], <tumble-window-size>, [<time-zone>])
[WHERE clause]
GROUP BY <wstart | wend>, ...
EMIT STREAM <window-emit-policy>
...
```

Tumble window means a fixed non-overlapped time window. Here is one example for a 5 seconds tumble window:

```
["2020-01-01 00:00:00", "2020-01-01 00:00:05]
["2020-01-01 00:00:05", "2020-01-01 00:00:10]
["2020-01-01 00:00:10", "2020-01-01 00:00:15]
...
```

`TUMBLE` window in Proton is left closed and right open `[)` meaning it includes all events which have timestamps
**greater or equal** to the **lower bound** of the window, but **less** than the **upper bound** of the window.

`TUMBLE` in the above SQL spec is a table function whose core responsibility is assigning tumble window to each event in
a streaming way. The `TUMBLE` table function will generate 2 new columns: `wstart, wend` which corresponds to the low and high
bounds of a tumble window.

`TUMBLE` table function accepts 4 parameters: `<timestamp-column>` and `<time-zone>` are optional, the others are mandatory.

When `<timestamp-column>` parameter is omitted from the query, the table's default time column which is `_time` will be used.

When `<time-zone>` parameter is omitted the system's default timezone will be used. `<time-zone>` is a string type parameter,
for example `UTC`.

`<tumble-windows-size>` is an interval parameter: `INTERVAL <n> <UNIT>` where `<UNIT>` supports
`SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `QUARTER`, `YEAR`.

`EMIT STREAM` clause has the same semantic as other streaming queries.

`<window-emit-policy>` supports 2 polices for now
1. `AFTER WATERMARK`: aggregation results will be emitted and pushed to clients right after watermark is observed. This is the default behavior when this clause is omitted.
2. `AFTER WATERMARK AND DELAY <internval>`: aggregation results will be held after watermark is observed until the specified delay reaches.

Note **watermark** is an internal timestamp which is observed and calculated and emitted by Proton
and is used to indicate when a streaming window shall close. It is guaranteed to be increased monotonically per stream query.

Examples

```sql
SELECT device, max(cpu_usage)
FROM tumble(devices, INTERVAL 5 SECOND)
GROUP BY device, wend
EMIT STREAM AFTER WATERMARK;
```

The above example SQL continuously aggregates max cpu usage per device per tumble window for table `devices`. Every time a window
is closed, Proton emits the aggregation results.

Note the emitted result is a `snapshot` of the aggregation instead of a `delta` as the global streaming aggregation does.

```sql
SELECT device, max(cpu_usage)
FROM tumble(devices, INTERVAL 5 SECOND)
GROUP BY device, wend
EMIT STREAM AFTER WATERMARK DELAY INTERVAL 2 SECOND;
```

The above example SQL continuously aggregates max cpu usage per device per tumble window for table `devices`. Every time a window
is closed, Proton waits for another 2 seconds and then emits the aggregation results.

```sql
SELECT device, max(cpu_usage)
FROM tumble(devices, timestamp, INTERVAL 5 SECOND)
GROUP BY device, wend
EMIT STREAM AFTER WATERMARK DELAY INTERVAL 2 SECOND;
```

Same as the above delayed tumble window aggregation, except in this query, user specifies a specific time column `timestamp` for stream windowing.

## Hop Streaming Window Aggregation

```sql
SELECT <aggr-function>
FROM HOP(<table>, [<timestamp-column>], <hop-slide-size>, [hop-windows-size], [<time-zone>])
[WHERE clause]
GROUP BY <wstart | wend>, ...
EMIT STREAM <window-emit-policy>
...
```

Hop window is a more generalized window comparing to tumble window. Hop window has an additional
parameter called `<hop-slide-size>` which means window progress this slide size every time. There are 3 cases:

1. `<hop-slide-size>` is less than `<hop-window-size>`. Hop windows have overlaps meaning an event can fall into several hop windows.
2. `<hop-slide-size>` is equal to `<hop-window-size>`. Degenerated to a tumble window.
3. `<hop-slide-size>` is greater than `<hop-window-size>`. Windows has gap in between. Usually not useful, hence not supported so far.

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
FROM hop(devices, INTERVAL 2 SECOND, INTERVAL 5 SECOND)
GROUP BY device, wend
EMIT STREAM AFTER WATERMARK;
```

The above example SQL continuously aggregates max cpu usage per device per hop window for table `devices`. Every time a window
is closed, Proton emits the aggregation results.

Note like other streaming queries, the emitted result is a `snapshot` of the aggregation instead of a `delta`.
