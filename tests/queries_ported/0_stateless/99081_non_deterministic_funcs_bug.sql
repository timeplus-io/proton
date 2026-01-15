DROP STREAM IF EXISTS 99081_kv;

CREATE STREAM 99081_kv(i int, v string) primary key i settings mode='versioned_kv';

select ts, ts2, v from (select now() as ts, i, v from 99081_kv) as kv1 join (select now64(3) as ts2, i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0; -- { serverError UNSUPPORTED }
select ts, ts2, v from (select now() as ts, i, v from 99081_kv) as kv1 join (select now64(3) as ts2, i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0 settings query_mode='table';

--- `now() as ts` and `now64(3) as ts2` are not used
select i, v from (select now() as ts, i, v from 99081_kv) as kv1 join (select now64(3) as ts2, i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0;

select now() as ts, now64(3) as ts2, v, _tp_delta from (select i, v from 99081_kv) as kv1 join (select i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0 emit changelog;
select now() as ts, now64(3) as ts2, v from (select i, v from 99081_kv) as kv1 join (select i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0;

--- https://github.com/timeplus-io/proton-enterprise/issues/11122: streaming now() predicate should not be pushed down
SELECT '======== non-deterministic predicate expressions should not be pushed down ========';

--- Case-1: subquery
SELECT
    sum(explain ILIKE '%Filter%') AS filter_count
FROM (
    EXPLAIN PLAN
    SELECT v
    FROM (SELECT * FROM 99081_kv) AS s
    WHERE now() > to_datetime('1970-01-01 00:00:01') --- now()
);

SELECT
    sum(explain ILIKE '%Filter%') AS filter_count
FROM (
    EXPLAIN PLAN
    SELECT v
    FROM (SELECT * FROM 99081_kv) AS s
    WHERE now64(3) > to_datetime64('1970-01-01 00:00:01.000', 3) --- now64(3)
);

--- Case-2: join
SELECT
    sum(explain ILIKE '%Filter%') AS filter_count
FROM (
    EXPLAIN PLAN
    SELECT kv1.v
    FROM 99081_kv AS kv1
    JOIN (SELECT i FROM 99081_kv) AS kv2 ON kv1.i = kv2.i
    WHERE now() > to_datetime('1970-01-01 00:00:01') --- now()
);

SELECT
    sum(explain ILIKE '%Filter%') AS filter_count
FROM (
    EXPLAIN PLAN
    SELECT kv1.v
    FROM 99081_kv AS kv1
    JOIN (SELECT i FROM 99081_kv) AS kv2 ON kv1.i = kv2.i
    WHERE now64(3) > to_datetime64('1970-01-01 00:00:01.000', 3) --- now64(3)
);

DROP STREAM IF EXISTS 99081_kv;
