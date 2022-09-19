-- Tags: not_supported, blocked_by_SummingMergeTree

DROP STREAM IF EXISTS test_table;
DROP STREAM IF EXISTS numbers;
DROP STREAM IF EXISTS test_mv;
DROP STREAM IF EXISTS src;
DROP STREAM IF EXISTS dst;
DROP STREAM IF EXISTS mv;
DROP STREAM IF EXISTS dist;

create stream test_table (key uint32, value Decimal(16, 6)) ENGINE = SummingMergeTree() ORDER BY key;
create stream numbers (number uint64) ENGINE=Memory;

CREATE MATERIALIZED VIEW test_mv TO test_table (number uint64, value Decimal(38, 6))
AS SELECT number, sum(number) AS value FROM (SELECT *, to_decimal64(number, 6) AS val FROM numbers) GROUP BY number;

INSERT INTO numbers SELECT * FROM numbers(100000);

SELECT sum(value) FROM test_mv;
SELECT sum(value) FROM (SELECT number, sum(number) AS value FROM (SELECT *, to_decimal64(number, 6) AS val FROM numbers) GROUP BY number);

create stream src (n uint64, s FixedString(16)) ENGINE=Memory;
create stream dst (n uint8, s string) ;
CREATE MATERIALIZED VIEW mv TO dst (n string) AS SELECT * FROM src;
SET allow_experimental_bigint_types=1;
create stream dist (n Int128) ENGINE=Distributed(test_cluster_two_shards, currentDatabase(), mv);

INSERT INTO src SELECT number, to_string(number) FROM numbers(1000);
INSERT INTO mv SELECT to_string(number + 1000) FROM numbers(1000); -- { serverError 53 }
INSERT INTO mv SELECT array_join(['42', 'test']); -- { serverError 53 }

SELECT count(), sum(n), sum(to_int64(s)), max(n), min(n) FROM src;
SELECT count(), sum(n), sum(to_int64(s)), max(n), min(n) FROM dst;
SELECT count(), sum(to_int64(n)), max(n), min(n) FROM mv;
SELECT count(), sum(to_int64(n)), max(n), min(n) FROM dist; -- { serverError 70 }
SELECT count(), sum(to_int64(n)), max(to_uint32(n)), min(to_int128(n)) FROM dist;

DROP STREAM test_table;
DROP STREAM numbers;
DROP STREAM test_mv;
DROP STREAM src;
DROP STREAM dst;
DROP STREAM mv;
DROP STREAM dist;
