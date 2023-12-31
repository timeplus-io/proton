DROP STREAM IF EXISTS t;

CREATE STREAM t (`key` uint32, `created_at` Date, `value` uint32, PROJECTION xxx (SELECT key, created_at, sum(value) GROUP BY key, created_at)) ENGINE = MergeTree PARTITION BY to_YYYYMM(created_at) ORDER BY key;

INSERT INTO t SELECT 1 AS key, today() + (number % 30), number FROM numbers(1000);

ALTER STREAM t UPDATE value = 0 WHERE (value > 0) AND (created_at >= '2021-12-21') SETTINGS allow_experimental_projection_optimization = 1;

DROP STREAM IF EXISTS t;
