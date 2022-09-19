SELECT 'TTL WHERE';
DROP STREAM IF EXISTS ttl_where;

create stream ttl_where
(
    `d` date,
    `i` uint32
)
ENGINE = MergeTree
ORDER BY tuple()
TTL d + toIntervalYear(10) DELETE WHERE i % 3 = 0,
    d + toIntervalYear(40) DELETE WHERE i % 3 = 1;

-- This test will fail at 2040-10-10

INSERT INTO ttl_where SELECT to_date('2000-10-10'), number FROM numbers(10);
INSERT INTO ttl_where SELECT to_date('1970-10-10'), number FROM numbers(10);
OPTIMIZE STREAM ttl_where FINAL;

SELECT * FROM ttl_where ORDER BY d, i;

DROP STREAM ttl_where;

SELECT 'TTL GROUP BY';
DROP STREAM IF EXISTS ttl_group_by;

create stream ttl_group_by
(
    `d` date,
    `i` uint32,
    `v` uint64
)
ENGINE = MergeTree
ORDER BY (to_start_of_month(d), i % 10)
TTL d + toIntervalYear(10) GROUP BY to_start_of_month(d), i % 10 SET d = any(to_start_of_month(d)), i = any(i % 10), v = sum(v),
    d + toIntervalYear(40) GROUP BY to_start_of_month(d) SET d = any(to_start_of_month(d)), v = sum(v);

INSERT INTO ttl_group_by SELECT to_date('2000-10-10'), number, number FROM numbers(100);
INSERT INTO ttl_group_by SELECT to_date('1970-10-10'), number, number FROM numbers(100);
OPTIMIZE STREAM ttl_group_by FINAL;

SELECT * FROM ttl_group_by ORDER BY d, i;

DROP STREAM ttl_group_by;
