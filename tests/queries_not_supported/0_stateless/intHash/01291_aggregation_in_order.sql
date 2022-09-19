SET query_mode = 'table';
drop stream IF EXISTS pk_order;

SET optimize_aggregation_in_order = 1;

create stream pk_order(a uint64, b uint64, c uint64, d uint64) ENGINE=MergeTree() ORDER BY (a, b);
INSERT INTO pk_order(a, b, c, d) VALUES (1, 1, 101, 1), (1, 2, 102, 1), (1, 3, 103, 1), (1, 4, 104, 1);
INSERT INTO pk_order(a, b, c, d) VALUES (1, 5, 104, 1), (1, 6, 105, 1), (2, 1, 106, 2), (2, 1, 107, 2);
INSERT INTO pk_order(a, b, c, d) VALUES (2, 2, 107, 2), (2, 3, 108, 2), (2, 4, 109, 2);

-- Order after group by in order is determined

SELECT a, b FROM pk_order GROUP BY a, b ORDER BY a, b;
SELECT a FROM pk_order GROUP BY a ORDER BY a;

SELECT a, b, sum(c), avg(d) FROM pk_order GROUP BY a, b ORDER BY a, b;
SELECT a, sum(c), avg(d) FROM pk_order GROUP BY a ORDER BY a;
SELECT a, sum(c), avg(d) FROM pk_order GROUP BY -a ORDER BY a;

drop stream IF EXISTS pk_order;

create stream pk_order (d DateTime, a int32, b int32) ENGINE = MergeTree ORDER BY (d, a)
    PARTITION BY to_date(d) SETTINGS index_granularity=1;

INSERT INTO pk_order
    SELECT to_datetime('2019-05-05 00:00:00') + INTERVAL number % 10 DAY, number, intHash32(number) from numbers(100);

set max_block_size = 1;

SELECT d, max(b) FROM pk_order GROUP BY d, a ORDER BY d, a LIMIT 5;
SELECT d, avg(a) FROM pk_order GROUP BY to_string(d) ORDER BY to_string(d) LIMIT 5;
SELECT toStartOfHour(d) as d1, min(a), max(b) FROM pk_order GROUP BY d1 ORDER BY d1 LIMIT 5;

drop stream pk_order;
