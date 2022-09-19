DROP STREAM IF EXISTS pk_order;

SET optimize_read_in_order = 1;

create stream pk_order(a uint64, b uint64, c uint64, d uint64) ENGINE=MergeTree() ORDER BY (a, b);
INSERT INTO pk_order(a, b, c, d) VALUES (1, 1, 101, 1), (1, 2, 102, 1), (1, 3, 103, 1), (1, 4, 104, 1);
INSERT INTO pk_order(a, b, c, d) VALUES (1, 5, 104, 1), (1, 6, 105, 1), (2, 1, 106, 2), (2, 1, 107, 2);

INSERT INTO pk_order(a, b, c, d) VALUES (2, 2, 107, 2), (2, 3, 108, 2), (2, 4, 109, 2);

SELECT b FROM pk_order ORDER BY a, b;
SELECT a FROM pk_order ORDER BY a, b;

SELECT a, b FROM pk_order ORDER BY a, b;
SELECT a, b FROM pk_order ORDER BY a DESC, b;
SELECT a, b FROM pk_order ORDER BY a, b DESC;
SELECT a, b FROM pk_order ORDER BY a DESC, b DESC;
SELECT a FROM pk_order ORDER BY a DESC;

SELECT a, b, c FROM pk_order ORDER BY a, b, c;
SELECT a, b, c FROM pk_order ORDER BY a DESC, b, c;
SELECT a, b, c FROM pk_order ORDER BY a, b DESC, c;
SELECT a, b, c FROM pk_order ORDER BY a, b, c DESC;
SELECT a, b, c FROM pk_order ORDER BY a DESC, b DESC, c;
SELECT a, b, c FROM pk_order ORDER BY a DESC, b, c DESC;
SELECT a, b, c FROM pk_order ORDER BY a, b DESC, c DESC;
SELECT a, b, c FROM pk_order ORDER BY a DESC, b DESC, c DESC;

DROP STREAM IF EXISTS pk_order;

create stream pk_order (d DateTime, a int32, b int32) ENGINE = MergeTree ORDER BY (d, a)
    PARTITION BY to_date(d) SETTINGS index_granularity=1;

INSERT INTO pk_order
    SELECT to_datetime('2019-05-05 00:00:00') + INTERVAL number % 10 DAY, number, intHash32(number) from numbers(100);

set max_block_size = 1;

-- Currently checking number of read rows while reading in pk order not working precise. TODO: fix it.
-- SET max_rows_to_read = 10;

SELECT d FROM pk_order ORDER BY d LIMIT 5;
SELECT d, b FROM pk_order ORDER BY d, b LIMIT 5;
SELECT d, a FROM pk_order ORDER BY d DESC, a DESC LIMIT 5;
SELECT d, a FROM pk_order ORDER BY d DESC, -a LIMIT 5;
SELECT d, a FROM pk_order ORDER BY d DESC, a DESC LIMIT 5;
SELECT toStartOfHour(d) as d1 FROM pk_order ORDER BY d1 LIMIT 5;

DROP STREAM pk_order;

create stream pk_order (a int, b int) ENGINE = MergeTree ORDER BY (a / b);
INSERT INTO pk_order SELECT number % 10 + 1, number % 6 + 1 from numbers(100);
SELECT * FROM pk_order ORDER BY (a / b), a LIMIT 5;

DROP STREAM pk_order;
