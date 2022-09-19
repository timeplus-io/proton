DROP STREAM IF EXISTS sorted;
create stream sorted (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 8192);

INSERT INTO sorted (x) SELECT int_div(number, 100000) AS x FROM system.numbers LIMIT 1000000;

SET max_threads = 1;

SELECT count() FROM sorted;
SELECT DISTINCT x FROM sorted;

INSERT INTO sorted (x) SELECT (intHash64(number) % 1000 = 0 ? 999 : int_div(number, 100000)) AS x FROM system.numbers LIMIT 1000000;

SELECT count() FROM sorted;
SELECT DISTINCT x FROM sorted;

DROP STREAM sorted;
