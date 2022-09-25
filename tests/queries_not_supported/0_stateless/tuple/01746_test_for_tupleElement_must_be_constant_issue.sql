DROP STREAM IF EXISTS ttt01746;
create stream ttt01746 (d date, n uint64) ENGINE = MergeTree() PARTITION BY to_monday(d) ORDER BY n;
INSERT INTO ttt01746 SELECT to_date('2021-02-14') + (number % 30) AS d, number AS n FROM numbers(1500000);
set optimize_move_to_prewhere=0;
SELECT arraySort(x -> x.2, [tuple('a', 10)]) AS X FROM ttt01746 WHERE d >= to_date('2021-03-03') - 2 ORDER BY n LIMIT 1;
SELECT arraySort(x -> x.2, [tuple('a', 10)]) AS X FROM ttt01746 PREWHERE d >= to_date('2021-03-03') - 2 ORDER BY n LIMIT 1;
DROP STREAM ttt01746;
