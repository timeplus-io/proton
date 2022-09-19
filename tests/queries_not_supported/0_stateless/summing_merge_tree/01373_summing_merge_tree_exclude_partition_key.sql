-- Tags: not_supported, blocked_by_SummingMergeTree

SET optimize_on_insert = 0;

DROP STREAM IF EXISTS tt_01373;

create stream tt_01373
(a int64, d int64, val int64) 
ENGINE = SummingMergeTree PARTITION BY (a) ORDER BY (d);

SYSTEM STOP MERGES tt_01373;

INSERT INTO tt_01373 SELECT number%13, number%17, 1 from numbers(1000000);

SELECT '---';
SELECT count(*) FROM tt_01373;

SELECT '---';
SELECT count(*) FROM tt_01373 FINAL;

SELECT '---';
SELECT a, count() FROM tt_01373 FINAL GROUP BY a ORDER BY a;

SYSTEM START MERGES tt_01373;

OPTIMIZE STREAM tt_01373 FINAL;
SELECT '---';
SELECT a, count() FROM tt_01373 GROUP BY a ORDER BY a;

DROP STREAM IF EXISTS tt_01373;
