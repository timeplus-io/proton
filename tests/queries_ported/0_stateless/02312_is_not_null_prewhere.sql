DROP STREAM IF EXISTS bug_36995;

CREATE STREAM bug_36995(
    `time` DateTime,
    `product` string)
ENGINE = MergeTree
ORDER BY time AS
SELECT '2022-01-01 00:00:00','1';

SELECT * FROM bug_36995
WHERE (time IS NOT NULL) AND (product IN (SELECT '1'))
SETTINGS optimize_move_to_prewhere = 1;

SELECT * FROM bug_36995
WHERE (time IS NOT NULL) AND (product IN (SELECT '1'))
SETTINGS optimize_move_to_prewhere = 0;

SELECT * FROM bug_36995
PREWHERE (time IS NOT NULL) WHERE (product IN (SELECT '1'));

DROP STREAM bug_36995;
