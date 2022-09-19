DROP STREAM IF EXISTS mt;
create stream mt (x uint8, y date) ENGINE = MergeTree ORDER BY x;

SELECT count()
FROM mt
ANY LEFT JOIN
(
    SELECT 1 AS x
) js2 USING (x)
PREWHERE x IN (1) WHERE y = today();

DROP STREAM mt;
