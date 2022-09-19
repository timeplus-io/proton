DROP STREAM IF EXISTS prewhere;

create stream prewhere (light uint8, heavy string) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere SELECT 0, randomPrintableASCII(10000) FROM numbers(10000);
SELECT array_join([light]) != 0 AS cond, length(heavy) FROM prewhere WHERE light != 0 AND cond != 0;

DROP STREAM prewhere;
