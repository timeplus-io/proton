DROP STREAM IF EXISTS prewhere;
create stream prewhere (x array(uint64), y ALIAS x, s string) ENGINE = MergeTree ORDER BY tuple();
SELECT count() FROM prewhere PREWHERE (length(s) >= 1) = 0 WHERE NOT ignore(y);
DROP STREAM prewhere;
