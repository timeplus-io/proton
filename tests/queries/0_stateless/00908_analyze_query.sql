DROP STREAM IF EXISTS a;
create stream a (a uint8, b uint8) ENGINE MergeTree ORDER BY a;

EXPLAIN SYNTAX SELECT * FROM a;

DROP STREAM a;
