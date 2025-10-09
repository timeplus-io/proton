DROP STREAM IF EXISTS 99005_stream;
CREATE STREAM 99005_stream(i int, v int);

EXPLAIN SYNTAX select count() as x, lag(x) from changelog(99005_stream, i) partition by i;

DROP STREAM 99005_stream;
