DROP STREAM IF EXISTS 99005_stream;
CREATE STREAM 99005_stream(i int, v int);

EXPLAIN SYNTAX select count() over (partition by i) as x, lag(x) from changelog(99005_stream, i);

DROP STREAM 99005_stream;
