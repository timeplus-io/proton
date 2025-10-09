DROP STREAM IF EXISTS stream_99004;
CREATE STREAM stream_99004 (a int, b int, c int);

SELECT now() as window_start; -- { clientError SYNTAX_ERROR }
SELECT now() as window_end; -- { clientError SYNTAX_ERROR }

EXPLAIN PIPELINE SELECT date_add(window_start, 8h) as window_start, count() FROM tumble(stream_99004, 2s) group by window_start; -- { clientError SYNTAX_ERROR }
EXPLAIN PIPELINE SELECT date_add(window_end, 8h) as window_end, count() FROM tumble(stream_99004, 2s) group by window_end; -- { clientError SYNTAX_ERROR }

DROP STREAM IF EXISTS stream_99004;
