DROP STREAM IF EXISTS numbers_indexed;
DROP STREAM IF EXISTS squares;

CREATE STREAM numbers_indexed Engine=MergeTree ORDER BY number PARTITION BY bit_shift_right(number,8) SETTINGS index_granularity=8 AS SELECT * FROM numbers(16384);

CREATE VIEW squares AS WITH number*2 AS square_number SELECT number, square_number FROM numbers_indexed;

SET max_rows_to_read=8, read_overflow_mode='throw';

WITH number * 2 AS square_number SELECT number, square_number FROM numbers_indexed WHERE number = 999;

SELECT * FROM squares WHERE number = 999;

EXPLAIN SYNTAX SELECT number, square_number FROM ( WITH number * 2 AS square_number SELECT number, square_number FROM numbers_indexed) AS squares WHERE number = 999;

DROP STREAM IF EXISTS squares;
DROP STREAM IF EXISTS numbers_indexed;
