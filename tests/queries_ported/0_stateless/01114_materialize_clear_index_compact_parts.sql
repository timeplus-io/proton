DROP STREAM IF EXISTS minmax_compact;

CREATE STREAM minmax_compact
(
    u64 uint64,
    i64 int64,
    i32 int32
)
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', min_rows_for_wide_part = 1000000;

select sleep(2) FORMAT Null;

INSERT INTO minmax_compact(u64, i64, i32) VALUES (0, 2, 1), (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1), (5, 2, 1), (6, 1, 2), (7, 1, 2), (8, 1, 2), (9, 1, 2);

select sleep(2) FORMAT Null;

SET mutations_sync = 1;
ALTER STREAM minmax_compact ADD INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1;

ALTER STREAM minmax_compact MATERIALIZE INDEX idx IN PARTITION 1;
select sleep(2) FORMAT Null;

set max_rows_to_read = 8;
SELECT count() FROM table(minmax_compact) WHERE i64 = 2;

ALTER STREAM minmax_compact MATERIALIZE INDEX idx IN PARTITION 2;
select sleep(2) FORMAT Null;

set max_rows_to_read = 6;
SELECT count() FROM table(minmax_compact) WHERE i64 = 2;

ALTER STREAM minmax_compact CLEAR INDEX idx IN PARTITION 1;
select sleep(1) FORMAT Null;

ALTER STREAM minmax_compact CLEAR INDEX idx IN PARTITION 2;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;


SELECT count() FROM table(minmax_compact) WHERE i64 = 2; -- { serverError TOO_MANY_ROWS }

set max_rows_to_read = 10;
SELECT count() FROM table(minmax_compact) WHERE i64 = 2;

DROP STREAM minmax_compact;
