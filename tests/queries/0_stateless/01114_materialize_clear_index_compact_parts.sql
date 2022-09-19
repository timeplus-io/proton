DROP STREAM IF EXISTS minmax_compact;

create stream minmax_compact
(
    u64 uint64,
    i64 int64,
    i32 int32
) ENGINE = MergeTree()
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2, min_rows_for_wide_part = 1000000;

INSERT INTO minmax_compact VALUES (0, 2, 1), (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1), (5, 2, 1), (6, 1, 2), (7, 1, 2), (8, 1, 2), (9, 1, 2);

SET mutations_sync = 1;
ALTER STREAM minmax_compact ADD INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1;

ALTER STREAM minmax_compact MATERIALIZE INDEX idx IN PARTITION 1;
set max_rows_to_read = 8;
SELECT count() FROM minmax_compact WHERE i64 = 2;

ALTER STREAM minmax_compact MATERIALIZE INDEX idx IN PARTITION 2;
set max_rows_to_read = 6;
SELECT count() FROM minmax_compact WHERE i64 = 2;

ALTER STREAM minmax_compact CLEAR INDEX idx IN PARTITION 1;
ALTER STREAM minmax_compact CLEAR INDEX idx IN PARTITION 2;

SELECT count() FROM minmax_compact WHERE i64 = 2; -- { serverError 158 }

set max_rows_to_read = 10;
SELECT count() FROM minmax_compact WHERE i64 = 2;

DROP STREAM minmax_compact;
