-- Tags: no-parallel
SET system_events_show_zero_values = 1;

CREATE TEMPORARY STREAM t (x uint64) ENGINE = Memory();
INSERT INTO t SELECT value FROM system.events WHERE event = 'OverflowBreak';
SELECT count() FROM system.numbers FORMAT Null SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break';
INSERT INTO t SELECT value FROM system.events WHERE event = 'OverflowBreak';
SELECT max(x) - min(x) FROM t;

TRUNCATE STREAM t;
INSERT INTO t SELECT value FROM system.events WHERE event = 'OverflowThrow';
SELECT count() FROM system.numbers SETTINGS max_rows_to_read = 1, read_overflow_mode = 'throw'; -- { serverError 158 }
INSERT INTO t SELECT value FROM system.events WHERE event = 'OverflowThrow';
SELECT max(x) - min(x) FROM t;

TRUNCATE STREAM t;
INSERT INTO t SELECT value FROM system.events WHERE event = 'OverflowAny';
SELECT number, count() FROM numbers(100000) GROUP BY number FORMAT Null SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any';
INSERT INTO t SELECT value FROM system.events WHERE event = 'OverflowAny';
SELECT max(x) - min(x) FROM t;
