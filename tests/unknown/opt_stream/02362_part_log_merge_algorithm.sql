CREATE STREAM data_horizontal (
    key int
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO data_horizontal VALUES (1);
OPTIMIZE STREAM data_horizontal FINAL;
SYSTEM FLUSH LOGS;
SELECT stream, part_name, event_type, merge_algorithm FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND stream = 'data_horizontal' ORDER BY event_time_microseconds;

CREATE STREAM data_vertical
(
    key uint64,
    value string
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS index_granularity_bytes = 0, enable_mixed_granularity_parts = 0, min_bytes_for_wide_part = 0,
vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO data_vertical VALUES (1, '1');
INSERT INTO data_vertical VALUES (2, '2');
OPTIMIZE STREAM data_vertical FINAL;
SYSTEM FLUSH LOGS;
SELECT stream, part_name, event_type, merge_algorithm FROM system.part_log WHERE event_date >= yesterday() AND database = currentDatabase() AND stream = 'data_vertical' ORDER BY event_time_microseconds;
