DROP STREAM IF EXISTS t_move_to_prewhere;

create stream t_move_to_prewhere (id uint32, a uint8, b uint8, c uint8, fat_string string)
ENGINE = MergeTree ORDER BY id PARTITION BY id
SETTINGS min_rows_for_wide_part = 100, min_bytes_for_wide_part = 0;

INSERT INTO t_move_to_prewhere SELECT 1, number % 2 = 0, number % 3 = 0, number % 5 = 0, repeat('a', 1000) FROM numbers(1000);
INSERT INTO t_move_to_prewhere SELECT 2, number % 2 = 0, number % 3 = 0, number % 5 = 0, repeat('a', 1000) FROM numbers(10);

SELECT partition, part_type FROM system.parts
WHERE table = 't_move_to_prewhere' AND database = currentDatabase()
ORDER BY partition;

SELECT count() FROM t_move_to_prewhere WHERE a AND b AND c AND NOT ignore(fat_string);
EXPLAIN SYNTAX SELECT count() FROM t_move_to_prewhere WHERE a AND b AND c AND NOT ignore(fat_string);

DROP STREAM IF EXISTS t_move_to_prewhere;

-- With only compact parts, we cannot move 3 conditions to PREWHERE,
-- because we don't know sizes and we can use only number of columns in conditions.
-- Sometimes moving a lot of columns to prewhere may be harmful.

create stream t_move_to_prewhere (id uint32, a uint8, b uint8, c uint8, fat_string string)
ENGINE = MergeTree ORDER BY id PARTITION BY id
SETTINGS min_rows_for_wide_part = 10000, min_bytes_for_wide_part = 100000000;

INSERT INTO t_move_to_prewhere SELECT 1, number % 2 = 0, number % 3 = 0, number % 5 = 0, repeat('a', 1000) FROM numbers(1000);
INSERT INTO t_move_to_prewhere SELECT 2, number % 2 = 0, number % 3 = 0, number % 5 = 0, repeat('a', 1000) FROM numbers(10);

SELECT partition, part_type FROM system.parts
WHERE table = 't_move_to_prewhere' AND database = currentDatabase()
ORDER BY partition;

SELECT count() FROM t_move_to_prewhere WHERE a AND b AND c AND NOT ignore(fat_string);
EXPLAIN SYNTAX SELECT count() FROM t_move_to_prewhere WHERE a AND b AND c AND NOT ignore(fat_string);

DROP STREAM IF EXISTS t_move_to_prewhere;
