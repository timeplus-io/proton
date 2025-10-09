DROP STREAM IF EXISTS table_in_memory;

CREATE STREAM table_in_memory
(
    `id` uint64,
    `value` uint64
)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS min_bytes_for_wide_part=1000, min_bytes_for_compact_part=900;

SELECT 'init state';
INSERT INTO table_in_memory SELECT int_div(number, 10), number FROM numbers(30);

SELECT count() FROM table_in_memory;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory' AND database=current_database();

SELECT 'drop part 0';
ALTER STREAM table_in_memory DROP PARTITION 0;

SELECT count() FROM table_in_memory;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory' AND database=current_database() AND active;

SELECT 'detach table';
DETACH STREAM table_in_memory;

SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory' AND database=current_database();

SELECT 'attach table';
ATTACH STREAM table_in_memory;

SELECT count() FROM table_in_memory;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory' AND database=current_database() and active;
