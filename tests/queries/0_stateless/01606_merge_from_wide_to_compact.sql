DROP STREAM IF EXISTS wide_to_comp;

create stream wide_to_comp (a int, b int, c int)
    ENGINE = MergeTree ORDER BY a
    settings vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0;

SYSTEM STOP merges wide_to_comp;

INSERT INTO wide_to_comp SELECT number, number, number FROM numbers(100000);
INSERT INTO wide_to_comp SELECT number, number, number FROM numbers(100000);
INSERT INTO wide_to_comp SELECT number, number, number FROM numbers(100000);

SELECT name, part_type FROM system.parts WHERE table = 'wide_to_comp' AND database = currentDatabase() AND active ORDER BY name;

ALTER STREAM wide_to_comp MODIFY setting min_rows_for_wide_part = 10000000;
SYSTEM START merges wide_to_comp;
OPTIMIZE STREAM wide_to_comp FINAL;

SELECT name, part_type FROM system.parts WHERE table = 'wide_to_comp' AND database = currentDatabase() AND active ORDER BY name;
SELECT count() FROM wide_to_comp WHERE not ignore(*);

SYSTEM STOP merges wide_to_comp;
INSERT INTO wide_to_comp SELECT number, number, number FROM numbers(100000);

SELECT name, part_type FROM system.parts WHERE table = 'wide_to_comp' AND database = currentDatabase() AND active ORDER BY name;

ALTER STREAM wide_to_comp MODIFY setting min_rows_for_wide_part = 10000000;
SYSTEM START merges wide_to_comp;
OPTIMIZE STREAM wide_to_comp FINAL;

SELECT name, part_type FROM system.parts WHERE table = 'wide_to_comp' AND database = currentDatabase() AND active ORDER BY name;
SELECT count() FROM wide_to_comp WHERE not ignore(*);

DROP STREAM wide_to_comp;
