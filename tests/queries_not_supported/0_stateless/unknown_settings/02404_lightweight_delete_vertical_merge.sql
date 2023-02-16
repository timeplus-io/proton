DROP STREAM IF EXISTS lwd_test;

CREATE STREAM lwd_test
(
    `id` uint64,
    `value` string
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 1;


INSERT INTO lwd_test SELECT number AS id, to_string(number) AS value FROM numbers(10);

SELECT * FROM lwd_test ORDER BY id, value;

SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;



SET mutations_sync = 0;
SET allow_experimental_lightweight_delete = 1;

-- delete some rows using LWD
DELETE FROM lwd_test WHERE (id % 3) = 0;


SELECT * FROM lwd_test ORDER BY id, value;


SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;


-- optimize table to physically delete the rows
OPTIMIZE TABLE lwd_test FINAL SETTINGS mutations_sync = 2;

SELECT * FROM lwd_test ORDER BY id, value;


SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;


-- delete more rows
DELETE FROM lwd_test WHERE (id % 2) = 0;


SELECT * FROM lwd_test ORDER BY id, value;


SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;


-- add another part that doesn't have deleted rows
INSERT INTO lwd_test SELECT number AS id, to_string(number+100) AS value FROM numbers(10);

SELECT * FROM lwd_test ORDER BY id, value;

SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;


-- optimize table to merge 2 parts together: the 1st has LDW rows and the 2nd doesn't have LWD rows
-- physically delete the rows
OPTIMIZE TABLE lwd_test FINAL SETTINGS mutations_sync = 2;

SELECT * FROM lwd_test ORDER BY id, value;


SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;


-- add another part that doesn't have deleted rows
INSERT INTO lwd_test SELECT number AS id, to_string(number+200) AS value FROM numbers(10);

SELECT * FROM lwd_test ORDER BY id, value;

SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;


-- delete more rows
DELETE FROM lwd_test WHERE (id % 3) = 2;


SELECT * FROM lwd_test ORDER BY id, value;


SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;


-- optimize table to merge 2 parts together, both of them have LWD rows
-- physically delete the rows
OPTIMIZE TABLE lwd_test FINAL SETTINGS mutations_sync = 2;

SELECT * FROM lwd_test ORDER BY id, value;


SELECT name, part_type
FROM system.parts
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name;


SELECT name, column, type, rows
FROM system.parts_columns
WHERE (database = current_database()) AND (table = 'lwd_test') AND active
ORDER BY name, column;


DROP STREAM lwd_test;
