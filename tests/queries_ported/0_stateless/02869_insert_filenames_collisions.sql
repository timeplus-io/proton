DROP STREAM IF EXISTS t_collisions;

SELECT lower(hex(reverse(cast(sip_hash128('very_very_long_column_name_that_will_be_replaced_with_hash'), 'fixed_string(16)'))));

CREATE STREAM t_collisions
(
    `very_very_long_column_name_that_will_be_replaced_with_hash` int32,
    `e798545eefc8b7a1c2c81ff00c064ad8` int32
)
ENGINE = MergeTree
ORDER BY tuple_cast()
SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 42; -- { serverError BAD_ARGUMENTS }

DROP STREAM IF EXISTS t_collisions;

CREATE STREAM t_collisions
(
    `col1` int32,
    `e798545eefc8b7a1c2c81ff00c064ad8` int32
)
ENGINE = MergeTree
ORDER BY tuple_cast()
SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 42;

ALTER STREAM t_collisions ADD COLUMN very_very_long_column_name_that_will_be_replaced_with_hash int32;  -- { serverError BAD_ARGUMENTS }
ALTER STREAM t_collisions RENAME COLUMN col1 TO very_very_long_column_name_that_will_be_replaced_with_hash;  -- { serverError BAD_ARGUMENTS }

DROP STREAM IF EXISTS t_collisions;

CREATE STREAM t_collisions
(
    `very_very_long_column_name_that_will_be_replaced_with_hash` int32,
    `e798545eefc8b7a1c2c81ff00c064ad8` int32
)
ENGINE = MergeTree
ORDER BY tuple_cast()
SETTINGS replace_long_file_name_to_hash = 0;

INSERT INTO t_collisions VALUES (1, 1);

ALTER STREAM t_collisions MODIFY SETTING replace_long_file_name_to_hash = 1, max_file_name_length = 42; -- { serverError BAD_ARGUMENTS }

INSERT INTO t_collisions VALUES (2, 2);

SELECT * FROM t_collisions ORDER BY e798545eefc8b7a1c2c81ff00c064ad8;

DROP STREAM IF EXISTS t_collisions;

CREATE STREAM t_collisions
(
    `id` int,
    `col` array(string),
    `col.s` array(low_cardinality(string)),
    `col.u` array(low_cardinality(string))
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP STREAM IF EXISTS t_collisions;

CREATE STREAM t_collisions
(
    `id` int,
    `col` string,
    `col.s` array(low_cardinality(string)),
    `col.u` array(low_cardinality(string))
)
ENGINE = MergeTree
ORDER BY id;

ALTER STREAM t_collisions MODIFY COLUMN col array(string); -- { serverError BAD_ARGUMENTS }

DROP STREAM IF EXISTS t_collisions;
