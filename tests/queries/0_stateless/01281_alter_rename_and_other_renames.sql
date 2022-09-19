DROP STREAM IF EXISTS rename_table_multiple;

create stream rename_table_multiple (key int32, value1 string, value2 int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO rename_table_multiple VALUES (1, 2, 3);

ALTER STREAM rename_table_multiple RENAME COLUMN value1 TO value1_string, MODIFY COLUMN value1_string string; --{serverError 48}
ALTER STREAM rename_table_multiple MODIFY COLUMN value1 string, RENAME COLUMN value1 to value1_string; --{serverError 48}

ALTER STREAM rename_table_multiple RENAME COLUMN value1 TO value1_string;
ALTER STREAM rename_table_multiple MODIFY COLUMN value1_string string;

SHOW create stream rename_table_multiple;

SELECT * FROM rename_table_multiple FORMAT TSVWithNames;

INSERT INTO rename_table_multiple VALUES (4, '5', 6);

ALTER STREAM rename_table_multiple RENAME COLUMN value2 TO value2_old, ADD COLUMN value2 int64 DEFAULT 7;

SHOW create stream rename_table_multiple;

SELECT * FROM rename_table_multiple ORDER BY key FORMAT TSVWithNames;

INSERT INTO rename_table_multiple VALUES (7, '8', 9, 10);

ALTER STREAM rename_table_multiple DROP COLUMN value2_old, RENAME COLUMN value2 TO value2_old;

SHOW create stream rename_table_multiple;

SELECT * FROM rename_table_multiple ORDER BY key FORMAT TSVWithNames;

DROP STREAM IF EXISTS rename_table_multiple;

DROP STREAM IF EXISTS rename_table_multiple_compact;

create stream rename_table_multiple_compact (key int32, value1 string, value2 int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_rows_for_wide_part = 100000;

INSERT INTO rename_table_multiple_compact VALUES (1, 2, 3);

ALTER STREAM rename_table_multiple_compact RENAME COLUMN value1 TO value1_string, MODIFY COLUMN value1_string string; --{serverError 48}
ALTER STREAM rename_table_multiple_compact MODIFY COLUMN value1 string, RENAME COLUMN value1 to value1_string; --{serverError 48}

ALTER STREAM rename_table_multiple_compact RENAME COLUMN value1 TO value1_string;
ALTER STREAM rename_table_multiple_compact MODIFY COLUMN value1_string string;

SHOW create stream rename_table_multiple_compact;

SELECT * FROM rename_table_multiple_compact FORMAT TSVWithNames;

INSERT INTO rename_table_multiple_compact VALUES (4, '5', 6);

ALTER STREAM rename_table_multiple_compact RENAME COLUMN value2 TO value2_old, ADD COLUMN value2 int64 DEFAULT 7;

SHOW create stream rename_table_multiple_compact;

SELECT * FROM rename_table_multiple_compact ORDER BY key FORMAT TSVWithNames;

INSERT INTO rename_table_multiple_compact VALUES (7, '8', 9, 10);

ALTER STREAM rename_table_multiple_compact DROP COLUMN value2_old, RENAME COLUMN value2 TO value2_old;

SHOW create stream rename_table_multiple_compact;

SELECT * FROM rename_table_multiple_compact ORDER BY key FORMAT TSVWithNames;

DROP STREAM IF EXISTS rename_table_multiple_compact;
