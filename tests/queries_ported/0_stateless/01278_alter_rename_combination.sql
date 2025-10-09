SET query_mode='table';

DROP STREAM IF EXISTS rename_table;

CREATE STREAM rename_table (key int32, value1 int32, value2 int32) ORDER BY tuple_cast() SETTINGS min_bytes_for_wide_part=0;
select sleep(2) FORMAT Null;

INSERT INTO rename_table(key, value1, value2) VALUES (1, 2, 3);
select sleep(2) FORMAT Null;
-- replace one with other
ALTER STREAM rename_table RENAME COLUMN value1 TO old_value1;
select sleep(2) FORMAT Null;
ALTER STREAM rename_table RENAME COLUMN value2 TO value1;
select sleep(2) FORMAT Null;

SHOW CREATE STREAM rename_table;

SELECT (* except _tp_time) FROM rename_table FORMAT TSVWithNames;

INSERT INTO rename_table(key, old_value1, value1) VALUES (4, 5, 6);
select sleep(2) FORMAT Null;

-- rename all columns simultaneously
ALTER STREAM rename_table RENAME COLUMN old_value1 TO v1;
select sleep(2) FORMAT Null;
ALTER STREAM rename_table RENAME COLUMN value1 TO v2;
select sleep(2) FORMAT Null;
ALTER STREAM rename_table RENAME COLUMN key to k;
select sleep(2) FORMAT Null;

SHOW CREATE STREAM rename_table;

SELECT (* except _tp_time) FROM rename_table ORDER BY k FORMAT TSVWithNames;

DROP STREAM IF EXISTS rename_table;

SELECT '---polymorphic---';

DROP STREAM IF EXISTS rename_table_polymorphic;

CREATE STREAM rename_table_polymorphic (
  key int32,
  value1 int32,
  value2 int32
)
ORDER BY tuple_cast()
SETTINGS min_rows_for_wide_part = 10000;
select sleep(2) FORMAT Null;

INSERT INTO rename_table_polymorphic(key, value1, value2) VALUES (1, 2, 3);
select sleep(2) FORMAT Null;

ALTER STREAM rename_table_polymorphic RENAME COLUMN value1 TO old_value1;
select sleep(2) FORMAT Null;
ALTER STREAM rename_table_polymorphic RENAME COLUMN value2 TO value1;
select sleep(2) FORMAT Null;

SHOW CREATE STREAM rename_table_polymorphic;

SELECT (* except _tp_time) FROM rename_table_polymorphic FORMAT TSVWithNames;

INSERT INTO rename_table_polymorphic(key, old_value1, value1) VALUES (4, 5, 6);
select sleep(2) FORMAT Null;

-- rename all columns simultaneously
ALTER STREAM rename_table_polymorphic RENAME COLUMN old_value1 TO v1;
select sleep(2) FORMAT Null;
ALTER STREAM rename_table_polymorphic RENAME COLUMN value1 TO v2;
select sleep(2) FORMAT Null;
ALTER STREAM rename_table_polymorphic RENAME COLUMN key to k;
select sleep(2) FORMAT Null;

SHOW CREATE STREAM rename_table_polymorphic;

SELECT (* except _tp_time) FROM rename_table_polymorphic ORDER BY k FORMAT TSVWithNames;

DROP STREAM IF EXISTS rename_table_polymorphic;
