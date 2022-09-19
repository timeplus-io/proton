DROP STREAM IF EXISTS alter_column_02126;
create stream alter_column_02126 (a int, x int, y int) ENGINE = MergeTree ORDER BY a;
SHOW create stream alter_column_02126;
ALTER STREAM alter_column_02126 ALTER COLUMN x TYPE Float32;
SHOW create stream alter_column_02126;
ALTER STREAM alter_column_02126 ALTER COLUMN x TYPE float64, MODIFY COLUMN y Float32;
SHOW create stream alter_column_02126;
ALTER STREAM alter_column_02126 MODIFY COLUMN y TYPE Float32; -- { clientError 62 }
ALTER STREAM alter_column_02126 ALTER COLUMN y Float32; -- { clientError 62 }
