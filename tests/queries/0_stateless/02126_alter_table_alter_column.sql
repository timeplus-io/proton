DROP STREAM IF EXISTS alter_column_02126;
create stream alter_column_02126 (a int, x int, y int) ENGINE = MergeTree ORDER BY a;
SHOW create stream alter_column_02126;
ALTER STREAM alter_column_02126 ALTER COLUMN x TYPE float32;
SHOW create stream alter_column_02126;
ALTER STREAM alter_column_02126 ALTER COLUMN x TYPE float64, MODIFY COLUMN y float32;
SHOW create stream alter_column_02126;
ALTER STREAM alter_column_02126 MODIFY COLUMN y TYPE float32; -- { clientError 62 }
ALTER STREAM alter_column_02126 ALTER COLUMN y float32; -- { clientError 62 }
