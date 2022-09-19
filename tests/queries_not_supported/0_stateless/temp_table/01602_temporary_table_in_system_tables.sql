DROP TEMPORARY STREAM IF EXISTS test_01602a;
DROP TEMPORARY STREAM IF EXISTS test_01602b;

CREATE TEMPORARY STREAM test_01602a(x uint32);
CREATE TEMPORARY STREAM test_01602b(y float64, z string);

SELECT database, name, create_table_query, engine, engine_full, is_temporary FROM system.tables WHERE name LIKE 'test_01602%' ORDER BY name;
SELECT * FROM system.columns WHERE table LIKE 'test_01602%' ORDER BY table, name;

SHOW CREATE TEMPORARY STREAM test_01602a;
SHOW CREATE TEMPORARY STREAM test_01602b;

SELECT COUNT() FROM system.databases WHERE name='_temporary_and_external_tables';
SELECT COUNT() FROM system.tables WHERE database='_temporary_and_external_tables';
SELECT COUNT() FROM system.columns WHERE database='_temporary_and_external_tables';

DROP TEMPORARY TABLE test_01602a;
DROP TEMPORARY TABLE test_01602b;
