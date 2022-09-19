-- Tags: no-parallel

DROP DATABASE IF EXISTS test_show_tables;

CREATE DATABASE test_show_tables;

create stream test_show_tables.A (A uint8) ;
create stream test_show_tables.B (A uint8) ;

SHOW TABLES from test_show_tables;
SHOW TABLES in system where engine like '%System%' and name in ('numbers', 'one');

SELECT name, to_uint32(metadata_modification_time) > 0, engine_full, create_table_query FROM system.tables WHERE database = 'test_show_tables' ORDER BY name FORMAT TSVRaw;

CREATE TEMPORARY STREAM test_temporary_table (id uint64);
SELECT name FROM system.tables WHERE is_temporary = 1 AND name = 'test_temporary_table';

create stream test_show_tables.test_log(id uint64)  ;
CREATE MATERIALIZED VIEW test_show_tables.test_materialized   AS SELECT * FROM test_show_tables.test_log;
SELECT dependencies_database, dependencies_table FROM system.tables WHERE name = 'test_log';

DROP DATABASE test_show_tables;


-- Check that create_table_query works for system tables and unusual Databases
DROP DATABASE IF EXISTS test_DatabaseMemory;
CREATE DATABASE test_DatabaseMemory ;
create stream test_DatabaseMemory.A (A uint8) ENGINE = Null;

SELECT sum(ignore(*, metadata_modification_time, engine_full, create_table_query)) FROM system.tables WHERE database = 'test_DatabaseMemory';

DROP DATABASE test_DatabaseMemory;
