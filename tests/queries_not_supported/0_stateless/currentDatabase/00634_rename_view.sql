-- Tags: no-replicated-database
-- Tag no-replicated-database: Does not support renaming of multiple tables in single query

DROP STREAM IF EXISTS test1_00634;
DROP STREAM IF EXISTS test2_00634;
DROP STREAM IF EXISTS v_test1;
DROP STREAM IF EXISTS v_test2;
DROP STREAM IF EXISTS v_test11;
DROP STREAM IF EXISTS v_test22;

create stream test1_00634 (id uint8) engine = TinyLog;
create stream test2_00634 (id uint8) engine = TinyLog;

create view v_test1 as select id from test1_00634;
create view v_test2 as select id from test2_00634;

rename table v_test1 to v_test11, v_test2 to v_test22;

SELECT name, engine FROM system.tables WHERE name IN ('v_test1', 'v_test2', 'v_test11', 'v_test22') AND database = currentDatabase() ORDER BY name;

DROP STREAM test1_00634;
DROP STREAM test2_00634;
DROP STREAM v_test11;
DROP STREAM v_test22;
