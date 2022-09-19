-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;
DROP STREAM IF EXISTS test;
DROP STREAM IF EXISTS lv;
create stream test (n int8) ;
CREATE LIVE VIEW lv AS SELECT * FROM test;
DETACH TABLE lv;
INSERT INTO test VALUES (42);
DROP STREAM test;
ATTACH TABLE lv;
DROP STREAM lv;
