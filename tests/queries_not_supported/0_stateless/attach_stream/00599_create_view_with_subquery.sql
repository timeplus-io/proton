-- Tags: no-parallel

DROP STREAM IF EXISTS test_00599;
DROP STREAM IF EXISTS test_view_00599;

create stream test_00599(id uint64)  ;
CREATE VIEW test_view_00599 AS SELECT * FROM test_00599 WHERE id = (SELECT 1);

DETACH STREAM test_view_00599;
ATTACH STREAM test_view_00599;

SHOW create stream test_view_00599;

DROP STREAM IF EXISTS test_00599;
DROP STREAM IF EXISTS test_view_00599;
