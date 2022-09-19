DROP STREAM IF EXISTS view1_00270;
DROP STREAM IF EXISTS view2_00270;
DROP STREAM IF EXISTS merge_view_00270;

CREATE VIEW view1_00270 AS SELECT number FROM system.numbers LIMIT 10;
CREATE VIEW view2_00270 AS SELECT number FROM system.numbers LIMIT 10;
create stream merge_view_00270 (number uint64) ENGINE = Merge(currentDatabase(), '^view');

SELECT 'Hello, world!' FROM merge_view_00270 LIMIT 5;

DROP STREAM view1_00270;
DROP STREAM view2_00270;
DROP STREAM merge_view_00270;
