DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;
DROP STREAM IF EXISTS mv1;
DROP STREAM IF EXISTS mv2;

create stream test1 (a uint8) ENGINE MergeTree ORDER BY a;
create stream test2 (a uint8) ENGINE MergeTree ORDER BY a;

CREATE MATERIALIZED VIEW mv1 TO test1 AS SELECT a FROM test2;
CREATE MATERIALIZED VIEW mv2 TO test2 AS SELECT a FROM test1;

insert into test1 values (1); -- { serverError 306 }

DROP STREAM test1;
DROP STREAM test2;
DROP STREAM mv1;
DROP STREAM mv2;
