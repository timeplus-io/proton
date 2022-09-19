DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;
DROP STREAM IF EXISTS mv;

create stream test1 (a uint8, b string) ENGINE MergeTree ORDER BY a;
create stream test2 (c uint8, d string) ENGINE MergeTree ORDER BY c;
CREATE MATERIALIZED VIEW mv TO test1 (b string, a uint8) AS SELECT d AS b, c AS a FROM test2;

INSERT INTO test2 VALUES (1, 'test');

SELECT * FROM test1;

DROP STREAM test1;
DROP STREAM test2;
DROP STREAM mv;
