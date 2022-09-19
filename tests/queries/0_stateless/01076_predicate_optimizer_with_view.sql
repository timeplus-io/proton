DROP STREAM IF EXISTS test;
DROP STREAM IF EXISTS test_view;

create stream test(date date, id int8, name string, value int64) ENGINE = MergeTree(date, (id, date), 8192);
CREATE VIEW test_view AS SELECT * FROM test;

SET enable_optimize_predicate_expression = 1;

-- Optimize predicate expression with view
EXPLAIN SYNTAX SELECT * FROM test_view WHERE id = 1;
EXPLAIN SYNTAX SELECT * FROM test_view WHERE id = 2;
EXPLAIN SYNTAX SELECT id FROM test_view WHERE id  = 1;
EXPLAIN SYNTAX SELECT s.id FROM test_view AS s WHERE s.id = 1;

SELECT * FROM (SELECT to_uint64(b), sum(id) AS b FROM test) WHERE `to_uint64(sum(id))` = 3; -- { serverError 47 }

DROP STREAM IF EXISTS test;
DROP STREAM IF EXISTS test_view;
