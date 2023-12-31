SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_substitute_columns = 1;
SET optimize_append_index = 1;

DROP STREAM IF EXISTS index_append_test_test;

CREATE STREAM index_append_test_test (i int64, a uint32, b uint64, CONSTRAINT c1 ASsumE i <= 2 * b AND i + 40 > a) ENGINE = MergeTree() ORDER BY i;
INSERT INTO index_append_test_test VALUES (1, 10, 1), (2, 20, 2);

EXPLAIN SYNTAX SELECT i FROM index_append_test_test WHERE a = 0;
EXPLAIN SYNTAX SELECT i FROM index_append_test_test WHERE a < 0;
EXPLAIN SYNTAX SELECT i FROM index_append_test_test WHERE a >= 0;
EXPLAIN SYNTAX SELECT i FROM index_append_test_test WHERE 2 * b < 100;

DROP STREAM index_append_test_test;
