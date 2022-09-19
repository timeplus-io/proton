SET convert_query_to_cnf = 1;

DROP STREAM IF EXISTS cnf_test;

create stream cnf_test (i int64) ENGINE = MergeTree() ORDER BY i;

EXPLAIN SYNTAX SELECT i FROM cnf_test WHERE NOT ((i > 1) OR (i > 2));
EXPLAIN SYNTAX SELECT i FROM cnf_test WHERE NOT ((i > 1) AND (i > 2));

EXPLAIN SYNTAX SELECT i FROM cnf_test WHERE ((i > 1) AND (i > 2)) OR ((i > 3) AND (i > 4)) OR ((i > 5) AND (i > 6));

EXPLAIN SYNTAX SELECT i FROM cnf_test WHERE NOT (((i > 1) OR (i > 2)) AND ((i > 3) OR (i > 4)) AND ((i > 5) OR (i > 6)));

EXPLAIN SYNTAX SELECT i FROM cnf_test WHERE ((i > 1) AND (i > 2) AND (i > 7)) OR ((i > 3) AND (i > 4) AND (i > 8)) OR ((i > 5) AND (i > 6));

EXPLAIN SYNTAX SELECT i FROM cnf_test WHERE ((i > 1) OR (i > 2) OR (i > 7)) AND ((i > 3) OR (i > 4) OR (i > 8)) AND NOT ((i > 5) OR (i > 6));

DROP STREAM cnf_test;
