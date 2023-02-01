SET convert_query_to_cnf = 0;

DROP STREAM IF EXISTS n;
DROP STREAM IF EXISTS r;

CREATE STREAM n (k uint32) ENGINE = Memory;
CREATE STREAM r (k uint32, name string) ENGINE = Memory;

SET enable_optimize_predicate_expression = 0;

EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name = 'A';
EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name LIKE 'A%';
EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name NOT LIKE 'A%';

DROP STREAM n;
DROP STREAM r;
