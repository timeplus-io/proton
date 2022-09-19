DROP STREAM IF EXISTS n;
DROP STREAM IF EXISTS r;

create stream n (k uint32) ;
create stream r (k uint32, name string) ;

SET enable_optimize_predicate_expression = 0;

EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name = 'A';
EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name LIKE 'A%';
EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name NOT LIKE 'A%';

DROP STREAM n;
DROP STREAM r;
