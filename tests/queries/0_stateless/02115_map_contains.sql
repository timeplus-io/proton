DROP STREAM IF EXISTS t_map_contains;

create stream t_map_contains (m Map(string, uint32)) ;

INSERT INTO t_map_contains VALUES (map('a', 1, 'b', 2)), (map('c', 3, 'd', 4));

SET optimize_functions_to_subcolumns = 1;

EXPLAIN SYNTAX SELECT mapContains(m, 'a') FROM t_map_contains;
SELECT mapContains(m, 'a') FROM t_map_contains;

DROP STREAM t_map_contains;
