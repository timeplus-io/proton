DROP STREAM IF EXISTS t_functions_to_subcolumns_alias;

CREATE STREAM t_functions_to_subcolumns_alias (id uint64, t tuple(uint64, string), m map(string, uint64)) ENGINE = Memory;
INSERT INTO t_functions_to_subcolumns_alias VALUES (1, (100, 'abc'), map('foo', 1, 'bar', 2)) (2, NULL, map());

SELECT count(id) AS cnt FROM t_functions_to_subcolumns_alias FORMAT TSVWithNames;
SELECT tupleElement(t, 1) as t0, t0 FROM t_functions_to_subcolumns_alias FORMAT TSVWithNames;
SELECT mapContains(m, 'foo') AS hit FROM t_functions_to_subcolumns_alias FORMAT TSVWithNames;

DROP STREAM t_functions_to_subcolumns_alias;
