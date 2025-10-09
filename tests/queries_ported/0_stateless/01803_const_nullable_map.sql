DROP STREAM IF EXISTS t_map_null;

CREATE STREAM t_map_null (a map(string, string), b string) engine = MergeTree() ORDER BY a;
INSERT INTO t_map_null VALUES (map_cast('a', 'b', 'c', 'd'), 'foo');
SELECT count() FROM t_map_null WHERE a = map_cast('name', NULL, '', NULL);

DROP STREAM t_map_null;
