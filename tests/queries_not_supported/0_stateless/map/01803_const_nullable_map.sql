DROP STREAM IF EXISTS t_map_null;

SET allow_experimental_map_type = 1;

CREATE STREAM t_map_null (a map(string, string), b string) engine = MergeTree() ORDER BY a;
INSERT INTO t_map_null VALUES (map('a', 'b', 'c', 'd'), 'foo');
SELECT count() FROM t_map_null WHERE a = map('name', NULL, '', NULL);

DROP STREAM t_map_null;
