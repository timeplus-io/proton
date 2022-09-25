SELECT x FROM (SELECT to_nullable(1) AS x) WHERE to_bool(x);
SELECT x FROM (SELECT to_nullable(0) AS x) WHERE to_bool(x);
SELECT x FROM (SELECT NULL AS x) WHERE to_bool(x);

SELECT 1 WHERE to_bool(to_nullable(1));
SELECT 1 WHERE to_bool(to_nullable(0));
SELECT 1 WHERE NULL;

SELECT x FROM (SELECT to_nullable(materialize(1)) AS x) WHERE to_bool(x);
SELECT x FROM (SELECT to_nullable(materialize(0)) AS x) WHERE to_bool(x);
SELECT x FROM (SELECT materialize(NULL) AS x) WHERE to_bool(x);

SELECT materialize('Hello') WHERE to_bool(to_nullable(materialize(1)));
SELECT materialize('Hello') WHERE to_bool(to_nullable(materialize(0)));
SELECT materialize('Hello') WHERE materialize(NULL);

SELECT x, y FROM (SELECT number % 3 = 0 ? NULL : number AS x, number AS y FROM system.numbers LIMIT 10) WHERE x % 2 != 0;
