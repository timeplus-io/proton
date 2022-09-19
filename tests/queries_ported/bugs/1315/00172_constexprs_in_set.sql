SELECT sumIf(number, x), sum(x) FROM (SELECT number, number IN (0 + 1, 2 + 3, to_uint64(concat('8', ''))) AS x FROM system.numbers LIMIT 10);
SELECT to_date('2015-06-12') IN to_date('2015-06-12');
SELECT to_date('2015-06-12') IN (to_date('2015-06-12'));
SELECT today() IN (to_date('2014-01-01'), to_date(now()));
SELECT - -1 IN (2 - 1);
SELECT - -1 IN (2 - 1, 3);
WITH (1, 2) AS a SELECT 1 IN a, 3 IN a;
