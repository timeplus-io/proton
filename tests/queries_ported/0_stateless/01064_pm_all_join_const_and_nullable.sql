SET join_algorithm = 'partial_merge';

SELECT count(1), uniq_exact(1) FROM (
SELECT materialize(1) as k FROM numbers(1) as nums
JOIN (SELECT materialize(1) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT materialize(1) as k FROM numbers(1) as nums
JOIN (SELECT 1 AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT 1 as k FROM numbers(1) as nums
JOIN (SELECT materialize(1) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT 1 as k FROM numbers(1) as nums
JOIN (SELECT 1 AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT 'first nullable';

SELECT count(1), uniq_exact(1) FROM (
SELECT materialize(to_nullable(1)) as k FROM numbers(1) as nums
JOIN (SELECT materialize(1) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT materialize(to_nullable(1)) as k FROM numbers(1) as nums
JOIN (SELECT 1 AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT to_nullable(1) as k FROM numbers(1) as nums
JOIN (SELECT materialize(1) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT to_nullable(1) as k FROM numbers(1) as nums
JOIN (SELECT 1 AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT 'second nullable';

SELECT count(1), uniq_exact(1) FROM (
SELECT materialize(1) as k FROM numbers(1) as nums
JOIN (SELECT materialize(to_nullable(1)) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT materialize(1) as k FROM numbers(1) as nums
JOIN (SELECT to_nullable(1) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT 1 as k FROM numbers(1) as nums
JOIN (SELECT materialize(to_nullable(1)) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT 1 as k FROM numbers(1) as nums
JOIN (SELECT to_nullable(1) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT 'both nullable';

SELECT count(1), uniq_exact(1) FROM (
SELECT materialize(to_nullable(1)) as k FROM numbers(1) as nums
JOIN (SELECT materialize(to_nullable(1)) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT materialize(to_nullable(1)) as k FROM numbers(1) as nums
JOIN (SELECT to_nullable(1) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT to_nullable(1) as k FROM numbers(1) as nums
JOIN (SELECT materialize(to_nullable(1)) AS k, number as n FROM numbers(100000)) as j
USING k);

SELECT count(1), uniq_exact(1) FROM (
SELECT to_nullable(1) as k FROM numbers(1) as nums
JOIN (SELECT to_nullable(1) AS k, number as n FROM numbers(100000)) as j
USING k);
