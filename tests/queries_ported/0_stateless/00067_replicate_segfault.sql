-- Tags: replica

SELECT array_filter(x -> materialize(0), materialize([0])) AS p, arrayAll(y -> array_exists(x -> y != x, p), p) AS test;
SELECT array_filter(x -> materialize(0), materialize([''])) AS p, arrayAll(y -> array_exists(x -> y != x, p), p) AS test;
