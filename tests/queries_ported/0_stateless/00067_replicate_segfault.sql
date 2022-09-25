-- Tags: replica

SELECT array_filter(x -> to_bool(materialize(0)), materialize([0])) AS p, array_all(y -> array_exists(x -> y != x, p), p) AS test;
SELECT array_filter(x -> to_bool(materialize(0)), materialize([''])) AS p, array_all(y -> array_exists(x -> y != x, p), p) AS test;
