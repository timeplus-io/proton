-- Tags: replica

SELECT count() > 0 FROM (SELECT ParsedParams.Key1 AS p FROM test.visits WHERE arrayAll(y -> array_exists(x -> y != x, p), p))
