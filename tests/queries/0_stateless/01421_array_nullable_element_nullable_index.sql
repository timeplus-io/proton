SELECT [to_nullable(1)] AS x, x[to_nullable(1)] AS y;
SELECT materialize([to_nullable(1)]) AS x, x[to_nullable(1)] AS y;
SELECT [to_nullable(1)] AS x, x[materialize(to_nullable(1))] AS y;
SELECT materialize([to_nullable(1)]) AS x, x[materialize(to_nullable(1))] AS y;
