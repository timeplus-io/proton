DROP STREAM IF EXISTS foo;
DROP STREAM IF EXISTS foo_merge;
DROP STREAM IF EXISTS t2;

create stream foo(Id int32, Val int32) Engine=MergeTree PARTITION BY Val ORDER BY Id;
INSERT INTO foo SELECT number, number%5 FROM numbers(100000);

create stream foo_merge as foo ENGINE=Merge(currentDatabase(), '^foo');

create stream t2 (Id int32, Val int32, X int32) Engine=Memory;
INSERT INTO t2 values (4, 3, 4);

SET force_primary_key = 1, force_index_by_date=1;

SELECT * FROM foo_merge WHERE Val = 3 AND Id = 3;
SELECT count(), X FROM foo_merge JOIN t2 USING Val WHERE Val = 3 AND Id = 3 AND t2.X == 4 GROUP BY X;
SELECT count(), X FROM foo_merge JOIN t2 USING Val WHERE Val = 3 AND (Id = 3 AND t2.X == 4) GROUP BY X;
SELECT count(), X FROM foo_merge JOIN t2 USING Val WHERE Val = 3 AND Id = 3 GROUP BY X;
SELECT count(), X FROM (SELECT * FROM foo_merge) f JOIN t2 USING Val WHERE Val = 3 AND Id = 3 GROUP BY X;

DROP STREAM IF EXISTS foo;
DROP STREAM IF EXISTS foo_merge;
DROP STREAM IF EXISTS t2;
