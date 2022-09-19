-- Tags: distributed

DROP STREAM IF EXISTS union1;
DROP STREAM IF EXISTS union2;
create stream union1 ( date date, a int32, b int32, c int32, d int32) ENGINE = MergeTree(date, (a, date), 8192);
create stream union2 ( date date, a int32, b int32, c int32, d int32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'union1');
ALTER STREAM union2 MODIFY ORDER BY a; -- { serverError 48 }
DROP STREAM union1;
DROP STREAM union2;
