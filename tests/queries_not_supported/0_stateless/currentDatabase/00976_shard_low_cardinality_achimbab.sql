-- Tags: shard

DROP STREAM IF EXISTS han_1;
create stream han_1 (k int32, date_dt low_cardinality(Nullable(string))) 
ENGINE = MergeTree() PARTITION BY k ORDER BY k;
INSERT INTO han_1 values (1, '2019-07-31');
SELECT k, uniq(date_dt) FROM remote('127.0.0.{1,2}', currentDatabase(), han_1) GROUP BY k;
DROP STREAM IF EXISTS han_1;
