-- Tags: distributed

SET insert_distributed_sync = 1;

DROP STREAM IF EXISTS low_cardinality;
DROP STREAM IF EXISTS low_cardinality_all;

create stream low_cardinality (d date, x uint32, s low_cardinality(string)) ENGINE = MergeTree(d, x, 8192);
create stream low_cardinality_all (d date, x uint32, s low_cardinality(string)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), low_cardinality, sipHash64(s));

INSERT INTO low_cardinality_all (d,x,s) VALUES ('2018-11-12',1,'123');
SELECT s FROM low_cardinality_all;

DROP STREAM IF EXISTS low_cardinality;
DROP STREAM IF EXISTS low_cardinality_all;
