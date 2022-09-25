DROP STREAM IF EXISTS tmp_01781;
DROP STREAM IF EXISTS dist_01781;

SET prefer_localhost_replica=0;

create stream tmp_01781 (n low_cardinality(string)) ENGINE=Memory;
create stream dist_01781 (n low_cardinality(string)) Engine=Distributed(test_cluster_two_shards, currentDatabase(), tmp_01781, cityHash64(n));

SET insert_distributed_sync=1;
INSERT INTO dist_01781 VALUES ('1'),('2');
-- different low_cardinality size
INSERT INTO dist_01781 SELECT * FROM numbers(1000);

SET insert_distributed_sync=0;
SYSTEM STOP DISTRIBUTED SENDS dist_01781;
INSERT INTO dist_01781 VALUES ('1'),('2');
-- different low_cardinality size
INSERT INTO dist_01781 SELECT * FROM numbers(1000);
SYSTEM FLUSH DISTRIBUTED dist_01781;

DROP STREAM tmp_01781;
DROP STREAM dist_01781;
