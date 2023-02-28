-- Tags: global, no-parallel
CREATE DATABASE IF NOT EXISTS test_02115;
USE test_02115;

DROP STREAM IF EXISTS t1_local;
DROP STREAM IF EXISTS t2_local;
DROP STREAM IF EXISTS t1_all;
DROP STREAM IF EXISTS t2_all;

create stream t1_local(a int32) engine=MergeTree() order by a;
create stream t2_local as t1_local;

create stream t1_all as t1_local engine Distributed(test_cluster_two_shards_localhost, test_02115, t1_local, rand());
create stream t2_all as t2_local engine Distributed(test_cluster_two_shards_localhost, test_02115, t2_local, rand());

insert into t1_local values (1), (2), (3);
insert into t2_local values (1), (2), (3);

set distributed_product_mode = 'local';
select * from t1_all t1 where t1.a in (select t2.a from t2_all t2);
explain syntax select t1.* from t1_all t1 join t2_all t2 on t1.a = t2.a;
select t1.* from t1_all t1 join t2_all t2 on t1.a = t2.a ORDER BY t1.a;

SELECT '-';

set distributed_product_mode = 'global';
select * from t1_all t1 where t1.a in (select t2.a from t2_all t2);
explain syntax select t1.* from t1_all t1 join t2_all t2 on t1.a = t2.a;
select t1.* from t1_all t1 join t2_all t2 on t1.a = t2.a ORDER BY t1.a;

DROP STREAM t1_local;
DROP STREAM t2_local;
DROP STREAM t1_all;
DROP STREAM t2_all;
DROP DATABASE test_02115;
