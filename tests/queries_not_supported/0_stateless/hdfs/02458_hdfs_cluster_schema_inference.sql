-- Tags: no-fasttest, no-parallel, no-cpu-aarch64
-- Tag no-fasttest: Depends on Java

insert into stream function hdfs('hdfs://localhost:12222/test_02458_1.tsv', 'TSV', 'column1 uint32, column2 uint32, column3 uint32') select 1, 2, 3 settings hdfs_truncate_on_insert=1;
insert into stream function hdfs('hdfs://localhost:12222/test_02458_2.tsv', 'TSV', 'column1 uint32, column2 uint32, column3 uint32') select 4, 5, 6 settings hdfs_truncate_on_insert=1;

desc hdfsCluster('test_cluster_one_shard_three_replicas_localhost', 'hdfs://localhost:12222/test_02458_{1,2}.tsv');
desc hdfsCluster('test_cluster_one_shard_three_replicas_localhost', 'hdfs://localhost:12222/test_02458_{1,2}.tsv', 'TSV');

select * from hdfsCluster('test_cluster_one_shard_three_replicas_localhost', 'hdfs://localhost:12222/test_02458_{1,2}.tsv') order by c1, c2, c3;
select * from hdfsCluster('test_cluster_one_shard_three_replicas_localhost', 'hdfs://localhost:12222/test_02458_{1,2}.tsv', 'TSV') order by c1, c2, c3;

