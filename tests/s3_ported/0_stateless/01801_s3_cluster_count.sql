-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select count() from s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv');
select count(*) from s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv');
