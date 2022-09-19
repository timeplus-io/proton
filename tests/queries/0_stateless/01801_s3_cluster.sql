-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select * from s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'a uint64, b uint64, c uint64') ORDER BY a, b, c;
select * from s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'a uint64, b uint64, c uint64') ORDER BY a, b, c;
