-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

desc s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv');
desc s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV');
desc s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'c1 uint64, c2 uint64, c3 uint64');
desc s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest');
desc s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv', 'TSV', 'c1 uint64, c2 uint64, c3 uint64', 'auto');
desc s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV');
desc s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 uint64, c2 uint64, c3 uint64');
desc s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 uint64, c2 uint64, c3 uint64', 'auto');


SELECT * FROM s3(decodeURLComponent(NULL), [NULL]);  --{serverError BAD_ARGUMENTS}
