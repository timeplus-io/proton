select hostname();
select hostName() as h, count() from cluster(test_cluster_two_shards, system.one) group by h;
