drop stream if exists t;
create stream t (n int, s string) engine MergeTree order by n;
insert into t values (1, 'a');
select count(), count(n), count(s) from cluster('test_cluster_two_shards', current_database(), t);
drop stream t;
