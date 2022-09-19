SET query_mode = 'table';
drop stream if exists t;

create stream t (number uint64) engine = Distributed(test_cluster_two_shards, system, numbers);
select * from t where number = 0 limit 2 settings sleep_in_receive_cancel_ms = 10000, max_execution_time = 5;

drop stream t;
