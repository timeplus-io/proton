SET query_mode = 'table';
drop stream if exists t;
drop stream if exists td;
create stream t (val uint32) engine = MergeTree order by val;
create stream td engine = Distributed(test_shard_localhost, currentDatabase(), 't') as t;
select engine from system.tables where database = currentDatabase() and name = 'td';
drop stream if exists t;
drop stream if exists td;
