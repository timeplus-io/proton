-- Tags: distributed

-- Test from the issue https://github.com/ClickHouse/ClickHouse/issues/2610
SET query_mode = 'table';
drop stream if exists data_01227;
create stream data_01227 (key int) Engine=MergeTree() order by key;
insert into data_01227 select * from numbers(10);
select * from remote('127.1', currentDatabase(), data_01227) prewhere key global in (select key from data_01227 prewhere key = 2);
select * from cluster('test_cluster_two_shards', currentDatabase(), data_01227) prewhere key global in (select key from data_01227 prewhere key = 2);

drop stream data_01227;
