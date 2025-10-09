drop stream if exists data;
create stream data (part int, k int) engine=MergeTree() order by k partition by part;
insert into data values (1, 1)(2, 2);

-- { echoOn }
select * from data preWhere index_hint(_partition_id = '1');
select count() from data preWhere index_hint(_partition_id = '1');
select * from data where index_hint(_partition_id = '1');
select count() from data where index_hint(_partition_id = '1');