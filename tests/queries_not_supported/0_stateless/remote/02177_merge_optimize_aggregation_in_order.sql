SET query_mode = 'table';
drop stream if exists data_02177;
create stream data_02177 (key int) Engine=MergeTree() order by key;
insert into data_02177 values (1);

-- { echoOn }

-- regression for optimize_aggregation_in_order
-- that cause "Chunk should have AggregatedChunkInfo in GroupingAggregatedTransform" error
select count() from remote('127.{1,2}', currentDatabase(), data_02177) group by key settings optimize_aggregation_in_order=1;

-- { echoOff }
drop stream data_02177;
