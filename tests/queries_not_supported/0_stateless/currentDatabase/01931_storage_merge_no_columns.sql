SET query_mode = 'table';
drop stream if exists data;
create stream data (key int) engine=MergeTree() order by key;
select 1 from merge(currentDatabase(), '^data$') prewhere _table in (NULL);
drop stream data;
