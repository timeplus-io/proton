set send_logs_level = 'error';

DROP STREAM IF EXISTS merge_tree_table1;
create stream merge_tree_table1 (`s` LowCardinality(string), x uint32) ENGINE = MergeTree ORDER BY x settings index_granularity = 1;
create stream buffer_table1 ( `s` string , x uint32) ENGINE = Buffer(currentDatabase(), 'merge_tree_table1', 16, 10, 60, 10, 1000, 1048576, 2097152);
SELECT s FROM buffer_table1;

insert into merge_tree_table1 values ('a', 1);
select s from buffer_table1 where x = 1;
select s from buffer_table1 where x = 2;

DROP STREAM IF EXISTS merge_tree_table1;
DROP STREAM buffer_table1;
