drop stream if exists data_02233;
create stream data_02233 (partition int, parent_key int, child_key int, value int) engine=MergeTree() partition by partition order by parent_key;

insert into data_02233 values (1, 10, 100, 1000)(1, 20, 200, 2000);
insert into data_02233 values (2, 10, 100, 1000)(2, 20, 200, 2000);

-- fuzzer
SELECT child_key, parent_key, child_key FROM data_02233 GROUP BY parent_key, child_key, child_key ORDER BY child_key, parent_key ASC NULLS LAST SETTINGS max_threads = 1, optimize_aggregation_in_order = 1;
SELECT child_key, parent_key, child_key FROM data_02233 GROUP BY parent_key, child_key, child_key WITH TOTALS ORDER BY child_key, parent_key ASC NULLS LAST SETTINGS max_threads = 1, optimize_aggregation_in_order = 1;

drop stream data_02233;
