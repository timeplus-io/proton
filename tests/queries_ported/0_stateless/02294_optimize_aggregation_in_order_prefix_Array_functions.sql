drop stream if exists data_02294;
create stream data_02294 (a int64, b int64, grp_aggreg aggregate_function(group_array_array, array(uint64)), grp_simple simple_aggregate_function(group_array_array, array(uint64))) engine = MergeTree() order by a;
insert into data_02294 select int_div(number, 2) as a, 0 as b, group_array_array_state([to_uint64(number)]), group_array_array([to_uint64(number)]) from numbers(4) group by a, b;
SELECT array_sort(group_array_array_merge(grp_aggreg)) as gra , array_sort(group_array_array(grp_simple)) as grs FROM data_02294 group by a, b SETTINGS optimize_aggregation_in_order=1;
drop stream data_02294;
