drop stream if exists data_02293;
create stream data_02293 (a int64, grp_aggreg aggregate_function(group_array_array, array(uint64)), grp_simple simple_aggregate_function(group_array_array, array(uint64))) engine = MergeTree() order by a;
insert into data_02293 select 1 as a, group_array_array_state([to_uint64(number)]), group_array_array([to_uint64(number)]) from numbers(2) group by a;
SELECT array_sort(group_array_array_merge(grp_aggreg)) as gra , array_sort(group_array_array(grp_simple)) as grs FROM data_02293 group by a SETTINGS optimize_aggregation_in_order=1;
drop stream data_02293;
