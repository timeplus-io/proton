-- Tags: not_supported, blocked_by_SummingMergeTree

SET query_mode = 'table';
drop stream if exists test_smt;

create stream test_smt (id uint32, sMap SimpleAggregateFunction(sumMap, tuple(array(uint8), array(int64))), aMap aggregate_function(sumMap, tuple(array(uint8), array(int64)))) engine SummingMergeTree partition by tuple() order by id;

insert into test_smt select id, sumMap(k), sumMapState(k) from (select 2 as id, array_join([([0], [1]), ([0, 25], [-1, to_int64(1)])]) as k) group by id, rowNumberInAllBlocks();

select sumMap(sMap), sumMapMerge(aMap) from test_smt;

drop stream if exists test_smt;

drop stream if exists simple_agf_summing_mt;

create stream simple_agf_summing_mt (a int64, grp_aggreg aggregate_function(groupUniqArrayArray, array(uint64)), grp_simple SimpleAggregateFunction(groupUniqArrayArray, array(uint64))) engine = SummingMergeTree() order by a;

insert into simple_agf_summing_mt select 1 a, groupUniqArrayArrayState([to_uint64(number)]), groupUniqArrayArray([to_uint64(number)]) from numbers(1) group by a;

insert into simple_agf_summing_mt select 1 a, groupUniqArrayArrayState([to_uint64(number)]), groupUniqArrayArray([to_uint64(number)]) from numbers(2) group by a;

optimize table simple_agf_summing_mt final;

SELECT arraySort(groupUniqArrayArrayMerge(grp_aggreg)) gra , arraySort(groupUniqArrayArray(grp_simple)) grs FROM simple_agf_summing_mt group by a;

drop stream if exists simple_agf_summing_mt;
