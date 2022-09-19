-- Tags: not_supported, blocked_by_SummingMergeTree
SET query_mode = 'table';
drop stream if exists summing_merge_tree_aggregate_function;
drop stream if exists summing_merge_tree_null;

---- partition merge
create stream summing_merge_tree_aggregate_function (
    d date,
    k uint64,
    u aggregate_function(uniq, uint64)
) engine=SummingMergeTree(d, k, 1);

insert into summing_merge_tree_aggregate_function
select today() as d,
       number as k,
       uniqState(to_uint64(number % 500))
from numbers(5000)
group by d, k;

insert into summing_merge_tree_aggregate_function
select today() as d,
       number + 5000 as k,
       uniqState(to_uint64(number % 500))
from numbers(5000)
group by d, k;

select count() from summing_merge_tree_aggregate_function;
optimize table summing_merge_tree_aggregate_function;
select count() from summing_merge_tree_aggregate_function;

drop stream summing_merge_tree_aggregate_function;

---- sum + uniq + uniqExact
create stream summing_merge_tree_aggregate_function (
    d materialized today(),
    k uint64,
    c uint64,
    u aggregate_function(uniq, uint8),
    ue aggregate_function(uniqExact, uint8)
) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(1), uniqExactState(1);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(2), uniqExactState(2);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(3), uniqExactState(2);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(1), uniqExactState(1);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(2), uniqExactState(2);
insert into summing_merge_tree_aggregate_function select 1, 1, uniqState(3), uniqExactState(3);

select
    k, sum(c),
    uniqMerge(u), uniqExactMerge(ue)
from summing_merge_tree_aggregate_function group by k;

optimize table summing_merge_tree_aggregate_function;

select
    k, sum(c),
    uniqMerge(u), uniqExactMerge(ue)
from summing_merge_tree_aggregate_function group by k;

drop stream summing_merge_tree_aggregate_function;

---- sum + topK
create stream summing_merge_tree_aggregate_function (d materialized today(), k uint64, c uint64, x aggregate_function(topK(2), uint8)) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(1);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(2);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(2);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(3);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(3);
insert into summing_merge_tree_aggregate_function select 1, 1, topKState(2)(3);
select k, sum(c), topKMerge(2)(x) from summing_merge_tree_aggregate_function group by k;
optimize table summing_merge_tree_aggregate_function;
select k, sum(c), topKMerge(2)(x) from summing_merge_tree_aggregate_function group by k;

drop stream summing_merge_tree_aggregate_function;

---- sum + topKWeighted
create stream summing_merge_tree_aggregate_function (d materialized today(), k uint64, c uint64, x aggregate_function(topKWeighted(2), uint8, uint8)) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(1, 1);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(1, 1);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(1, 1);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(2, 2);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(2, 2);
insert into summing_merge_tree_aggregate_function select 1, 1, topKWeightedState(2)(3, 5);
select k, sum(c), topKWeightedMerge(2)(x) from summing_merge_tree_aggregate_function group by k;
optimize table summing_merge_tree_aggregate_function;
select k, sum(c), topKWeightedMerge(2)(x) from summing_merge_tree_aggregate_function group by k;

drop stream summing_merge_tree_aggregate_function;

---- avg
create stream summing_merge_tree_aggregate_function (d materialized today(), k uint64, x aggregate_function(avg, float64)) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, avgState(0.0);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.125);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.25);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.375);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.4375);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.5);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.5625);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.625);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.75);
insert into summing_merge_tree_aggregate_function select 1, avgState(0.875);
insert into summing_merge_tree_aggregate_function select 1, avgState(1.0);
select k, avgMerge(x) from summing_merge_tree_aggregate_function group by k;
optimize table summing_merge_tree_aggregate_function;
select k, avgMerge(x) from summing_merge_tree_aggregate_function group by k;

drop stream summing_merge_tree_aggregate_function;

---- quantile
create stream summing_merge_tree_aggregate_function (d materialized today(), k uint64, x aggregate_function(quantile(0.1), float64)) engine=SummingMergeTree(d, k, 8192);

insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.0);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.1);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.2);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.3);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.4);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.5);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.6);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.7);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.8);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(0.9);
insert into summing_merge_tree_aggregate_function select 1, quantileState(0.1)(1.0);
select k, round(quantileMerge(0.1)(x), 1) from summing_merge_tree_aggregate_function group by k;
optimize table summing_merge_tree_aggregate_function;
select k, round(quantileMerge(0.1)(x), 1) from summing_merge_tree_aggregate_function group by k;

drop stream summing_merge_tree_aggregate_function;

---- sum + uniq with more data
create stream summing_merge_tree_null (
    d materialized today(),
    k uint64,
    c uint64,
    u uint64
) engine=Null;

create materialized view summing_merge_tree_aggregate_function (
    d date,
    k uint64,
    c uint64,
    u aggregate_function(uniq, uint64)
) engine=SummingMergeTree(d, k, 8192)
as select d, k, sum(c) as c, uniqState(u) as u
from summing_merge_tree_null
group by d, k;

-- prime number 53 to avoid resonanse between %3 and %53
insert into summing_merge_tree_null select number % 3, 1, number % 53 from numbers(999999);

select k, sum(c), uniqMerge(u) from summing_merge_tree_aggregate_function group by k order by k;
optimize table summing_merge_tree_aggregate_function;
select k, sum(c), uniqMerge(u) from summing_merge_tree_aggregate_function group by k order by k;

drop stream summing_merge_tree_aggregate_function;
drop stream summing_merge_tree_null;
