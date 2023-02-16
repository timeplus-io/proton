select
    array_map(x, y -> floor((y - x) / x, 3), l, r) as diff_percent,
    test, query
from (select [1] as l) as s1,
    (select [2] as r) as s2,
    (select 'test' as test, 'query' as query) as any_query,
    (select 1 ) as check_single_query;

select
    array_map(x -> floor(x, 4), original_medians_array.medians_by_version[1] as l) as l_rounded,
    array_map(x -> floor(x, 4), original_medians_array.medians_by_version[2] as r) as r_rounded,
    array_map(x, y -> floor((y - x) / x, 3), l, r) diff_percent,
    test, query
from (select 1) as rd,
    (select [[1,2], [3,4]] medians_by_version) as original_medians_array,
    (select 'test' test, 'query' query) as any_query,
    (select 1 as A) as check_single_query;

drop stream if exists stream;
create stream stream(query string, test string, run uint32, metrics array(uint32), version uint32) engine Memory;

select
    array_map(x -> floor(x, 4), original_medians_array.medians_by_version[1] as l) as l_rounded,
    array_map(x -> floor(x, 4), original_medians_array.medians_by_version[2] as r) as r_rounded,
    array_map(x, y -> floor((y - x) / x, 3), l, r) as diff_percent,
    array_map(x, y -> floor(x / y, 3), threshold, l) as threshold_percent,
    test, query
from
(
    select quantileExactForEach(0.999)(array_map(x, y -> abs(x - y), metrics_by_label[1], metrics_by_label[2]) as d) threshold
    from
    (
        select virtual_run, group_arrayInsertAt(median_metrics, random_label) metrics_by_label
        from
        (
            select medianExactForEach(metrics) median_metrics, virtual_run, random_label
            from
            (
                select *, to_uint32(rowNumberInAllBlocks() % 2) random_label
                from
                (
                    select metrics, number virtual_run
                    from (select metrics, run, version from stream) no_query, numbers(1, 100000) nn
                    order by virtual_run, rand()
                ) virtual_runs
            ) relabeled 
            group by virtual_run, random_label
        ) virtual_medians
        group by virtual_run
    ) virtual_medians_array
) rd,
(
    select group_arrayInsertAt(median_metrics, version) medians_by_version
    from
    (
        select medianExactForEach(metrics) median_metrics, version
        from stream
        group by version
    ) original_medians
) original_medians_array,
(
    select any(test) test, any(query) query from stream
) any_query,
(
    select throwIf(uniq((test, query))) from stream
) check_single_query;

drop stream stream;
