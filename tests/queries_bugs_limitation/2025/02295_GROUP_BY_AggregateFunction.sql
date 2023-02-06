drop stream if exists data_02295;

create stream data_02295 (
    -- the order of "a" and "b" is important here
    -- (since finalizeChunk() accepts positions and they may be wrong)
    b int64,
    a int64,
    grp_aggreg aggregate_function(group_array_array, array(uint64))
) engine = MergeTree() order by a;
insert into data_02295 select 0 as b, int_div(number, 2) as a, group_array_array_state([to_uint64(number)]) from numbers(4) group by a, b;

-- { echoOn }
SELECT grp_aggreg FROM data_02295 GROUP BY a, grp_aggreg ORDER BY a SETTINGS optimize_aggregation_in_order = 0 FORMAT JSONEachRow;
SELECT grp_aggreg FROM data_02295 GROUP BY a, grp_aggreg ORDER BY a SETTINGS optimize_aggregation_in_order = 1 FORMAT JSONEachRow;
SELECT grp_aggreg FROM data_02295 GROUP BY a, grp_aggreg WITH TOTALS ORDER BY a SETTINGS optimize_aggregation_in_order = 0 FORMAT JSONEachRow;
SELECT grp_aggreg FROM data_02295 GROUP BY a, grp_aggreg WITH TOTALS ORDER BY a SETTINGS optimize_aggregation_in_order = 1 FORMAT JSONEachRow;
-- regression for incorrect positions passed to finalizeChunk()
SELECT a, min(b), max(b) FROM data_02295 GROUP BY a ORDER BY a, count() SETTINGS optimize_aggregation_in_order = 1;
SELECT a, min(b), max(b) FROM data_02295 GROUP BY a ORDER BY a, count() SETTINGS optimize_aggregation_in_order = 1, max_threads = 1;
SELECT a, min(b), max(b) FROM data_02295 GROUP BY a WITH TOTALS ORDER BY a, count() SETTINGS optimize_aggregation_in_order = 1;
SELECT a, min(b), max(b) FROM data_02295 GROUP BY a WITH TOTALS ORDER BY a, count() SETTINGS optimize_aggregation_in_order = 1, max_threads = 1;
-- { echoOff }

drop stream data_02295;
