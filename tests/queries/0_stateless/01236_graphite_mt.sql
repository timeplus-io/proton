
-- Use datetime('UTC') to have a common rollup window
SET query_mode = 'table';
drop stream if exists test_graphite;
create stream test_graphite (key uint32, Path string, Time datetime('UTC'), Value float64, Version uint32, col uint64)
    engine = GraphiteMergeTree('graphite_rollup') order by key settings index_granularity=10;

SET joined_subquery_requires_alias = 0;

INSERT into test_graphite
WITH dates AS
    (
        SELECT to_start_of_day(to_datetime(now('UTC'), 'UTC')) as today,
               today - INTERVAL 3 day as older_date
    )
    -- Newer than 2 days are kept in windows of 600 seconds
    select 1 AS key, 'sum_1' AS s, today - number * 60 - 30, number, 1, number from dates, numbers(300) union all
    select 2, 'sum_1', today - number * 60 - 30, number, 1, number from dates, numbers(300) union all
    select 1, 'sum_2', today - number * 60 - 30, number, 1, number from dates, numbers(300) union all
    select 2, 'sum_2', today - number * 60 - 30, number, 1, number from dates, numbers(300) union all
    select 1, 'max_1', today - number * 60 - 30, number, 1, number from dates, numbers(300) union all
    select 2, 'max_1', today - number * 60 - 30, number, 1, number from dates, numbers(300) union all
    select 1, 'max_2', today - number * 60 - 30, number, 1, number from dates, numbers(300) union all
    select 2, 'max_2', today - number * 60 - 30, number, 1, number from dates, numbers(300) union all

    -- Older than 2 days use 6000 second windows
    select 1 AS key, 'sum_1' AS s, older_date - number * 60 - 30, number, 1, number from dates, numbers(1200) union all
    select 2, 'sum_1', older_date - number * 60 - 30, number, 1, number from dates, numbers(1200) union all
    select 1, 'sum_2', older_date - number * 60 - 30, number, 1, number from dates, numbers(1200) union all
    select 2, 'sum_2', older_date - number * 60 - 30, number, 1, number from dates, numbers(1200) union all
    select 1, 'max_1', older_date - number * 60 - 30, number, 1, number from dates, numbers(1200) union all
    select 2, 'max_1', older_date - number * 60 - 30, number, 1, number from dates, numbers(1200) union all
    select 1, 'max_2', older_date - number * 60 - 30, number, 1, number from dates, numbers(1200) union all
    select 2, 'max_2', older_date - number * 60 - 30, number, 1, number from dates, numbers(1200);

select key, Path, Value, Version, col from test_graphite final order by key, Path, Time desc;

optimize table test_graphite final;

select key, Path, Value, Version, col from test_graphite order by key, Path, Time desc;

drop stream test_graphite;
