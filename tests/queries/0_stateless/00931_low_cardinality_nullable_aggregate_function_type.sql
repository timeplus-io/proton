SET query_mode = 'table';
drop stream if exists lc;

create stream lc (`date` date, `name` low_cardinality(nullable(string)), `clicks` nullable(int32)) ENGINE = MergeTree() ORDER BY date SETTINGS index_granularity = 8192;
INSERT INTO lc SELECT '2019-01-01', null, 0 FROM numbers(1000000);
SELECT date, arg_max(name, clicks) FROM lc GROUP BY date;

drop stream if exists lc;

