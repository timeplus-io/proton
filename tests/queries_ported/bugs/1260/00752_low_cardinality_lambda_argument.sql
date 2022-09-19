SET query_mode = 'table';
drop stream if exists lc_lambda;
create stream lc_lambda (arr array(LowCardinality(uint64))) engine = Memory;
insert into lc_lambda select range(number) from system.numbers limit 10;
select array_filter(x -> x % 2 == 0, arr) from lc_lambda;
drop stream if exists lc_lambda;

drop stream if exists test_array;
create stream test_array(resources_host array(LowCardinality(string))) ENGINE = MergeTree() ORDER BY (resources_host);
insert into test_array values (['a']);
SELECT array_map(i -> [resources_host[i]], array_enumerate(resources_host)) FROM test_array;
drop stream if exists test_array;
