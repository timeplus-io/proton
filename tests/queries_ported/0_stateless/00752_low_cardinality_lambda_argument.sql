SET query_mode = 'table';
drop stream if exists lc_lambda;
create stream lc_lambda (arr array(low_cardinality(uint64))) engine = Memory;
insert into lc_lambda(arr) select range(number) from system.numbers limit 10;
select sleep(3);
select array_filter(x -> x % 2 == 0, arr) from lc_lambda;
drop stream if exists lc_lambda;

drop stream if exists test_array;
create stream test_array(resources_host array(low_cardinality(string))) ENGINE = MergeTree() ORDER BY (resources_host);
insert into test_array(resources_host) values (['a']);
select sleep(3);
SELECT array_map(i -> [resources_host[i]], array_enumerate(resources_host)) FROM test_array;
drop stream if exists test_array;

SELECT array_map(x -> (x + (array_map(y -> ((x + y) + to_low_cardinality(1)), [])[1])), []);
