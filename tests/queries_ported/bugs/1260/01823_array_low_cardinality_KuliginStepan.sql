create temporary table test (
    arr array(array(LowCardinality(string)))
);

insert into test(arr) values ([['a'], ['b', 'c']]);

select array_filter(x -> 1, arr) from test;
