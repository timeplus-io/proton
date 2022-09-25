create temporary table test (
    arr array(array(low_cardinality(string)))
);

insert into test(arr) values ([['a'], ['b', 'c']]);

select array_filter(x -> true, arr) from test;
