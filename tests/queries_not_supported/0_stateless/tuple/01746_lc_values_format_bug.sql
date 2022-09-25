SET query_mode = 'table';
drop stream if exists lc_test;

create stream lc_test
(
    `id` low_cardinality(string)
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY id;

insert into lc_test values (to_string('a'));

select id from lc_test;
drop stream if exists lc_test;
