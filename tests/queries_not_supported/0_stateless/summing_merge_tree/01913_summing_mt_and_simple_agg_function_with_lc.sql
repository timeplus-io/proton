-- Tags: not_supported, blocked_by_SummingMergeTree
SET query_mode = 'table';
drop stream if exists smta;

create stream smta
(
    `k` int64,
    `a` aggregate_function(max, int64),
    `city` SimpleAggregateFunction(max, low_cardinality(string))
)
ENGINE = SummingMergeTree
ORDER BY k;

insert into smta(k, city) values (1, 'x');

select k, city from smta;

insert into smta(k, city) values (1, 'y');
optimize table smta;

select k, city from smta;

drop stream if exists smta;
