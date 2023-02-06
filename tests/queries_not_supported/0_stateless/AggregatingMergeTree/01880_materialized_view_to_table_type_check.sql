DROP STREAM IF EXISTS test_mv;
DROP STREAM IF EXISTS test;
DROP STREAM IF EXISTS test_input;

CREATE STREAM test_input(id int32) ENGINE=MergeTree() order by id; 

CREATE STREAM test(`id` int32, `pv` aggregate_function(sum, int32)) ENGINE = AggregatingMergeTree() ORDER BY id;

CREATE MATERIALIZED VIEW test_mv to test(`id` int32, `pv` aggregate_function(sum, int32)) as SELECT id, sumState(1) as pv from test_input group by id; -- { serverError 70 } 

INSERT INTO test_input SELECT to_int32(number % 1000) AS id FROM numbers(10);
select '----------test--------:';
select * from test;

create MATERIALIZED VIEW test_mv to test(`id` int32, `pv` aggregate_function(sum, int32)) as SELECT id, sumState(to_int32(1)) as pv from test_input group by id; 
INSERT INTO test_input SELECT to_int32(number % 1000) AS id FROM numbers(100,3);

select '----------test--------:';
select * from test;

DROP STREAM test_mv;
DROP STREAM test;
DROP STREAM test_input;
