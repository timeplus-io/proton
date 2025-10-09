-- Tags: no-fasttest, no-msan

drop stream if exists dummy;
CREATE STREAM dummy ( num1 int32, num2 enum8('foo' = 0, 'bar' = 1, 'tar' = 2) )
ENGINE = MergeTree ORDER BY num1 as select 5, 'bar';

set compile_aggregate_expressions=1;
set min_count_to_compile_aggregate_expression=0;

-- { echoOn }
SYSTEM DROP COMPILED EXPRESSION CACHE;
SELECT min_if(num1, num1 < 5) FROM dummy GROUP BY num2;
SYSTEM DROP COMPILED EXPRESSION CACHE;
SELECT min_if(num1, num1 >= 5) FROM dummy GROUP BY num2;
-- { echoOff }

drop stream dummy;
