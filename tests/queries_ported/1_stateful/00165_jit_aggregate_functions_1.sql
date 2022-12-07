SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

SELECT 'Aggregation using JIT compilation';

SELECT 'Simple functions';

SELECT
    CounterID,
    min(WatchID),
    max(WatchID),
    sum(WatchID),
    avg(WatchID),
    avg_weighted(WatchID, CounterID),
    count(WatchID)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions with non compilable function';

SELECT
    CounterID,
    min(WatchID),
    max(WatchID),
    sum(WatchID),
    sum(to_uint128(WatchID)),
    avg(WatchID),
    avg_weighted(WatchID, CounterID),
    count(WatchID)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions if combinator';

WITH (WatchID % 2 == 0) AS predicate
SELECT
    CounterID,
    min_if(WatchID,predicate),
    max_if(WatchID, predicate),
    sum_if(WatchID, predicate),
    avg_if(WatchID, predicate),
    avg_weighted_if(WatchID, CounterID, predicate),
    count_if(WatchID, predicate)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions without key';

SELECT
    min(WatchID) AS min_watch_id,
    max(WatchID),
    sum(WatchID),
    avg(WatchID),
    avg_weighted(WatchID, CounterID),
    count(WatchID)
FROM test.hits
ORDER BY min_watch_id DESC LIMIT 20;

SELECT 'Simple functions with non compilable function without key';

SELECT
    min(WatchID) AS min_watch_id,
    max(WatchID),
    sum(WatchID),
    sum(to_uint128(WatchID)),
    avg(WatchID),
    avg_weighted(WatchID, CounterID),
    count(WatchID)
FROM test.hits
ORDER BY min_watch_id DESC LIMIT 20;

SELECT 'Simple functions if combinator without key';

WITH (WatchID % 2 == 0) AS predicate
SELECT
    min_if(WatchID, predicate) as min_watch_id,
    max_if(WatchID, predicate),
    sum_if(WatchID, predicate),
    avg_if(WatchID, predicate),
    avg_weighted_if(WatchID, CounterID, predicate),
    count_if(WatchID, predicate)
FROM test.hits
ORDER BY min_watch_id
DESC LIMIT 20;

SET compile_aggregate_expressions = 0;

SELECT 'Aggregation without JIT compilation';

SELECT 'Simple functions';

SELECT
    CounterID,
    min(WatchID),
    max(WatchID),
    sum(WatchID),
    avg(WatchID),
    avg_weighted(WatchID, CounterID),
    count(WatchID)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions with non compilable function';
SELECT
    CounterID,
    min(WatchID),
    max(WatchID),
    sum(WatchID),
    sum(to_uint128(WatchID)),
    avg(WatchID),
    avg_weighted(WatchID, CounterID),
    count(WatchID)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions if combinator';

WITH (WatchID % 2 == 0) AS predicate
SELECT
    CounterID,
    min_if(WatchID,predicate),
    max_if(WatchID, predicate),
    sum_if(WatchID, predicate),
    avg_if(WatchID, predicate),
    avg_weighted_if(WatchID, CounterID, predicate),
    count_if(WatchID, predicate)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions without key';

SELECT
    min(WatchID) AS min_watch_id,
    max(WatchID),
    sum(WatchID),
    avg(WatchID),
    avg_weighted(WatchID, CounterID),
    count(WatchID)
FROM test.hits
ORDER BY min_watch_id DESC LIMIT 20;

SELECT 'Simple functions with non compilable function without key';

SELECT
    min(WatchID) AS min_watch_id,
    max(WatchID),
    sum(WatchID),
    sum(to_uint128(WatchID)),
    avg(WatchID),
    avg_weighted(WatchID, CounterID),
    count(WatchID)
FROM test.hits
ORDER BY min_watch_id DESC LIMIT 20;

SELECT 'Simple functions if combinator without key';

WITH (WatchID % 2 == 0) AS predicate
SELECT
    min_if(WatchID, predicate) as min_watch_id,
    max_if(WatchID, predicate),
    sum_if(WatchID, predicate),
    avg_if(WatchID, predicate),
    avg_weighted_if(WatchID, CounterID, predicate),
    count_if(WatchID, predicate)
FROM test.hits
ORDER BY min_watch_id
DESC LIMIT 20;
