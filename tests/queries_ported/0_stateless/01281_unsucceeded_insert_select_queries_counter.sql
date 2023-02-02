-- Tags: no-parallel, no-fasttest

DROP STREAM IF EXISTS current_failed_query_metrics;
DROP STREAM IF EXISTS to_insert;

CREATE STREAM current_failed_query_metrics (event low_cardinality(string), value uint64) ENGINE = Memory();


INSERT INTO current_failed_query_metrics 
SELECT event, value
FROM system.events
WHERE event in ('FailedQuery', 'FailedInsertQuery', 'FailedSelectQuery');

CREATE STREAM to_insert (value uint64) ENGINE = Memory();

-- Failed insert before execution
INSERT INTO stream_that_do_not_exists VALUES (42); -- { serverError 60 }

SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedInsertQuery'
) AS previous
ALL LEFT JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;


-- Failed insert in execution
INSERT INTO to_insert SELECT throw_if(1); -- { serverError 395 }

SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedInsertQuery'
) AS previous
ALL LEFT JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;


-- Failed select before execution
SELECT * FROM stream_that_do_not_exists; -- { serverError 60 }

SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedSelectQuery'
) AS previous
ALL LEFT  JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;

-- Failed select in execution
SELECT throw_if(1); -- { serverError 395 }

SELECT current_value - previous_value
FROM (
    SELECT event, value as current_value FROM system.events WHERE event like 'FailedSelectQuery'
) AS previous
ALL LEFT JOIN (
    SELECT event, value as previous_value FROM current_failed_query_metrics
) AS current
on previous.event = current.event;


DROP STREAM current_failed_query_metrics;
DROP STREAM to_insert;
