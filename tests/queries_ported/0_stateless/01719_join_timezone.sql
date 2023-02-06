DROP STREAM IF EXISTS test;

CREATE STREAM test (timestamp DateTime('UTC'), i uint8) Engine=MergeTree() PARTITION BY to_YYYYMM(timestamp) ORDER BY (i);
INSERT INTO test values ('2020-05-13 16:38:45', 1);

SELECT
    to_timezone(timestamp, 'America/Sao_Paulo') AS converted,
    timestamp AS original
FROM test
LEFT JOIN (SELECT 2 AS x) AS anything ON x = i
WHERE timestamp >= to_datetime('2020-05-13T00:00:00', 'America/Sao_Paulo');

/* This was incorrect result in previous ClickHouse versions:
┌─converted───────────┬─original────────────┐
│ 2020-05-13 16:38:45 │ 2020-05-13 16:38:45 │ <-- to_timezone is ignored.
└─────────────────────┴─────────────────────┘
*/

SELECT
    to_timezone(timestamp, 'America/Sao_Paulo') AS converted,
    timestamp AS original
FROM test
-- LEFT JOIN (SELECT 2 AS x) AS anything ON x = i -- Removing the join fixes the issue.
WHERE timestamp >= to_datetime('2020-05-13T00:00:00', 'America/Sao_Paulo');

/*
┌─converted───────────┬─original────────────┐
│ 2020-05-13 13:38:45 │ 2020-05-13 16:38:45 │ <-- to_timezone works.
└─────────────────────┴─────────────────────┘
*/

SELECT
    to_timezone(timestamp, 'America/Sao_Paulo') AS converted,
    timestamp AS original
FROM test
LEFT JOIN (SELECT 2 AS x) AS anything ON x = i
WHERE timestamp >= '2020-05-13T00:00:00'; -- Not using to_datetime in the WHERE also fixes the issue.

/*
┌─converted───────────┬─original────────────┐
│ 2020-05-13 13:38:45 │ 2020-05-13 16:38:45 │ <-- to_timezone works.
└─────────────────────┴─────────────────────┘
*/

DROP STREAM test;
