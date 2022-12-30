DROP STREAM IF EXISTS test.basic;
DROP STREAM IF EXISTS test.visits_null;

CREATE TABLE test.visits_null
(
    CounterID UInt32,
    StartDate date,
    Sign int8,
    UserID uint64
) ENGINE = Null;

CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree(StartDate, (CounterID, StartDate), 8192)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)                  AS Visits,
    uniqState(UserID)               AS Users
FROM test.visits_null
GROUP BY CounterID, StartDate;

INSERT INTO test.visits_null
SELECT
    CounterID,
    StartDate,
    Sign,
    UserID
FROM table(test.visits);


SELECT
    StartDate,
    sumMerge(Visits)                AS Visits,
    uniq_merge(Users)                AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sumMerge(Visits)                AS Visits,
    uniq_merge(Users)                AS Users
FROM test.basic
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sum(Sign)                       AS Visits,
    uniq(UserID)                    AS Users
FROM table(test.visits)
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;
OPTIMIZE STREAM test.basic;


DROP STREAM test.visits_null;
DROP STREAM test.basic;
