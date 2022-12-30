create stream visits
(
    `CounterID` uint32,
    `StartDate` date,
    `StartTime` DateTime,
    `GoalsID` array(uint32),
    `Sign` int8
)
ENGINE = Null;


CREATE MATERIALIZED VIEW goal_view TO goal
(
    `CounterID` uint32,
    `StartDate` date,
    `GoalID` uint32,
    `Visits` aggregate_function(sum_if, int8, uint8),
    `GoalReaches` aggregate_function(sum, int8)
) AS
SELECT
    CounterID,
    StartDate,
    GoalID,
    sum_ifState(Sign, _uniq = 1) AS Visits,
    sumState(Sign) AS GoalReaches
FROM visits
ARRAY JOIN
    GoalsID AS GoalID,
    array_enumerate_uniq(GoalsID) AS _uniq
GROUP BY
    CounterID,
    StartDate,
    GoalID
ORDER BY
    CounterID ASC,
    StartDate ASC,
    GoalID ASC;

create stream goal
(
     `CounterID` uint32,
     `StartDate` date,
     `GoalID` uint32,
     `Visits` aggregate_function(sum_if, int8, uint8),
     `GoalReaches` aggregate_function(sum, int8)
) ENGINE = AggregatingMergeTree PARTITION BY to_start_of_month(StartDate) ORDER BY (CounterID, StartDate, GoalID) SETTINGS index_granularity = 256;

INSERT INTO visits (`CounterID`,`StartDate`,`StartTime`,`Sign`,`GoalsID`) VALUES (1, to_date('2000-01-01'), to_datetime(to_date('2000-01-01')), 1, [1]);

DROP STREAM goal;
DROP STREAM goal_view;
DROP STREAM visits;
