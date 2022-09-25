-- this test cannot pass without the new DFA matching algorithm of sequenceMatch

DROP STREAM IF EXISTS sequence;

create stream sequence
(
    userID uint64,
    eventType Enum8('A' = 1, 'B' = 2, 'C' = 3, 'D' = 4),
    EventTime uint64
)
;

INSERT INTO sequence SELECT 1, number = 0 ? 'A' : (number < 1000000 ? 'B' : 'C'), number FROM numbers(1000001);
INSERT INTO sequence SELECT 1, 'D', 1e14;

SELECT 'ABC'
FROM sequence
GROUP BY userID
HAVING sequenceMatch('(?1).*(?2).*(?3)')(to_datetime(EventTime), eventType = 'A', eventType = 'B', eventType = 'C');

SELECT 'ABA'
FROM sequence
GROUP BY userID
HAVING sequenceMatch('(?1).*(?2).*(?3)')(to_datetime(EventTime), eventType = 'A', eventType = 'B', eventType = 'A');

SELECT 'ABBC'
FROM sequence
GROUP BY userID
HAVING sequenceMatch('(?1).*(?2).*(?3).*(?4)')(EventTime, eventType = 'A', eventType = 'B', eventType = 'B',eventType = 'C');

SELECT 'CD'
FROM sequence
GROUP BY userID
HAVING sequenceMatch('(?1)(?t>=10000000000000)(?2)')(EventTime, eventType = 'C', eventType = 'D');

DROP STREAM sequence;
