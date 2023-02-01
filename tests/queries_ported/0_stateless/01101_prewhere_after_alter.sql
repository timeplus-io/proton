DROP STREAM IF EXISTS test_a;
DROP STREAM IF EXISTS test_b;


CREATE STREAM test_a
(
    OldColumn string DEFAULT '',
    EventDate Date DEFAULT to_date(EventTime),
    EventTime DateTime
) ENGINE = MergeTree(EventDate, EventTime, 8192);

CREATE STREAM test_b
(
    OldColumn string DEFAULT '',
    NewColumn string DEFAULT '',
    EventDate Date DEFAULT to_date(EventTime),
    EventTime DateTime
) ENGINE = MergeTree(EventDate, EventTime, 8192);

INSERT INTO test_a (OldColumn, EventTime) VALUES('1', now());

INSERT INTO test_b (OldColumn, NewColumn, EventTime) VALUES('1', '1a', now());
INSERT INTO test_b (OldColumn, NewColumn, EventTime) VALUES('2', '2a', now());

ALTER STREAM test_a ADD COLUMN NewColumn string DEFAULT '' AFTER OldColumn;

INSERT INTO test_a (OldColumn, NewColumn, EventTime) VALUES('2', '2a', now());

SELECT NewColumn
FROM test_a
INNER JOIN
(SELECT OldColumn, NewColumn FROM test_b) s
Using OldColumn
PREWHERE NewColumn != '';

DROP STREAM test_a;
DROP STREAM test_b;
