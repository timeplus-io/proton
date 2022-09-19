DROP STREAM IF EXISTS testmt;

create stream testmt (`CounterID` uint64, `value` string) ENGINE = MergeTree() ORDER BY CounterID;

INSERT INTO testmt VALUES (1, '1'), (2, '2');

SELECT array_join([CounterID NOT IN (2)]) AS counter FROM testmt WHERE CounterID IN (2) GROUP BY counter;

DROP STREAM testmt;
