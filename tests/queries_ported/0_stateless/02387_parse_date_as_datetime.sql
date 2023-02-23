CREATE TEMPORARY STREAM test (`i` int64, `d` DateTime) ENGINE = Memory();
INSERT INTO test FORMAT JSONEachRow {"i": 123, "d": "2022-05-03"};
INSERT INTO test FORMAT JSONEachRow {"i": 456, "d": "2022-05-03 01:02:03"};
SELECT * FROM test ORDER BY i;

DROP STREAM test;

CREATE TEMPORARY STREAM test (`i` int64, `d` DateTime64) ENGINE = Memory();
INSERT INTO test FORMAT JSONEachRow {"i": 123, "d": "2022-05-03"};
INSERT INTO test FORMAT JSONEachRow {"i": 456, "d": "2022-05-03 01:02:03"};
SELECT * FROM test ORDER BY i;

DROP STREAM test;
