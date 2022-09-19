-- It's Ok to CLEAR column when there are columns with default expression depending on it.
-- But it's not Ok to DROP such column.

DROP STREAM IF EXISTS test;
create stream test (x uint8, y uint8 DEFAULT x + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
SELECT * FROM test ORDER BY x, y;
ALTER STREAM test CLEAR COLUMN x;
SELECT * FROM test ORDER BY x, y;
ALTER STREAM test DROP COLUMN x; -- { serverError 44 }
DROP STREAM test;

DROP STREAM IF EXISTS test;
create stream test (x uint8, y uint8 MATERIALIZED x + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
SELECT x, y FROM test ORDER BY x, y;
ALTER STREAM test CLEAR COLUMN x;
SELECT x, y FROM test ORDER BY x, y;
ALTER STREAM test DROP COLUMN x; -- { serverError 44 }
DROP STREAM test;

DROP STREAM IF EXISTS test;
create stream test (x uint8, y uint8 ALIAS x + 1, z string DEFAULT 'Hello') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
SELECT x, y FROM test ORDER BY x, y;
ALTER STREAM test CLEAR COLUMN x;
SELECT x, y FROM test ORDER BY x, y;
ALTER STREAM test DROP COLUMN x; -- { serverError 44 }
DROP STREAM test;


-- The original report from Mikhail Petrov
DROP STREAM IF EXISTS Test;
create stream Test (impression_id string,impression_id_compressed FixedString(16) DEFAULT UUIDStringToNum(substring(impression_id, 1, 36)), impression_id_hashed uint16 DEFAULT reinterpretAsUInt16(impression_id_compressed), event_date date ) ENGINE = MergeTree(event_date, impression_id_hashed, (event_date, impression_id_hashed), 8192);
alter stream Test clear column impression_id in partition '202001';
DROP STREAM Test;
