-- Tags: long

CREATE TEMPORARY STREAM test ("\\" string DEFAULT '\r\n\t\\' || '
');

INSERT INTO test VALUES ('Hello, world!');
INSERT INTO test ("\\") VALUES ('Hello, world!');

SELECT * FROM test;

DROP TEMPORARY TABLE test;
DROP STREAM IF EXISTS test;

create stream test (x uint64, "\\" string DEFAULT '\r\n\t\\' || '
') ENGINE = MergeTree ORDER BY x;

INSERT INTO test (x) VALUES (1);

SELECT * FROM test;

DROP STREAM test;

DROP STREAM IF EXISTS test_r1;
DROP STREAM IF EXISTS test_r2;

create stream test_r1 (x uint64, "\\" string DEFAULT '\r\n\t\\' || '
') ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_01669', 'r1') ORDER BY "\\";

INSERT INTO test_r1 ("\\") VALUES ('\\');

create stream test_r2 (x uint64, "\\" string DEFAULT '\r\n\t\\' || '
') ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_01669', 'r2') ORDER BY "\\";

SYSTEM SYNC REPLICA test_r2;

SELECT '---';
SELECT * FROM test_r1;
SELECT '---';
SELECT * FROM test_r2;

DROP STREAM test_r1;
DROP STREAM test_r2;
