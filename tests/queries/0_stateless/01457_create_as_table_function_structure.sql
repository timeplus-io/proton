-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01457;

CREATE DATABASE test_01457;

create stream tmp (n int8) ENGINE=Memory;

create stream test_01457.tf_remote AS remote('localhost', currentDatabase(), 'tmp');
SHOW create stream test_01457.tf_remote;
create stream test_01457.tf_remote_explicit_structure (n uint64) AS remote('localhost', currentDatabase(), 'tmp');
SHOW create stream test_01457.tf_remote_explicit_structure;
create stream test_01457.tf_numbers (number string) AS numbers(1);
SHOW create stream test_01457.tf_numbers;
create stream test_01457.tf_merge AS merge(currentDatabase(), 'tmp');
SHOW create stream test_01457.tf_merge;

DROP STREAM tmp;

DETACH DATABASE test_01457;
ATTACH DATABASE test_01457;

-- To suppress "Structure does not match (...), implicit conversion will be done." message
SET send_logs_level='error';

create stream tmp (n int8) ENGINE=Memory;
INSERT INTO test_01457.tf_remote_explicit_structure VALUES ('42');
SELECT * FROM tmp;
TRUNCATE TABLE tmp;
INSERT INTO test_01457.tf_remote VALUES (0);

SELECT (*,).1 AS c, to_type_name(c) FROM tmp;
SELECT (*,).1 AS c, to_type_name(c) FROM test_01457.tf_remote;
SELECT (*,).1 AS c, to_type_name(c) FROM test_01457.tf_remote_explicit_structure;
SELECT (*,).1 AS c, to_type_name(c) FROM test_01457.tf_numbers;
SELECT (*,).1 AS c, to_type_name(c) FROM test_01457.tf_merge;

DROP DATABASE test_01457;

DROP STREAM tmp;
