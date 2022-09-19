-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

SET optimize_on_insert = 0;

SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS old_style;
create stream old_style(d date, x uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00754/old_style', 'r1', d, x, 8192);
ALTER STREAM old_style ADD COLUMN y uint32, MODIFY ORDER BY (x, y); -- { serverError 36 }
DROP STREAM old_style;

DROP STREAM IF EXISTS summing_r1;
DROP STREAM IF EXISTS summing_r2;
create stream summing_r1(x uint32, y uint32, val uint32) ENGINE ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test_00754/summing', 'r1') ORDER BY (x, y);
create stream summing_r2(x uint32, y uint32, val uint32) ENGINE ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test_00754/summing', 'r2') ORDER BY (x, y);

/* Can't add an expression with existing column to ORDER BY. */
ALTER STREAM summing_r1 MODIFY ORDER BY (x, y, -val); -- { serverError 36 }

/* Can't add an expression with existing column to ORDER BY. */
ALTER STREAM summing_r1 ADD COLUMN z uint32 DEFAULT x + 1, MODIFY ORDER BY (x, y, -z); -- { serverError 36 }

/* Can't add nonexistent column to ORDER BY. */
ALTER STREAM summing_r1 MODIFY ORDER BY (x, y, nonexistent); -- { serverError 47 }

/* Can't modyfy ORDER BY so that it is no longer a prefix of the PRIMARY KEY. */
ALTER STREAM summing_r1 MODIFY ORDER BY x; -- { serverError 36 }

INSERT INTO summing_r1(x, y, val) VALUES (1, 2, 10), (1, 2, 20);
SYSTEM SYNC REPLICA summing_r2;

ALTER STREAM summing_r1 ADD COLUMN z uint32 AFTER y, MODIFY ORDER BY (x, y, -z);

INSERT INTO summing_r1(x, y, z, val) values (1, 2, 1, 30), (1, 2, 2, 40), (1, 2, 2, 50);
SYSTEM SYNC REPLICA summing_r2;

SELECT '*** Check that the parts are sorted according to the new key. ***';
SELECT * FROM summing_r2 ORDER BY _part;

SELECT '*** Check that the rows are collapsed according to the new key. ***';
SELECT * FROM summing_r2 FINAL ORDER BY x, y, z;

SELECT '*** Check SHOW create stream ***';
SHOW create stream summing_r2;

DETACH TABLE summing_r2;
ALTER STREAM summing_r1 ADD COLUMN t uint32 AFTER z, MODIFY ORDER BY (x, y, t * t) SETTINGS replication_alter_partitions_sync = 2; -- { serverError 341 }
ATTACH TABLE summing_r2;

SYSTEM SYNC REPLICA summing_r2;

SELECT '*** Check SHOW create stream after offline ALTER ***';
SHOW create stream summing_r2;

DROP STREAM summing_r1;
DROP STREAM summing_r2;
