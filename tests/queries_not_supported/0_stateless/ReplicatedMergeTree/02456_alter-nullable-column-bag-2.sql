DROP STREAM IF EXISTS t1 SYNC;
CREATE STREAM t1  (v uint64) ENGINE=ReplicatedMergeTree('/test/tables/{database}/test/t1', 'r1') ORDER BY v PARTITION BY v;
INSERT INTO t1 values(1);
ALTER STREAM t1 ADD COLUMN s string;
INSERT INTO t1 values(1, '1');
ALTER STREAM t1 MODIFY COLUMN s nullable(string);
-- SELECT _part, * FROM t1;

alter stream t1 detach partition 1;

SELECT _part, * FROM t1;
--0 rows in set. Elapsed: 0.001 sec.

alter stream t1 attach partition 1;
select count() from t1;

