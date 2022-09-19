-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

DROP STREAM IF EXISTS alter_00121;
create stream alter_00121 (d date, x uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/alter_00121/t1', 'r1', d, (d), 8192);

INSERT INTO alter_00121 VALUES ('2014-01-01', 1);
ALTER STREAM alter_00121 DROP COLUMN x;

DROP STREAM alter_00121;

create stream alter_00121 (d date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/alter_00121/t2', 'r1', d, (d), 8192);

INSERT INTO alter_00121 VALUES ('2014-01-01');
SELECT * FROM alter_00121 ORDER BY d;

ALTER STREAM alter_00121 ADD COLUMN x uint8;

INSERT INTO alter_00121 VALUES ('2014-02-01', 1);
SELECT * FROM alter_00121 ORDER BY d;

ALTER STREAM alter_00121 DROP COLUMN x;
SELECT * FROM alter_00121 ORDER BY d;

DROP STREAM alter_00121;
