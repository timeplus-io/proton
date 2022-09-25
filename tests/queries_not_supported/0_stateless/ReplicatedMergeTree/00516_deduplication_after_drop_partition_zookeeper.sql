-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed
SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS deduplication_by_partition;
create stream deduplication_by_partition(d date, x uint32) ENGINE =
    ReplicatedMergeTree('/clickhouse/tables/{database}/test_00516/deduplication_by_partition', 'r1', d, x, 8192);

INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 1);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 1);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);
SELECT sleep(3);

SELECT '*** Before DROP PARTITION ***';

SELECT * FROM deduplication_by_partition ORDER BY d, x;

ALTER STREAM deduplication_by_partition DROP PARTITION 200001;

SELECT '*** After DROP PARTITION ***';

SELECT * FROM deduplication_by_partition ORDER BY d, x;

INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 1);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 1);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-01-01', 4);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);
INSERT INTO deduplication_by_partition(d, x) VALUES ('2000-02-01', 6), ('2000-02-01', 7);
SELECT sleep(3);
SELECT '*** After INSERT ***';

SELECT * FROM deduplication_by_partition ORDER BY d, x;

DROP STREAM deduplication_by_partition;
