-- Tags: long, zookeeper, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

DROP STREAM IF EXISTS deduplication;
create stream deduplication (d date DEFAULT '2015-01-01', x int8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00226/deduplication', 'r1', d, x, 1);

INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);
INSERT INTO deduplication (x) VALUES (1);

SELECT * FROM deduplication;

DETACH STREAM deduplication;
ATTACH STREAM deduplication;

SELECT * FROM deduplication;

DROP STREAM deduplication;
