-- Tags: long, zookeeper

DROP STREAM IF EXISTS i20203_1;
DROP STREAM IF EXISTS i20203_2;

create stream i20203_1 (a int8)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/01715_background_checker_i20203', 'r1')
ORDER BY tuple();

create stream i20203_2 (a int8)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/01715_background_checker_i20203', 'r2')
ORDER BY tuple();

DETACH STREAM i20203_2;
INSERT INTO i20203_1 VALUES (2);

DETACH STREAM i20203_1;
ATTACH STREAM i20203_2;

-- sleep 10 seconds
SELECT number from numbers(10) where sleepEachRow(1) Format Null;

SELECT num_tries < 50
FROM system.replication_queue
WHERE table = 'i20203_2' AND database = currentDatabase();

ATTACH STREAM i20203_1;

DROP STREAM IF EXISTS i20203_1;
DROP STREAM IF EXISTS i20203_2;
