-- Tags: zookeeper

DROP STREAM IF EXISTS table_for_alter;

SET replication_alter_partitions_sync = 2;

create stream table_for_alter
(
    `d` date,
    `a` string,
    `b` uint8,
    `x` string,
    `y` int8,
    `version` uint64,
    `sign` int8 DEFAULT 1
)
ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{database}/01526_alter_add/t1', '1', sign, version)
PARTITION BY y
ORDER BY d
SETTINGS index_granularity = 8192;

INSERT INTO table_for_alter VALUES(to_date('2019-10-01'), 'a', 1, 'aa', 1, 1, 1);

DETACH STREAM table_for_alter;

ATTACH STREAM table_for_alter;


SELECT * FROM table_for_alter;

ALTER STREAM table_for_alter ADD COLUMN order uint32, MODIFY ORDER BY (d, order);


DETACH STREAM table_for_alter;

ATTACH STREAM table_for_alter;

SELECT * FROM table_for_alter;

SHOW create stream table_for_alter;

ALTER STREAM table_for_alter ADD COLUMN datum uint32, MODIFY ORDER BY (d, order, datum);

INSERT INTO table_for_alter VALUES(to_date('2019-10-02'), 'b', 2, 'bb', 2, 2, 2, 1, 2);

SELECT * FROM table_for_alter ORDER BY d;

SHOW create stream table_for_alter;

DETACH STREAM table_for_alter;

ATTACH STREAM table_for_alter;

DROP STREAM IF EXISTS table_for_alter;
