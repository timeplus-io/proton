-- Tags: long, zookeeper

DROP STREAM IF EXISTS report;

create stream report
(
    `product` Enum8('IU' = 1, 'WS' = 2),
    `machine` string,
    `branch` string,
    `generated_time` DateTime
)
ENGINE = MergeTree
PARTITION BY (product, toYYYYMM(generated_time))
ORDER BY (product, machine, branch, generated_time);

INSERT INTO report VALUES ('IU', 'lada', '2101', to_datetime('1970-04-19 15:00:00'));

SELECT * FROM report  WHERE product = 'IU';

ALTER STREAM report MODIFY COLUMN product Enum8('IU' = 1, 'WS' = 2, 'PS' = 3);

SELECT * FROM report WHERE product = 'PS';

INSERT INTO report VALUES ('PS', 'jeep', 'Grand Cherokee', to_datetime('2005-10-03 15:00:00'));

SELECT * FROM report WHERE product = 'PS';

DETACH TABLE report;
ATTACH TABLE report;

SELECT * FROM report WHERE product = 'PS';

DROP STREAM IF EXISTS report;

DROP STREAM IF EXISTS replicated_report;

create stream replicated_report
(
    `product` Enum8('IU' = 1, 'WS' = 2),
    `machine` string,
    `branch` string,
    `generated_time` DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/01747_alter_partition_key/t', '1')
PARTITION BY (product, toYYYYMM(generated_time))
ORDER BY (product, machine, branch, generated_time);

INSERT INTO replicated_report VALUES ('IU', 'lada', '2101', to_datetime('1970-04-19 15:00:00'));

SELECT * FROM replicated_report  WHERE product = 'IU';

ALTER STREAM replicated_report MODIFY COLUMN product Enum8('IU' = 1, 'WS' = 2, 'PS' = 3) SETTINGS replication_alter_partitions_sync=2;

SELECT * FROM replicated_report WHERE product = 'PS';

INSERT INTO replicated_report VALUES ('PS', 'jeep', 'Grand Cherokee', to_datetime('2005-10-03 15:00:00'));

SELECT * FROM replicated_report WHERE product = 'PS';

DETACH TABLE replicated_report;
ATTACH TABLE replicated_report;

SELECT * FROM replicated_report WHERE product = 'PS';

DROP STREAM IF EXISTS replicated_report;
