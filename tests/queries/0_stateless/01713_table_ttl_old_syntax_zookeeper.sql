-- Tags: zookeeper

DROP STREAM IF EXISTS ttl_table;

create stream ttl_table
(
    date date,
    value uint64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
TTL date + INTERVAL 2 MONTH; --{ serverError 36 }

create stream ttl_table
(
    date date,
    value uint64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
PARTITION BY date; --{ serverError 42 }

create stream ttl_table
(
    date date,
    value uint64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01713_table_ttl', '1', date, date, 8192)
ORDER BY value; --{ serverError 42 }

SELECT 1;

DROP STREAM IF EXISTS ttl_table;
