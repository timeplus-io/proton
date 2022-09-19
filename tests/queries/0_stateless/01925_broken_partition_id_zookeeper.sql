-- Tags: zookeeper

DROP STREAM IF EXISTS broken_partition;

create stream broken_partition
(
    date date,
    key uint64
)
ENGINE = ReplicatedMergeTree('/clickhouse/test_01925_{database}/rmt', 'r1')
ORDER BY tuple()
PARTITION BY date;

ALTER STREAM broken_partition DROP PARTITION ID '20210325_0_13241_6_12747'; --{serverError 248}

ALTER STREAM broken_partition DROP PARTITION ID '20210325_0_13241_6_12747'; --{serverError 248}

DROP STREAM IF EXISTS broken_partition;

DROP STREAM IF EXISTS old_partition_key;

create stream old_partition_key (sd date, dh uint64, ak uint32, ed date) ENGINE=MergeTree(sd, dh, (ak, ed, dh), 8192);

ALTER STREAM old_partition_key DROP PARTITION ID '20210325_0_13241_6_12747'; --{serverError 248}

ALTER STREAM old_partition_key DROP PARTITION ID '202103';

DROP STREAM old_partition_key;
