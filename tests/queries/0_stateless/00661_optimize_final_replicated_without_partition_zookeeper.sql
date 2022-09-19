-- Tags: replica

SET optimize_on_insert = 0;

DROP STREAM IF EXISTS partitioned_by_tuple_replica1_00661;
DROP STREAM IF EXISTS partitioned_by_tuple_replica2_00661;
create stream partitioned_by_tuple_replica1_00661(d date, x uint8, w string, y uint8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/partitioned_by_tuple_00661', '1') PARTITION BY (d, x) ORDER BY (d, x, w);
create stream partitioned_by_tuple_replica2_00661(d date, x uint8, w string, y uint8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/partitioned_by_tuple_00661', '2') PARTITION BY (d, x) ORDER BY (d, x, w);

INSERT INTO partitioned_by_tuple_replica1_00661 VALUES ('2000-01-02', 1, 'first', 3);
INSERT INTO partitioned_by_tuple_replica1_00661 VALUES ('2000-01-01', 2, 'first', 2);
INSERT INTO partitioned_by_tuple_replica1_00661 VALUES ('2000-01-01', 1, 'first', 1), ('2000-01-01', 1, 'first', 2);

OPTIMIZE STREAM partitioned_by_tuple_replica1_00661;

SYSTEM SYNC REPLICA partitioned_by_tuple_replica2_00661;
SELECT * FROM partitioned_by_tuple_replica2_00661 ORDER BY d, x, w, y;

OPTIMIZE STREAM partitioned_by_tuple_replica1_00661 FINAL;

SYSTEM SYNC REPLICA partitioned_by_tuple_replica2_00661;
SELECT * FROM partitioned_by_tuple_replica2_00661 ORDER BY d, x, w, y;

DROP STREAM partitioned_by_tuple_replica1_00661;
DROP STREAM partitioned_by_tuple_replica2_00661;
