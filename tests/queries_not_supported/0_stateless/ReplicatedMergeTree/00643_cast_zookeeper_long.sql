-- Tags: long, zookeeper

SET database_atomic_wait_for_drop_and_detach_synchronously=1;

DROP STREAM IF EXISTS cast1;
DROP STREAM IF EXISTS cast2;

create stream cast1
(
    x uint8,
    e enum8
    (
        'hello' = 1,
        'world' = 2
    )
    DEFAULT
    CAST
    (
        x
        AS
        enum8
        (
            'hello' = 1,
            'world' = 2
        )
    )
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00643/cast', 'r1') ORDER BY e;

SHOW create stream cast1 FORMAT TSVRaw;
DESC STREAM cast1;

INSERT INTO cast1 (x) VALUES (1);
SELECT * FROM cast1;

create stream cast2 AS cast1 ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00643/cast', 'r2') ORDER BY e;

SYSTEM SYNC REPLICA cast2;

SELECT * FROM cast2;

DROP STREAM cast1;
DROP STREAM cast2;
