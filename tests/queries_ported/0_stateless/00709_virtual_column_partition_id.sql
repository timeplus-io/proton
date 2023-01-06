DROP STREAM IF EXISTS partition_id;

create stream IF NOT EXISTS partition_id (d date DEFAULT '2000-01-01', x uint64) ENGINE = MergeTree(d, x, 5);

INSERT INTO partition_id VALUES (100, 1), (200, 2), (300, 3);

SELECT _partition_id FROM partition_id ORDER BY x;

DROP STREAM IF EXISTS partition_id;

