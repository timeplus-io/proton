DROP STREAM IF EXISTS group_by_pk;

CREATE STREAM group_by_pk (k uint64, v uint64)
ENGINE = MergeTree ORDER BY k PARTITION BY v % 50;

INSERT INTO group_by_pk SELECT number / 100, number FROM numbers(1000);

SELECT sum(v) AS s FROM group_by_pk GROUP BY k ORDER BY s DESC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, max_block_size = 1;

SELECT '=======';

SELECT sum(v) AS s FROM group_by_pk GROUP BY k ORDER BY s DESC LIMIT 5
SETTINGS optimize_aggregation_in_order = 0, max_block_size = 1;

DROP STREAM IF EXISTS group_by_pk;
