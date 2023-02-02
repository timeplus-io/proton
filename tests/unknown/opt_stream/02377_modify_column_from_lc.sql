DROP STREAM IF EXISTS t_modify_from_lc_1;
DROP STREAM IF EXISTS t_modify_from_lc_2;

SET allow_suspicious_low_cardinality_types = 1;

CREATE STREAM t_modify_from_lc_1
(
    id uint64,
    a low_cardinality(uint32) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

CREATE STREAM t_modify_from_lc_2
(
    id uint64,
    a low_cardinality(uint32) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_modify_from_lc_1 SELECT number, number FROM numbers(100000);
INSERT INTO t_modify_from_lc_2 SELECT number, number FROM numbers(100000);

OPTIMIZE STREAM t_modify_from_lc_1 FINAL;
OPTIMIZE STREAM t_modify_from_lc_2 FINAL;

ALTER STREAM t_modify_from_lc_1 MODIFY COLUMN a uint32;

-- Check that dictionary of low_cardinality is actually
-- dropped and total size on disk is reduced.
WITH group_array((table, bytes))::Map(string, uint64) AS stats
SELECT
    length(stats), stats['t_modify_from_lc_1'] < stats['t_modify_from_lc_2']
FROM
(
    SELECT stream, sum(bytes_on_disk) AS bytes FROM system.parts
    WHERE database = currentDatabase() AND stream LIKE 't_modify_from_lc%' AND active
    GROUP BY stream
);

DROP STREAM IF EXISTS t_modify_from_lc_1;
DROP STREAM IF EXISTS t_modify_from_lc_2;
