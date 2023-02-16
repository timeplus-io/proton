SET allow_suspicious_low_cardinality_types = 1;

DROP STREAM IF EXISTS t_summing_lc;

CREATE STREAM t_summing_lc
(
    `key` uint32,
    `val` low_cardinality(uint32),
    `date` DateTime
)
ENGINE = SummingMergeTree(val)
PARTITION BY date
ORDER BY key;

INSERT INTO t_summing_lc VALUES (1, 1, '2020-01-01'), (2, 1, '2020-01-02'), (1, 5, '2020-01-01'), (2, 5, '2020-01-02');

OPTIMIZE TABLE t_summing_lc FINAL;
SELECT * FROM t_summing_lc ORDER BY key;

DROP STREAM t_summing_lc;
