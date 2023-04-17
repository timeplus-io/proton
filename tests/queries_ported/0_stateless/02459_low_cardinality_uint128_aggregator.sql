SET allow_suspicious_low_cardinality_types = 1;
-- LC uint128
CREATE STREAM group_by_pk_lc_uint128 (`k` low_cardinality(uint128), `v` uint32) ENGINE = MergeTree ORDER BY k PARTITION BY v%50;
INSERT INTO group_by_pk_lc_uint128 SELECT number / 100, number FROM numbers(1000);
SELECT k, sum(v) AS s FROM group_by_pk_lc_uint128 GROUP BY k ORDER BY k ASC LIMIT 1024 SETTINGS optimize_aggregation_in_order = 1;
-- LC uint256
CREATE STREAM group_by_pk_lc_uint256 (`k` low_cardinality(uint256), `v` uint32) ENGINE = MergeTree ORDER BY k PARTITION BY v%50;
INSERT INTO group_by_pk_lc_uint256 SELECT number / 100, number FROM numbers(1000);
SELECT k, sum(v) AS s FROM group_by_pk_lc_uint256 GROUP BY k ORDER BY k ASC LIMIT 1024 SETTINGS optimize_aggregation_in_order = 1;
