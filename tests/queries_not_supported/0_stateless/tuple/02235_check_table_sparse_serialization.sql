DROP STREAM IF EXISTS t_sparse_02235;

CREATE STREAM t_sparse_02235 (a uint8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

SYSTEM STOP MERGES t_sparse_02235;

INSERT INTO t_sparse_02235 SELECT 1 FROM numbers(1000);
INSERT INTO t_sparse_02235 SELECT 0 FROM numbers(1000);

SELECT name, column, serialization_kind FROM system.parts_columns
WHERE database = current_database() AND stream = 't_sparse_02235'
ORDER BY name, column;

SET check_query_single_value_result = 0;
CHECK STREAM t_sparse_02235;

DROP STREAM t_sparse_02235;
