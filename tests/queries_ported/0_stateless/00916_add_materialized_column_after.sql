DROP STREAM IF EXISTS add_materialized_column_after;

CREATE STREAM add_materialized_column_after (x uint32, z uint64) ENGINE MergeTree ORDER BY x;
ALTER STREAM add_materialized_column_after ADD COLUMN y string MATERIALIZED to_string(x) AFTER x;

DESC STREAM add_materialized_column_after;

DROP STREAM add_materialized_column_after;
