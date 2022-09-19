DROP STREAM IF EXISTS t_sparse_detach;

create stream t_sparse_detach(id uint64, s string)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse_detach SELECT number, number % 20 = 0 ? to_string(number) : '' FROM numbers(10000);
INSERT INTO t_sparse_detach SELECT number, number % 20 = 0 ? to_string(number) : '' FROM numbers(10000);

OPTIMIZE STREAM t_sparse_detach FINAL;

SELECT count() FROM t_sparse_detach WHERE s != '';

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse_detach' AND database = currentDatabase() AND active
ORDER BY column;

DETACH TABLE t_sparse_detach;
ATTACH TABLE t_sparse_detach;

SELECT count() FROM t_sparse_detach WHERE s != '';

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse_detach' AND database = currentDatabase() AND active
ORDER BY column;

TRUNCATE TABLE t_sparse_detach;

ALTER STREAM t_sparse_detach
    MODIFY SETTING vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_sparse_detach SELECT number, number % 20 = 0 ? to_string(number) : '' FROM numbers(10000);
INSERT INTO t_sparse_detach SELECT number, number % 20 = 0 ? to_string(number) : '' FROM numbers(10000);

OPTIMIZE STREAM t_sparse_detach FINAL;

SELECT count() FROM t_sparse_detach WHERE s != '';

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse_detach' AND database = currentDatabase() AND active
ORDER BY column;

DETACH TABLE t_sparse_detach;
ATTACH TABLE t_sparse_detach;

SELECT count() FROM t_sparse_detach WHERE s != '';

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse_detach' AND database = currentDatabase() AND active
ORDER BY column;

DROP STREAM t_sparse_detach;
