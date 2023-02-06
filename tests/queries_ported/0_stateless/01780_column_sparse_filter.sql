DROP STREAM IF EXISTS t_sparse;

CREATE STREAM t_sparse (id uint64, u uint64, s string)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse SELECT
    number,
    if (number % 20 = 0, number, 0),
    if (number % 50 = 0, to_string(number), '')
FROM numbers(1, 100000);

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse' AND database = current_database()
ORDER BY column, serialization_kind;

SELECT count() FROM t_sparse WHERE u > 0;
SELECT count() FROM t_sparse WHERE not_empty(s);

SYSTEM STOP MERGES t_sparse;

INSERT INTO t_sparse SELECT
    number, number, to_string(number)
FROM numbers (1, 100000);

SELECT column, serialization_kind FROM system.parts_columns
WHERE table = 't_sparse' AND database = current_database()
ORDER BY column, serialization_kind;

SELECT count() FROM t_sparse WHERE u > 0;
SELECT count() FROM t_sparse WHERE not_empty(s);

DROP STREAM t_sparse;
