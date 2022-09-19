SET mutations_sync = 2;

DROP STREAM IF EXISTS t_sparse_alter;

create stream t_sparse_alter (id uint64, u uint64, s string)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO t_sparse_alter SELECT
    number,
    if (number % 11 = 0, number, 0),
    if (number % 13 = 0, to_string(number), '')
FROM numbers(2000);

SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse_alter' AND active ORDER BY name;

SELECT uniqExact(u), uniqExact(s) FROM t_sparse_alter;

ALTER STREAM t_sparse_alter DROP COLUMN s, RENAME COLUMN u TO t;
ALTER STREAM t_sparse_alter MODIFY COLUMN t uint16;

SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse_alter' AND active ORDER BY name;

SELECT uniqExact(t) FROM t_sparse_alter;

DROP STREAM t_sparse_alter;
