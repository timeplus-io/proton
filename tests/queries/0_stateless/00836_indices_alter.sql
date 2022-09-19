DROP STREAM IF EXISTS minmax_idx;
DROP STREAM IF EXISTS minmax_idx2;


create stream minmax_idx
(
    u64 uint64,
    i32 int32
) ENGINE = MergeTree()
ORDER BY u64;

INSERT INTO minmax_idx VALUES (1, 2);

ALTER STREAM minmax_idx ADD INDEX idx1 u64 * i32 TYPE minmax GRANULARITY 10;
ALTER STREAM minmax_idx ADD INDEX idx2 u64 + i32 TYPE minmax GRANULARITY 10;
ALTER STREAM minmax_idx ADD INDEX idx3 (u64 - i32) TYPE minmax GRANULARITY 10 AFTER idx1;

SHOW create stream minmax_idx;

SELECT * FROM minmax_idx WHERE u64 * i32 = 2;

INSERT INTO minmax_idx VALUES (1, 2);
INSERT INTO minmax_idx VALUES (1, 2);
INSERT INTO minmax_idx VALUES (1, 2);
INSERT INTO minmax_idx VALUES (1, 2);
INSERT INTO minmax_idx VALUES (1, 2);

SELECT * FROM minmax_idx WHERE u64 * i32 = 2;

ALTER STREAM minmax_idx DROP INDEX idx1;

SHOW create stream minmax_idx;

SELECT * FROM minmax_idx WHERE u64 * i32 = 2;

ALTER STREAM minmax_idx DROP INDEX idx2;
ALTER STREAM minmax_idx DROP INDEX idx3;

SHOW create stream minmax_idx;

ALTER STREAM minmax_idx ADD INDEX idx1 (u64 * i32) TYPE minmax GRANULARITY 10;

SHOW create stream minmax_idx;

SELECT * FROM minmax_idx WHERE u64 * i32 = 2;


create stream minmax_idx2
(
    u64 uint64,
    i32 int32,
    INDEX idx1 (u64 + i32) TYPE minmax GRANULARITY 10,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 10
) ENGINE = MergeTree()
ORDER BY u64;

INSERT INTO minmax_idx2 VALUES (1, 2);
INSERT INTO minmax_idx2 VALUES (1, 2);

SELECT * FROM minmax_idx2 WHERE u64 * i32 = 2;

ALTER STREAM minmax_idx2 DROP INDEX idx1, DROP INDEX idx2;

SHOW create stream minmax_idx2;

SELECT * FROM minmax_idx2 WHERE u64 * i32 = 2;

DROP STREAM minmax_idx;
DROP STREAM minmax_idx2;
