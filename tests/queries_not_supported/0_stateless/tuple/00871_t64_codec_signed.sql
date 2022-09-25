DROP STREAM IF EXISTS t64;

create stream t64
(
    i8 int8,
    t_i8 int8 Codec(T64, LZ4),
    i16 int16,
    t_i16 int16 Codec(T64, LZ4),
    i32 int32,
    t_i32 int32 Codec(T64, LZ4),
    i64 int64,
    t_i64 int64 Codec(T64, LZ4)
) ENGINE MergeTree() ORDER BY tuple();

INSERT INTO t64 SELECT to_int32(number)-1 AS x, x, x, x, x, x, x, x FROM numbers(2);
INSERT INTO t64 SELECT to_int32(number)-1 AS x, x, x, x, x, x, x, x FROM numbers(3);
INSERT INTO t64 SELECT 42 AS x, x, x, x, x, x, x, x FROM numbers(4);

SELECT * FROM t64 ORDER BY i64;

INSERT INTO t64 SELECT (intExp2(8) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
SELECT i8, t_i8 FROM t64 WHERE i8 != t_i8;

INSERT INTO t64 SELECT number AS x, x, x, x, x, x, x, x FROM numbers(intExp2(8));
INSERT INTO t64 SELECT number AS x, x, x, x, x, x, x, x FROM numbers(intExp2(9));
SELECT * FROM t64 WHERE i8 != t_i8;
SELECT * FROM t64 WHERE i16 != t_i16;
SELECT * FROM t64 WHERE i32 != t_i32;
SELECT * FROM t64 WHERE i64 != t_i64;

INSERT INTO t64 SELECT (intExp2(16) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(16) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(11);
INSERT INTO t64 SELECT (intExp2(16) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(64);
INSERT INTO t64 SELECT (intExp2(16) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
INSERT INTO t64 SELECT (intExp2(16) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(16)) + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(16)) + number) AS x, x, x, x, x, x, x, x FROM numbers(11);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(16)) + number) AS x, x, x, x, x, x, x, x FROM numbers(64);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(16)) + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
INSERT INTO t64 SELECT (1 - to_int64(intExp2(16)) + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
SELECT * FROM t64 WHERE i8 != t_i8;
SELECT * FROM t64 WHERE i16 != t_i16;
SELECT * FROM t64 WHERE i32 != t_i32;
SELECT * FROM t64 WHERE i64 != t_i64;

INSERT INTO t64 SELECT (intExp2(24) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(24) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(11);
INSERT INTO t64 SELECT (intExp2(24) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(128);
INSERT INTO t64 SELECT (intExp2(24) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(129);
INSERT INTO t64 SELECT (intExp2(24) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(129);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(24)) + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(24)) + number) AS x, x, x, x, x, x, x, x FROM numbers(11);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(24)) + number) AS x, x, x, x, x, x, x, x FROM numbers(128);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(24)) + number) AS x, x, x, x, x, x, x, x FROM numbers(129);
INSERT INTO t64 SELECT (1 - to_int64(intExp2(24)) + number) AS x, x, x, x, x, x, x, x FROM numbers(129);
SELECT * FROM t64 WHERE i8 != t_i8;
SELECT * FROM t64 WHERE i16 != t_i16;
SELECT * FROM t64 WHERE i32 != t_i32;
SELECT * FROM t64 WHERE i64 != t_i64;

INSERT INTO t64 SELECT (intExp2(32) - 2 + number) AS x, x, x, x, x, x, x, x FROM numbers(2);
INSERT INTO t64 SELECT (intExp2(32) - 2 + number) AS x, x, x, x, x, x, x, x FROM numbers(3);
INSERT INTO t64 SELECT (intExp2(32) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(64);
INSERT INTO t64 SELECT (intExp2(32) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
INSERT INTO t64 SELECT (intExp2(32) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(32)) + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(32)) + number) AS x, x, x, x, x, x, x, x FROM numbers(11);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(32)) + number) AS x, x, x, x, x, x, x, x FROM numbers(64);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(32)) + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
INSERT INTO t64 SELECT (1 - to_int64(intExp2(32)) + number) AS x, x, x, x, x, x, x, x FROM numbers(65);
SELECT * FROM t64 WHERE i8 != t_i8;
SELECT * FROM t64 WHERE i16 != t_i16;
SELECT * FROM t64 WHERE i32 != t_i32;
SELECT * FROM t64 WHERE i64 != t_i64;

INSERT INTO t64 SELECT (intExp2(40) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(40) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (intExp2(40) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(512);
INSERT INTO t64 SELECT (intExp2(40) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(513);
INSERT INTO t64 SELECT (intExp2(40) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(513);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(40)) + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(40)) + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(40)) + number) AS x, x, x, x, x, x, x, x FROM numbers(512);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(40)) + number) AS x, x, x, x, x, x, x, x FROM numbers(513);
INSERT INTO t64 SELECT (1 - to_int64(intExp2(40)) + number) AS x, x, x, x, x, x, x, x FROM numbers(513);
SELECT * FROM t64 WHERE i8 != t_i8;
SELECT * FROM t64 WHERE i16 != t_i16;
SELECT * FROM t64 WHERE i32 != t_i32;
SELECT * FROM t64 WHERE i64 != t_i64;

INSERT INTO t64 SELECT (intExp2(48) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(48) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (intExp2(48) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(1024);
INSERT INTO t64 SELECT (intExp2(48) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(1025);
INSERT INTO t64 SELECT (intExp2(48) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(1025);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(48)) + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(48)) + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(48)) + number) AS x, x, x, x, x, x, x, x FROM numbers(1024);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(48)) + number) AS x, x, x, x, x, x, x, x FROM numbers(1025);
INSERT INTO t64 SELECT (1 - to_int64(intExp2(48)) + number) AS x, x, x, x, x, x, x, x FROM numbers(1025);
SELECT * FROM t64 WHERE i8 != t_i8;
SELECT * FROM t64 WHERE i16 != t_i16;
SELECT * FROM t64 WHERE i32 != t_i32;
SELECT * FROM t64 WHERE i64 != t_i64;

INSERT INTO t64 SELECT (intExp2(56) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (intExp2(56) - 10 + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (intExp2(56) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(2048);
INSERT INTO t64 SELECT (intExp2(56) - 64 + number) AS x, x, x, x, x, x, x, x FROM numbers(2049);
INSERT INTO t64 SELECT (intExp2(56) - 1 + number) AS x, x, x, x, x, x, x, x FROM numbers(2049);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(56)) + number) AS x, x, x, x, x, x, x, x FROM numbers(10);
INSERT INTO t64 SELECT (10 - to_int64(intExp2(56)) + number) AS x, x, x, x, x, x, x, x FROM numbers(20);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(56)) + number) AS x, x, x, x, x, x, x, x FROM numbers(2048);
INSERT INTO t64 SELECT (64 - to_int64(intExp2(56)) + number) AS x, x, x, x, x, x, x, x FROM numbers(2049);
INSERT INTO t64 SELECT (1 - to_int64(intExp2(56)) + number) AS x, x, x, x, x, x, x, x FROM numbers(2049);
SELECT * FROM t64 WHERE i8 != t_i8;
SELECT * FROM t64 WHERE i16 != t_i16;
SELECT * FROM t64 WHERE i32 != t_i32;
SELECT * FROM t64 WHERE i64 != t_i64;

OPTIMIZE STREAM t64 FINAL;

SELECT * FROM t64 WHERE i8 != t_i8;
SELECT * FROM t64 WHERE i16 != t_i16;
SELECT * FROM t64 WHERE i32 != t_i32;
SELECT * FROM t64 WHERE i64 != t_i64;

DROP STREAM t64;
