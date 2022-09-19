SELECT
    to_type_name(ui64), to_type_name(i64),
    to_type_name(ui32), to_type_name(i32),
    to_type_name(ui16), to_type_name(i16),
    to_type_name(ui8), to_type_name(i8)
FROM generateRandom('ui64 uint64, i64 int64, ui32 uint32, i32 int32, ui16 uint16, i16 Int16, ui8 uint8, i8 int8')
LIMIT 1;
SELECT
    ui64, i64,
    ui32, i32,
    ui16, i16,
    ui8, i8
FROM generateRandom('ui64 uint64, i64 int64, ui32 uint32, i32 int32, ui16 uint16, i16 Int16, ui8 uint8, i8 int8', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)
FROM generateRandom('i Enum8(\'hello\' = 1, \'world\' = 5)')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Enum8(\'hello\' = 1, \'world\' = 5)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)
FROM generateRandom('i array(Nullable(Enum8(\'hello\' = 1, \'world\' = 5)))')
LIMIT 1;
SELECT
  i
FROM generateRandom('i array(Nullable(Enum8(\'hello\' = 1, \'world\' = 5)))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)s
FROM generateRandom('i Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200))')
LIMIT 1;
SELECT
  i
FROM generateRandom('i Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
to_type_name(d), to_type_name(dt), to_type_name(dtm)
FROM generateRandom('d date, dt datetime(\'Europe/Moscow\'), dtm datetime(\'Europe/Moscow\')')
LIMIT 1;
SELECT
d, dt, dtm
FROM generateRandom('d date, dt datetime(\'Europe/Moscow\'), dtm datetime(\'Europe/Moscow\')', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
to_type_name(dt64), to_type_name(dts64), to_type_name(dtms64)
FROM generateRandom('dt64 DateTime64(3, \'Europe/Moscow\'), dts64 DateTime64(6, \'Europe/Moscow\'), dtms64 DateTime64(6 ,\'Europe/Moscow\')')
LIMIT 1;
SELECT
dt64, dts64, dtms64
FROM generateRandom('dt64 DateTime64(3, \'Europe/Moscow\'), dts64 DateTime64(6, \'Europe/Moscow\'), dtms64 DateTime64(6 ,\'Europe/Moscow\')', 1, 10, 10)
LIMIT 10;
SELECT
to_type_name(d32)
FROM generateRandom('d32 Date32')
LIMIT 1;
SELECT
d32
FROM generateRandom('d32 Date32', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(f32), to_type_name(f64)
FROM generateRandom('f32 Float32, f64 float64')
LIMIT 1;
SELECT
  f32, f64
FROM generateRandom('f32 Float32, f64 float64', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(d32), to_type_name(d64), to_type_name(d64)
FROM generateRandom('d32 Decimal32(4), d64 Decimal64(8), d128 Decimal128(16)')
LIMIT 1;
SELECT
  d32, d64, d128
FROM generateRandom('d32 Decimal32(4), d64 Decimal64(8), d128 Decimal128(16)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)
FROM generateRandom('i tuple(int32, int64)')
LIMIT 1;
SELECT
  i
FROM generateRandom('i tuple(int32, int64)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)
FROM generateRandom('i array(int8)')
LIMIT 1;
SELECT
  i
FROM generateRandom('i array(int8)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)
FROM generateRandom('i array(Nullable(int32))')
LIMIT 1;
SELECT
  i
FROM generateRandom('i array(Nullable(int32))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)
FROM generateRandom('i tuple(int32, array(int64))')
LIMIT 1;
SELECT
  i
FROM generateRandom('i tuple(int32, array(int64))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)
FROM generateRandom('i Nullable(string)', 1)
LIMIT 1;
SELECT
  i
FROM generateRandom('i Nullable(string)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
  to_type_name(i)
FROM generateRandom('i array(string)')
LIMIT 1;
SELECT
  i
FROM generateRandom('i array(string)', 1, 10, 10)
LIMIT 10;

SELECT '-';
SELECT
    to_type_name(i)
FROM generateRandom('i UUID')
LIMIT 1;
SELECT
    i
FROM generateRandom('i UUID', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
    to_type_name(i)
FROM generateRandom('i array(Nullable(UUID))')
LIMIT 1;
SELECT
    i
FROM generateRandom('i array(Nullable(UUID))', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
    to_type_name(i)
FROM generateRandom('i FixedString(4)')
LIMIT 1;
SELECT
    hex(i)
FROM generateRandom('i FixedString(4)', 1, 10, 10)
LIMIT 10;
SELECT '-';
SELECT
    to_type_name(i)
FROM generateRandom('i string')
LIMIT 1;
SELECT
    i
FROM generateRandom('i string', 1, 10, 10)
LIMIT 10;
SELECT '-';
DROP STREAM IF EXISTS test_table;
create stream test_table(a array(int8), d Decimal32(4), c tuple(DateTime64(3, 'Europe/Moscow'), UUID)) ENGINE=Memory;
INSERT INTO test_table SELECT * FROM generateRandom('a array(int8), d Decimal32(4), c tuple(DateTime64(3, \'Europe/Moscow\'), UUID)', 1, 10, 2)
LIMIT 10;

SELECT * FROM test_table ORDER BY a, d, c;

DROP STREAM IF EXISTS test_table;

SELECT '-';

DROP STREAM IF EXISTS test_table_2;
create stream test_table_2(a array(int8), b uint32, c Nullable(string), d Decimal32(4), e Nullable(Enum16('h' = 1, 'w' = 5 , 'o' = -200)), f float64, g tuple(date, datetime('Europe/Moscow'), DateTime64(3, 'Europe/Moscow'), UUID), h FixedString(2)) ENGINE=Memory;
INSERT INTO test_table_2 SELECT * FROM generateRandom('a array(int8), b uint32, c Nullable(string), d Decimal32(4), e Nullable(Enum16(\'h\' = 1, \'w\' = 5 , \'o\' = -200)), f float64, g tuple(date, datetime(\'Europe/Moscow\'), DateTime64(3, \'Europe/Moscow\'), UUID), h FixedString(2)', 10, 5, 3)
LIMIT 10;

SELECT a, b, c, d, e, f, g, hex(h) FROM test_table_2 ORDER BY a, b, c, d, e, f, g, h;
SELECT '-';

DROP STREAM IF EXISTS test_table_2;

