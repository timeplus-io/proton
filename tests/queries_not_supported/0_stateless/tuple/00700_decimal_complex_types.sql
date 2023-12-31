DROP STREAM IF EXISTS decimal;

create stream decimal
(
    a array(Decimal32(3)),
    b array(Decimal64(3)),
    c array(Decimal128(3)),
    nest nested
    (
        a Decimal(9,2),
        b Decimal(18,2),
        c Decimal(38,2)
    ),
    tup tuple(Decimal32(1), Decimal64(1), Decimal128(1))
) ;

INSERT INTO decimal (a, b, c, nest.a, nest.b, nest.c, tup)
    VALUES ([0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9], [1.1, 1.2], [2.1, 2.2], [3.1, 3.2], (9.1, 9.2, 9.3));

SELECT to_type_name(a), to_type_name(b), to_type_name(c) FROM decimal;
SELECT to_type_name(nest.a), to_type_name(nest.b), to_type_name(nest.c) FROM decimal;
SELECT to_type_name(a[1]), to_type_name(b[2]), to_type_name(c[3]) FROM decimal;
SELECT to_type_name(nest.a[1]), to_type_name(nest.b[1]), to_type_name(nest.c[1]) FROM decimal;
SELECT to_type_name(tup), to_type_name(tup.1), to_type_name(tup.2), to_type_name(tup.3) FROM decimal;

SELECT array_join(a) FROM decimal;
SELECT array_join(b) FROM decimal;
SELECT array_join(c) FROM decimal;

SELECT tup, tup.1, tup.2, tup.3 FROM decimal;
SELECT a, arrayPopBack(a), arrayPopFront(a), arrayResize(a, 1), arraySlice(a, 2, 1) FROM decimal;
SELECT b, arrayPopBack(b), arrayPopFront(b), arrayResize(b, 1), arraySlice(b, 2, 1) FROM decimal;
SELECT c, arrayPopBack(c), arrayPopFront(c), arrayResize(c, 1), arraySlice(c, 2, 1) FROM decimal;
SELECT nest.a, arrayPopBack(nest.a), arrayPopFront(nest.a), arrayResize(nest.a, 1), arraySlice(nest.a, 2, 1) FROM decimal;
SELECT nest.b, arrayPopBack(nest.b), arrayPopFront(nest.b), arrayResize(nest.b, 1), arraySlice(nest.b, 2, 1) FROM decimal;
SELECT nest.c, arrayPopBack(nest.c), arrayPopFront(nest.c), arrayResize(nest.c, 1), arraySlice(nest.c, 2, 1) FROM decimal;
SELECT arrayPushBack(a, to_decimal32(0, 3)), arrayPushFront(a, to_decimal32(0, 3)) FROM decimal;
SELECT arrayPushBack(b, to_decimal64(0, 3)), arrayPushFront(b, to_decimal64(0, 3)) FROM decimal;
SELECT arrayPushBack(c, toDecimal128(0, 3)), arrayPushFront(c, toDecimal128(0, 3)) FROM decimal;

SELECT arrayPushBack(a, to_decimal32(0, 2)) AS x, to_type_name(x) FROM decimal;
SELECT arrayPushBack(b, to_decimal64(0, 2)) AS x, to_type_name(x) FROM decimal;
SELECT arrayPushBack(c, toDecimal128(0, 2)) AS x, to_type_name(x) FROM decimal;
SELECT arrayPushFront(a, to_decimal32(0, 4)) AS x, to_type_name(x) FROM decimal;
SELECT arrayPushFront(b, to_decimal64(0, 4)) AS x, to_type_name(x) FROM decimal;
SELECT arrayPushFront(c, toDecimal128(0, 4)) AS x, to_type_name(x) FROM decimal;

SELECT length(a), length(b), length(c) FROM decimal;
SELECT length(nest.a), length(nest.b), length(nest.c) FROM decimal;
SELECT empty(a), empty(b), empty(c) FROM decimal;
SELECT empty(nest.a), empty(nest.b), empty(nest.c) FROM decimal;
SELECT not_empty(a), not_empty(b), not_empty(c) FROM decimal;
SELECT not_empty(nest.a), not_empty(nest.b), not_empty(nest.c) FROM decimal;
SELECT arrayUniq(a), arrayUniq(b), arrayUniq(c) FROM decimal;
SELECT arrayUniq(nest.a), arrayUniq(nest.b), arrayUniq(nest.c) FROM decimal;

SELECT has(a, to_decimal32(0.1, 3)), has(a, to_decimal32(1.0, 3)) FROM decimal;
SELECT has(b, to_decimal64(0.4, 3)), has(b, to_decimal64(1.0, 3)) FROM decimal;
SELECT has(c, toDecimal128(0.7, 3)), has(c, toDecimal128(1.0, 3)) FROM decimal;

SELECT has(a, to_decimal32(0.1, 2)) FROM decimal;
SELECT has(a, to_decimal32(0.1, 4)) FROM decimal;
SELECT has(a, to_decimal64(0.1, 3)) FROM decimal;
SELECT has(a, toDecimal128(0.1, 3)) FROM decimal;
SELECT has(b, to_decimal32(0.4, 3)) FROM decimal;
SELECT has(b, to_decimal64(0.4, 2)) FROM decimal;
SELECT has(b, to_decimal64(0.4, 4)) FROM decimal;
SELECT has(b, toDecimal128(0.4, 3)) FROM decimal;
SELECT has(c, to_decimal32(0.7, 3)) FROM decimal;
SELECT has(c, to_decimal64(0.7, 3)) FROM decimal;
SELECT has(c, toDecimal128(0.7, 2)) FROM decimal;
SELECT has(c, toDecimal128(0.7, 4)) FROM decimal;

SELECT index_of(a, to_decimal32(0.1, 3)), index_of(a, to_decimal32(1.0, 3)) FROM decimal;
SELECT index_of(b, to_decimal64(0.5, 3)), index_of(b, to_decimal64(1.0, 3)) FROM decimal;
SELECT index_of(c, toDecimal128(0.9, 3)), index_of(c, toDecimal128(1.0, 3)) FROM decimal;

SELECT index_of(a, to_decimal32(0.1, 2)) FROM decimal;
SELECT index_of(a, to_decimal32(0.1, 4)) FROM decimal;
SELECT index_of(a, to_decimal64(0.1, 3)) FROM decimal;
SELECT index_of(a, toDecimal128(0.1, 3)) FROM decimal;
SELECT index_of(b, to_decimal32(0.4, 3)) FROM decimal;
SELECT index_of(b, to_decimal64(0.4, 2)) FROM decimal;
SELECT index_of(b, to_decimal64(0.4, 4)) FROM decimal;
SELECT index_of(b, toDecimal128(0.4, 3)) FROM decimal;
SELECT index_of(c, to_decimal32(0.7, 3)) FROM decimal;
SELECT index_of(c, to_decimal64(0.7, 3)) FROM decimal;
SELECT index_of(c, toDecimal128(0.7, 2)) FROM decimal;
SELECT index_of(c, toDecimal128(0.7, 4)) FROM decimal;

SELECT arrayConcat(a, b) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(a, c) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(b, c) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(a, nest.a) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(b, nest.b) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(c, nest.c) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(a, nest.b) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(a, nest.c) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(b, nest.a) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(b, nest.c) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(c, nest.a) AS x, to_type_name(x) FROM decimal;
SELECT arrayConcat(c, nest.b) AS x, to_type_name(x) FROM decimal;

SELECT to_decimal32(12345.6789, 4) AS x, count_equal([x+1, x, x], x), count_equal([x, x-1, x], x), count_equal([x, x], x-0);
SELECT to_decimal32(-12345.6789, 4) AS x, count_equal([x+1, x, x], x), count_equal([x, x-1, x], x), count_equal([x, x], x+0);
SELECT to_decimal64(123456789.123456789, 9) AS x, count_equal([x+1, x, x], x), count_equal([x, x-1, x], x), count_equal([x, x], x-0);
SELECT to_decimal64(-123456789.123456789, 9) AS x, count_equal([x+1, x, x], x), count_equal([x, x-1, x], x), count_equal([x, x], x+0);
SELECT toDecimal128(0.123456789123456789, 18) AS x, count_equal([x+1, x, x], x), count_equal([x, x-1, x], x), count_equal([x, x], x-0);
SELECT toDecimal128(-0.1234567891123456789, 18) AS x, count_equal([x+1, x, x], x), count_equal([x, x-1, x], x), count_equal([x, x], x+0);

SELECT to_type_name(x) FROM (SELECT to_decimal32('1234.5', 5) AS x UNION ALL SELECT to_int8(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('1234.5', 5) AS x UNION ALL SELECT to_uint8(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('12345.0', 4) AS x UNION ALL SELECT to_int16(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('12345.0', 4) AS x UNION ALL SELECT to_uint16(0) AS x) WHERE x = 0;

SELECT to_type_name(x) FROM (SELECT to_decimal32('12.345', 7) AS x UNION ALL SELECT to_int8(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('12.345', 7) AS x UNION ALL SELECT to_uint8(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('1234.5', 5) AS x UNION ALL SELECT to_int16(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('1234.5', 5) AS x UNION ALL SELECT to_uint16(0) AS x) WHERE x = 0;

SELECT to_type_name(x) FROM (SELECT to_decimal32('12345.00', 4) AS x UNION ALL SELECT to_int32(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('12345.00', 4) AS x UNION ALL SELECT to_uint32(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('12345.00', 4) AS x UNION ALL SELECT to_int64(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal32('12345.00', 4) AS x UNION ALL SELECT to_uint64(0) AS x) WHERE x = 0;

SELECT to_type_name(x) FROM (SELECT to_decimal64('12345.00', 4) AS x UNION ALL SELECT to_int8(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal64('12345.00', 4) AS x UNION ALL SELECT to_uint8(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal64('12345.00', 4) AS x UNION ALL SELECT to_int16(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal64('12345.00', 4) AS x UNION ALL SELECT to_uint16(0) AS x) WHERE x = 0;

SELECT to_type_name(x) FROM (SELECT to_decimal64('12345.00', 4) AS x UNION ALL SELECT to_int32(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal64('12345.00', 4) AS x UNION ALL SELECT to_uint32(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal64('12345.00', 4) AS x UNION ALL SELECT to_int64(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal64('12345.00', 4) AS x UNION ALL SELECT to_uint64(0) AS x) WHERE x = 0;

SELECT to_type_name(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT to_int8(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT to_uint8(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT to_int16(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT to_uint16(0) AS x) WHERE x = 0;

SELECT to_type_name(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT to_int32(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT to_uint32(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT to_int64(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT toDecimal128('12345.00', 4) AS x UNION ALL SELECT to_uint64(0) AS x) WHERE x = 0;

SELECT to_type_name(x) FROM (SELECT to_decimal32('12345', 0) AS x UNION ALL SELECT to_int32(0) AS x) WHERE x = 0;
SELECT to_type_name(x) FROM (SELECT to_decimal64('12345', 0) AS x UNION ALL SELECT to_int64(0) AS x) WHERE x = 0;

SELECT number % 2 ? to_decimal32('32.1', 5) : to_decimal32('32.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? to_decimal32('32.1', 5) : to_decimal64('64.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? to_decimal32('32.1', 5) : toDecimal128('128.2', 5) FROM system.numbers LIMIT 2;

SELECT number % 2 ? to_decimal64('64.1', 5) : to_decimal32('32.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? to_decimal64('64.1', 5) : to_decimal64('64.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? to_decimal64('64.1', 5) : toDecimal128('128.2', 5) FROM system.numbers LIMIT 2;

SELECT number % 2 ? toDecimal128('128.1', 5) : to_decimal32('32.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? toDecimal128('128.1', 5) : to_decimal64('64.2', 5) FROM system.numbers LIMIT 2;
SELECT number % 2 ? toDecimal128('128.1', 5) : toDecimal128('128.2', 5) FROM system.numbers LIMIT 2;

SELECT number % 2 ? to_decimal32('32.1', 5) : to_decimal32('32.2', 1) FROM system.numbers LIMIT 2; -- { serverError 48 }
SELECT number % 2 ? to_decimal32('32.1', 5) : to_decimal64('64.2', 2) FROM system.numbers LIMIT 2; -- { serverError 48 }
SELECT number % 2 ? to_decimal32('32.1', 5) : toDecimal128('128.2', 3) FROM system.numbers LIMIT 2; -- { serverError 48 }

SELECT number % 2 ? to_decimal64('64.1', 5) : to_decimal32('32.2', 1) FROM system.numbers LIMIT 2; -- { serverError 48 }
SELECT number % 2 ? to_decimal64('64.1', 5) : to_decimal64('64.2', 2) FROM system.numbers LIMIT 2; -- { serverError 48 }
SELECT number % 2 ? to_decimal64('64.1', 5) : toDecimal128('128.2', 3) FROM system.numbers LIMIT 2; -- { serverError 48 }

SELECT number % 2 ? toDecimal128('128.1', 5) : to_decimal32('32.2', 1) FROM system.numbers LIMIT 2; -- { serverError 48 }
SELECT number % 2 ? toDecimal128('128.1', 5) : to_decimal64('64.2', 2) FROM system.numbers LIMIT 2; -- { serverError 48 }
SELECT number % 2 ? toDecimal128('128.1', 5) : toDecimal128('128.2', 3) FROM system.numbers LIMIT 2; -- { serverError 48 }

DROP STREAM IF EXISTS decimal;
