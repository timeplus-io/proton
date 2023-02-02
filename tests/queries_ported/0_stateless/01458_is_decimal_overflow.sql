SELECT is_decimal_overflow(to_decimal32(0, 0), 0),
       is_decimal_overflow(to_decimal64(0, 0), 0),
       is_decimal_overflow(to_decimal128(0, 0), 0);

SELECT is_decimal_overflow(to_decimal32(1000000000, 0), 9),
       is_decimal_overflow(to_decimal32(1000000000, 0)),
       is_decimal_overflow(to_decimal32(-1000000000, 0), 9),
       is_decimal_overflow(to_decimal32(-1000000000, 0));
SELECT is_decimal_overflow(to_decimal32(999999999, 0), 9),
       is_decimal_overflow(to_decimal32(999999999, 0)),
       is_decimal_overflow(to_decimal32(-999999999, 0), 9),
       is_decimal_overflow(to_decimal32(-999999999, 0));
SELECT is_decimal_overflow(to_decimal32(999999999, 0), 8),
       is_decimal_overflow(to_decimal32(10, 0), 1),
       is_decimal_overflow(to_decimal32(1, 0), 0),
       is_decimal_overflow(to_decimal32(-999999999, 0), 8),
       is_decimal_overflow(to_decimal32(-10, 0), 1),
       is_decimal_overflow(to_decimal32(-1, 0), 0);

SELECT is_decimal_overflow(materialize(to_decimal32(1000000000, 0)), 9),
       is_decimal_overflow(materialize(to_decimal32(1000000000, 0))),
       is_decimal_overflow(materialize(to_decimal32(-1000000000, 0)), 9),
       is_decimal_overflow(materialize(to_decimal32(-1000000000, 0)));
SELECT is_decimal_overflow(materialize(to_decimal32(999999999, 0)), 9),
       is_decimal_overflow(materialize(to_decimal32(999999999, 0))),
       is_decimal_overflow(materialize(to_decimal32(-999999999, 0)), 9),
       is_decimal_overflow(materialize(to_decimal32(-999999999, 0)));
SELECT is_decimal_overflow(materialize(to_decimal32(999999999, 0)), 8),
       is_decimal_overflow(materialize(to_decimal32(10, 0)), 1),
       is_decimal_overflow(materialize(to_decimal32(1, 0)), 0),
       is_decimal_overflow(materialize(to_decimal32(-999999999, 0)), 8),
       is_decimal_overflow(materialize(to_decimal32(-10, 0)), 1),
       is_decimal_overflow(materialize(to_decimal32(-1, 0)), 0);

SELECT is_decimal_overflow(to_decimal64(1000000000000000000, 0), 18),
       is_decimal_overflow(to_decimal64(1000000000000000000, 0)),
       is_decimal_overflow(to_decimal64(-1000000000000000000, 0), 18),
       is_decimal_overflow(to_decimal64(-1000000000000000000, 0));
SELECT is_decimal_overflow(to_decimal64(999999999999999999, 0), 18),
       is_decimal_overflow(to_decimal64(999999999999999999, 0)),
       is_decimal_overflow(to_decimal64(-999999999999999999, 0), 18),
       is_decimal_overflow(to_decimal64(-999999999999999999, 0));
SELECT is_decimal_overflow(to_decimal64(999999999999999999, 0), 17),
       is_decimal_overflow(to_decimal64(10, 0), 1),
       is_decimal_overflow(to_decimal64(1, 0), 0),
       is_decimal_overflow(to_decimal64(-999999999999999999, 0), 17),
       is_decimal_overflow(to_decimal64(-10, 0), 1),
       is_decimal_overflow(to_decimal64(-1, 0), 0);

SELECT is_decimal_overflow(materialize(to_decimal64(1000000000000000000, 0)), 18),
       is_decimal_overflow(materialize(to_decimal64(1000000000000000000, 0))),
       is_decimal_overflow(materialize(to_decimal64(-1000000000000000000, 0)), 18),
       is_decimal_overflow(materialize(to_decimal64(-1000000000000000000, 0)));
SELECT is_decimal_overflow(materialize(to_decimal64(999999999999999999, 0)), 18),
       is_decimal_overflow(materialize(to_decimal64(999999999999999999, 0))),
       is_decimal_overflow(materialize(to_decimal64(-999999999999999999, 0)), 18),
       is_decimal_overflow(materialize(to_decimal64(-999999999999999999, 0)));
SELECT is_decimal_overflow(materialize(to_decimal64(999999999999999999, 0)), 17),
       is_decimal_overflow(materialize(to_decimal64(10, 0)), 1),
       is_decimal_overflow(materialize(to_decimal64(1, 0)), 0),
       is_decimal_overflow(materialize(to_decimal64(-999999999999999999, 0)), 17),
       is_decimal_overflow(materialize(to_decimal64(-10, 0)), 1),
       is_decimal_overflow(materialize(to_decimal64(-1, 0)), 0);

SELECT is_decimal_overflow(to_decimal128('99999999999999999999999999999999999999', 0) + 1, 38),
       is_decimal_overflow(to_decimal128('99999999999999999999999999999999999999', 0) + 1),
       is_decimal_overflow(to_decimal128('-99999999999999999999999999999999999999', 0) - 1, 38),
       is_decimal_overflow(to_decimal128('-99999999999999999999999999999999999999', 0) - 1);
SELECT is_decimal_overflow(to_decimal128('99999999999999999999999999999999999999', 0), 38),
       is_decimal_overflow(to_decimal128('99999999999999999999999999999999999999', 0)),
       is_decimal_overflow(to_decimal128('-99999999999999999999999999999999999999', 0), 38),
       is_decimal_overflow(to_decimal128('-99999999999999999999999999999999999999', 0));
SELECT is_decimal_overflow(to_decimal128('99999999999999999999999999999999999999', 0), 37),
       is_decimal_overflow(to_decimal128('10', 0), 1),
       is_decimal_overflow(to_decimal128('1', 0), 0),
       is_decimal_overflow(to_decimal128('-99999999999999999999999999999999999999', 0), 37),
       is_decimal_overflow(to_decimal128('-10', 0), 1),
       is_decimal_overflow(to_decimal128('-1', 0), 0);

SELECT is_decimal_overflow(materialize(to_decimal128('99999999999999999999999999999999999999', 0)) + 1, 38),
       is_decimal_overflow(materialize(to_decimal128('99999999999999999999999999999999999999', 0)) + 1),
       is_decimal_overflow(materialize(to_decimal128('-99999999999999999999999999999999999999', 0)) - 1, 38),
       is_decimal_overflow(materialize(to_decimal128('-99999999999999999999999999999999999999', 0)) - 1);
SELECT is_decimal_overflow(materialize(to_decimal128('99999999999999999999999999999999999999', 0)), 38),
       is_decimal_overflow(materialize(to_decimal128('99999999999999999999999999999999999999', 0))),
       is_decimal_overflow(materialize(to_decimal128('-99999999999999999999999999999999999999', 0)), 38),
       is_decimal_overflow(materialize(to_decimal128('-99999999999999999999999999999999999999', 0)));
SELECT is_decimal_overflow(materialize(to_decimal128('99999999999999999999999999999999999999', 0)), 37),
       is_decimal_overflow(materialize(to_decimal128('10', 0)), 1),
       is_decimal_overflow(materialize(to_decimal128('1', 0)), 0),
       is_decimal_overflow(materialize(to_decimal128('-99999999999999999999999999999999999999', 0)), 37),
       is_decimal_overflow(materialize(to_decimal128('-10', 0)), 1),
       is_decimal_overflow(materialize(to_decimal128('-1', 0)), 0);

SELECT is_decimal_overflow(to_nullable(to_decimal32(42, 0)), 1),
       is_decimal_overflow(materialize(to_nullable(to_decimal32(42, 0))), 2),
       is_decimal_overflow(to_nullable(to_decimal64(42, 0)), 1),
       is_decimal_overflow(materialize(to_nullable(to_decimal64(42, 0))), 2),
       is_decimal_overflow(to_nullable(to_decimal128(42, 0)), 1),
       is_decimal_overflow(materialize(to_nullable(to_decimal128(42, 0))), 2);
