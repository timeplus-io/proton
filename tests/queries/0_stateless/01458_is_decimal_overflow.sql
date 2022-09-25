SELECT isDecimalOverflow(to_decimal32(0, 0), 0),
       isDecimalOverflow(to_decimal64(0, 0), 0),
       isDecimalOverflow(toDecimal128(0, 0), 0);

SELECT isDecimalOverflow(to_decimal32(1000000000, 0), 9),
       isDecimalOverflow(to_decimal32(1000000000, 0)),
       isDecimalOverflow(to_decimal32(-1000000000, 0), 9),
       isDecimalOverflow(to_decimal32(-1000000000, 0));
SELECT isDecimalOverflow(to_decimal32(999999999, 0), 9),
       isDecimalOverflow(to_decimal32(999999999, 0)),
       isDecimalOverflow(to_decimal32(-999999999, 0), 9),
       isDecimalOverflow(to_decimal32(-999999999, 0));
SELECT isDecimalOverflow(to_decimal32(999999999, 0), 8),
       isDecimalOverflow(to_decimal32(10, 0), 1),
       isDecimalOverflow(to_decimal32(1, 0), 0),
       isDecimalOverflow(to_decimal32(-999999999, 0), 8),
       isDecimalOverflow(to_decimal32(-10, 0), 1),
       isDecimalOverflow(to_decimal32(-1, 0), 0);

SELECT isDecimalOverflow(materialize(to_decimal32(1000000000, 0)), 9),
       isDecimalOverflow(materialize(to_decimal32(1000000000, 0))),
       isDecimalOverflow(materialize(to_decimal32(-1000000000, 0)), 9),
       isDecimalOverflow(materialize(to_decimal32(-1000000000, 0)));
SELECT isDecimalOverflow(materialize(to_decimal32(999999999, 0)), 9),
       isDecimalOverflow(materialize(to_decimal32(999999999, 0))),
       isDecimalOverflow(materialize(to_decimal32(-999999999, 0)), 9),
       isDecimalOverflow(materialize(to_decimal32(-999999999, 0)));
SELECT isDecimalOverflow(materialize(to_decimal32(999999999, 0)), 8),
       isDecimalOverflow(materialize(to_decimal32(10, 0)), 1),
       isDecimalOverflow(materialize(to_decimal32(1, 0)), 0),
       isDecimalOverflow(materialize(to_decimal32(-999999999, 0)), 8),
       isDecimalOverflow(materialize(to_decimal32(-10, 0)), 1),
       isDecimalOverflow(materialize(to_decimal32(-1, 0)), 0);

SELECT isDecimalOverflow(to_decimal64(1000000000000000000, 0), 18),
       isDecimalOverflow(to_decimal64(1000000000000000000, 0)),
       isDecimalOverflow(to_decimal64(-1000000000000000000, 0), 18),
       isDecimalOverflow(to_decimal64(-1000000000000000000, 0));
SELECT isDecimalOverflow(to_decimal64(999999999999999999, 0), 18),
       isDecimalOverflow(to_decimal64(999999999999999999, 0)),
       isDecimalOverflow(to_decimal64(-999999999999999999, 0), 18),
       isDecimalOverflow(to_decimal64(-999999999999999999, 0));
SELECT isDecimalOverflow(to_decimal64(999999999999999999, 0), 17),
       isDecimalOverflow(to_decimal64(10, 0), 1),
       isDecimalOverflow(to_decimal64(1, 0), 0),
       isDecimalOverflow(to_decimal64(-999999999999999999, 0), 17),
       isDecimalOverflow(to_decimal64(-10, 0), 1),
       isDecimalOverflow(to_decimal64(-1, 0), 0);

SELECT isDecimalOverflow(materialize(to_decimal64(1000000000000000000, 0)), 18),
       isDecimalOverflow(materialize(to_decimal64(1000000000000000000, 0))),
       isDecimalOverflow(materialize(to_decimal64(-1000000000000000000, 0)), 18),
       isDecimalOverflow(materialize(to_decimal64(-1000000000000000000, 0)));
SELECT isDecimalOverflow(materialize(to_decimal64(999999999999999999, 0)), 18),
       isDecimalOverflow(materialize(to_decimal64(999999999999999999, 0))),
       isDecimalOverflow(materialize(to_decimal64(-999999999999999999, 0)), 18),
       isDecimalOverflow(materialize(to_decimal64(-999999999999999999, 0)));
SELECT isDecimalOverflow(materialize(to_decimal64(999999999999999999, 0)), 17),
       isDecimalOverflow(materialize(to_decimal64(10, 0)), 1),
       isDecimalOverflow(materialize(to_decimal64(1, 0)), 0),
       isDecimalOverflow(materialize(to_decimal64(-999999999999999999, 0)), 17),
       isDecimalOverflow(materialize(to_decimal64(-10, 0)), 1),
       isDecimalOverflow(materialize(to_decimal64(-1, 0)), 0);

SELECT isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0) + 1, 38),
       isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0) + 1),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0) - 1, 38),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0) - 1);
SELECT isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0), 38),
       isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0)),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0), 38),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0));
SELECT isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0), 37),
       isDecimalOverflow(toDecimal128('10', 0), 1),
       isDecimalOverflow(toDecimal128('1', 0), 0),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0), 37),
       isDecimalOverflow(toDecimal128('-10', 0), 1),
       isDecimalOverflow(toDecimal128('-1', 0), 0);

SELECT isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0)) + 1, 38),
       isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0)) + 1),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)) - 1, 38),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)) - 1);
SELECT isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0)), 38),
       isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0))),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)), 38),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)));
SELECT isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0)), 37),
       isDecimalOverflow(materialize(toDecimal128('10', 0)), 1),
       isDecimalOverflow(materialize(toDecimal128('1', 0)), 0),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)), 37),
       isDecimalOverflow(materialize(toDecimal128('-10', 0)), 1),
       isDecimalOverflow(materialize(toDecimal128('-1', 0)), 0);

SELECT isDecimalOverflow(to_nullable(to_decimal32(42, 0)), 1),
       isDecimalOverflow(materialize(to_nullable(to_decimal32(42, 0))), 2),
       isDecimalOverflow(to_nullable(to_decimal64(42, 0)), 1),
       isDecimalOverflow(materialize(to_nullable(to_decimal64(42, 0))), 2),
       isDecimalOverflow(to_nullable(toDecimal128(42, 0)), 1),
       isDecimalOverflow(materialize(to_nullable(toDecimal128(42, 0))), 2);
