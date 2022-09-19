DROP STREAM IF EXISTS values_list;

SELECT * FROM VALUES('a uint64, s string', (1, 'one'), (2, 'two'), (3, 'three'));
create stream values_list AS VALUES('a uint64, s string', (1, 'one'), (2, 'two'), (3, 'three'));
SELECT * FROM values_list;

SELECT subtractYears(date, 1), subtractYears(date_time, 1) FROM VALUES('date date, date_time DateTime', (to_date('2019-01-01'), to_datetime('2019-01-01 00:00:00')));

SELECT * FROM VALUES('s string', ('abra'), ('cadabra'), ('abracadabra'));

SELECT * FROM VALUES('n uint64, s string, ss string', (1 + 22, '23', to_string(23)), (to_uint64('24'), '24', concat('2', '4')));

SELECT * FROM VALUES('a Decimal(4, 4), b string, c string', (divide(to_decimal32(5, 3), 3), 'a', 'b'));

SELECT * FROM VALUES('x float64', to_uint64(-1)); -- { serverError 69; }
SELECT * FROM VALUES('x float64', NULL); -- { serverError 53; }
SELECT * FROM VALUES('x Nullable(float64)', NULL);

DROP STREAM values_list;
