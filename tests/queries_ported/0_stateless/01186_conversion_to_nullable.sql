select to_uint8(x) from values('x nullable(string)', '42', NULL, '0', '', '256');
select to_int64(x) from values('x nullable(string)', '42', NULL, '0', '', '256');

select to_date(x) from values('x nullable(string)', '2020-12-24', NULL, '0000-00-00', '', '9999-01-01');
select to_datetime(x, 'Asia/Istanbul') from values('x nullable(string)', '2020-12-24 01:02:03', NULL, '0000-00-00 00:00:00', '');
select to_datetime64(x, 2, 'Asia/Istanbul') from values('x nullable(string)', '2020-12-24 01:02:03', NULL, '0000-00-00 00:00:00', '');
select to_unix_timestamp(x, 'Asia/Istanbul') from values ('x nullable(string)', '2000-01-01 13:12:12', NULL, '');

select to_decimal32(x, 2) from values ('x nullable(string)', '42', NULL, '3.14159');
select to_decimal64(x, 8) from values ('x nullable(string)', '42', NULL, '3.14159');

select to_string(x) from values ('x nullable(string)', '42', NULL, 'test');
select to_fixed_string(x, 8) from values ('x nullable(string)', '42', NULL, 'test');
    