SELECT to_date('2018-06-21') % 234 = to_uint16(to_date('2018-06-21')) % 234;
SELECT to_date('2018-06-21') % 23456 = to_uint16(to_date('2018-06-21')) % 23456;
SELECT to_date('2018-06-21') % 12376 = to_uint16(to_date('2018-06-21')) % 12376;
SELECT to_datetime('2018-06-21 12:12:12') % 234 = to_uint32(to_datetime('2018-06-21 12:12:12')) % 234;
SELECT to_datetime('2018-06-21 12:12:12') % 23456 = to_uint32(to_datetime('2018-06-21 12:12:12')) % 23456;
SELECT to_datetime('2018-06-21 12:12:12') % 12376 = to_uint32(to_datetime('2018-06-21 12:12:12')) % 12376;

SELECT to_date('2018-06-21') % 234.8 = to_uint16(to_date('2018-06-21')) % 234.8;
SELECT to_date('2018-06-21') % 23456.8 = to_uint16(to_date('2018-06-21')) % 23456.8;
SELECT to_date('2018-06-21') % 12376.8 = to_uint16(to_date('2018-06-21')) % 12376.8;
SELECT to_datetime('2018-06-21 12:12:12') % 234.8 = to_uint32(to_datetime('2018-06-21 12:12:12')) % 234.8;
SELECT to_datetime('2018-06-21 12:12:12') % 23456.8 = to_uint32(to_datetime('2018-06-21 12:12:12')) % 23456.8;
SELECT to_datetime('2018-06-21 12:12:12') % 12376.8 = to_uint32(to_datetime('2018-06-21 12:12:12')) % 12376.8;
