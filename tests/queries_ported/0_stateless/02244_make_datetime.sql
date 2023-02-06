select make_datetime(1991, 8, 24, 21, 4, 0);
select make_datetime(1991, 8, 24, 21, 4, 0, 'CET');
select cast(make_datetime(1991, 8, 24, 21, 4, 0, 'CET') as DateTime('UTC'));

select to_type_name(make_datetime(1991, 8, 24, 21, 4, 0));
select to_type_name(make_datetime(1991, 8, 24, 21, 4, 0, 'CET'));

select make_datetime(1925, 1, 1, 0, 0, 0, 'UTC');
select make_datetime(1924, 12, 31, 23, 59, 59, 'UTC');
select make_datetime(2283, 11, 11, 23, 59, 59, 'UTC');
select make_datetime(2283, 11, 12, 0, 0, 0, 'UTC');
select make_datetime(2262, 4, 11, 23, 47, 16, 'UTC');
select make_datetime(2262, 4, 11, 23, 47, 17, 'UTC');
select make_datetime(2262, 4, 11, 23, 47, 16, 'UTC');

select make_datetime(1984, 0, 1, 0, 0, 0, 'UTC');
select make_datetime(1984, 1, 0, 0, 0, 0, 'UTC');
select make_datetime(1984, 13, 1, 0, 0, 0, 'UTC');
select make_datetime(1984, 1, 41, 0, 0, 0, 'UTC');
select make_datetime(1984, 1, 1, 25, 0, 0, 'UTC');
select make_datetime(1984, 1, 1, 0, 70, 0, 'UTC');
select make_datetime(1984, 1, 1, 0, 0, 70, 'UTC');
select make_datetime(1984, 1, 1, 0, 0, 0, 'not a timezone'); -- { serverError 1000 }

select make_datetime(1984, 1, 1, 0, 0, 0, 'UTC');
select make_datetime(1983, 2, 29, 0, 0, 0, 'UTC');
select make_datetime(-1984, 1, 1, 0, 0, 0, 'UTC');
select make_datetime(1984, -1, 1, 0, 0, 0, 'UTC');
select make_datetime(1984, 1, -1, 0, 0, 0, 'UTC');
select make_datetime(1984, 1, 1, -1, 0, 0, 'UTC');
select make_datetime(1984, 1, 1, 0, -1, 0, 'UTC');
select make_datetime(1984, 1, 1, 0, 0, -1, 'UTC');

select make_datetime(65537, 8, 24, 21, 4, 0, 'UTC');
select make_datetime(1991, 65537, 24, 21, 4, 0, 'UTC');
select make_datetime(1991, 8, 65537, 21, 4, 0, 'UTC');
select make_datetime(1991, 8, 24, 65537, 4, 0, 'UTC');
select make_datetime(1991, 8, 24, 21, 65537, 0, 'UTC');
select make_datetime(1991, 8, 24, 21, 4, 65537, 'UTC');