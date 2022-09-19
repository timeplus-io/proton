-- types
select argMin(x.1, x.2), argMax(x.1, x.2) from (select (number, number + 1) as x from numbers(10));
select argMin(x.1, x.2), argMax(x.1, x.2) from (select (to_string(number), to_int32(number) + 1) as x from numbers(10));
select argMin(x.1, x.2), argMax(x.1, x.2) from (select (to_date(number, 'UTC'), to_datetime(number, 'UTC') + 1) as x from numbers(10));
select argMin(x.1, x.2), argMax(x.1, x.2) from (select (to_decimal32(number, 2), to_decimal64(number, 2) + 1) as x from numbers(10));

-- array
SELECT argMinArray(id, num), argMaxArray(id, num)  FROM (SELECT array_join([[10, 4, 3], [7, 5, 6], [8, 8, 2]]) AS num, array_join([[1, 2, 4], [2, 3, 3]]) AS id);
