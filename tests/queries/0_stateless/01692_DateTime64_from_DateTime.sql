select toDateTime64(to_datetime(1, 'Europe/Moscow'), 2);
select toDateTime64(to_date(1), 2) FORMAT Null; -- Unknown timezone
select toDateTime64(to_datetime(1), 2) FORMAT Null; -- Unknown timezone
select toDateTime64(to_datetime(1), 2, 'Europe/Moscow');
select toDateTime64(to_date(1), 2, 'Europe/Moscow');
select toDateTime64(to_datetime(1), 2, 'GMT');
select toDateTime64(to_date(1), 2, 'GMT');
