select to_datetime64(to_datetime(1, 'Asia/Istanbul'), 2);
select to_datetime64(to_date(1), 2) FORMAT Null; -- Unknown timezone
select to_datetime64(to_datetime(1), 2) FORMAT Null; -- Unknown timezone
select to_datetime64(to_datetime(1), 2, 'Asia/Istanbul');
select to_datetime64(to_date(1), 2, 'Asia/Istanbul');
select to_datetime64(to_datetime(1), 2, 'GMT');
select to_datetime64(to_date(1), 2, 'GMT');
