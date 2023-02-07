select 'year', date_diff('year', to_date('1969-12-25'), to_date('1970-01-05'));
select 'year', date_diff('year', to_datetime64('1969-12-25 10:00:00.000', 3), to_datetime64('1970-01-05 10:00:00.000', 3));

select 'quarter', date_diff('quarter', to_date('1969-12-25'), to_date('1970-01-05'));
select 'quarter', date_diff('quarter', to_datetime64('1969-12-25 10:00:00.000', 3), to_datetime64('1970-01-05 10:00:00.000', 3));

select 'month', date_diff('month', to_date('1969-12-25'), to_date('1970-01-05'));
select 'month', date_diff('month', to_datetime64('1969-12-25 10:00:00.000', 3), to_datetime64('1970-01-05 10:00:00.000', 3));

select 'week', date_diff('week', to_date('1969-12-25'), to_date('1970-01-05'));
select 'week', date_diff('week', to_datetime64('1969-12-25 10:00:00.000', 3), to_datetime64('1970-01-05 10:00:00.000', 3));

select 'day', date_diff('day', to_date('1969-12-25'), to_date('1970-01-05'));
select 'day', date_diff('day', to_datetime64('1969-12-25 10:00:00.000', 3), to_datetime64('1970-01-05 10:00:00.000', 3));

select 'minute', date_diff('minute', to_date('1969-12-31'), to_date('1970-01-01'));

select 'second', date_diff('second', to_date('1969-12-31'), to_date('1970-01-01'));
