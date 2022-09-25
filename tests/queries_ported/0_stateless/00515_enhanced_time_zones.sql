SELECT add_months(to_datetime('2017-11-05 08:07:47', 'Europe/Moscow'), 1, 'Asia/Kolkata');
SELECT add_months(to_datetime('2017-11-05 10:37:47', 'Asia/Kolkata'), 1);
SELECT add_months(to_timezone(to_datetime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Asia/Kolkata'), 1);

SELECT add_months(to_datetime('2017-11-05 08:07:47'), 1);
SELECT add_months(materialize(to_datetime('2017-11-05 08:07:47')), 1);
SELECT add_months(to_datetime('2017-11-05 08:07:47'), materialize(1));
SELECT add_months(materialize(to_datetime('2017-11-05 08:07:47')), materialize(1));

SELECT add_months(to_datetime('2017-11-05 08:07:47'), -1);
SELECT add_months(materialize(to_datetime('2017-11-05 08:07:47')), -1);
SELECT add_months(to_datetime('2017-11-05 08:07:47'), materialize(-1));
SELECT add_months(materialize(to_datetime('2017-11-05 08:07:47')), materialize(-1));

SELECT to_unix_timestamp('2017-11-05 08:07:47', 'Europe/Moscow');
SELECT to_unix_timestamp(to_datetime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Europe/Moscow');

SELECT to_datetime('2017-11-05 08:07:47', 'Europe/Moscow');
SELECT to_timezone(to_datetime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Asia/Kolkata');
SELECT to_string(to_datetime('2017-11-05 08:07:47', 'Europe/Moscow'));
SELECT to_string(to_timezone(to_datetime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Asia/Kolkata'));
SELECT to_string(to_datetime('2017-11-05 08:07:47', 'Europe/Moscow'), 'Asia/Kolkata');
