SELECT to_date('2016-08-02 12:34:19');
SELECT to_date(to_string(to_datetime('2000-01-01 00:00:00') + number)) FROM system.numbers LIMIT 3;
