SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS grop_uniq_array_date;
create stream grop_uniq_array_date (d date, dt datetime, id Integer) ;
INSERT INTO grop_uniq_array_date(d,dt,id) VALUES (to_date('2016-12-16'), to_datetime('2016-12-16 12:00:00'), 1) (to_date('2016-12-16'), to_datetime('2016-12-16 12:00:00'), 1);
SELECT sleep(3);
SELECT groupUniqArray(d), groupUniqArray(dt) FROM grop_uniq_array_date;
INSERT INTO grop_uniq_array_date(d,dt,id) VALUES (to_date('2016-12-17'), to_datetime('2016-12-17 12:00:00'), 1), (to_date('2016-12-18'), to_datetime('2016-12-18 12:00:00'), 1), (to_date('2016-12-16'), to_datetime('2016-12-16 12:00:00'), 2);
SELECT sleep(3);
SELECT length(groupUniqArray(2)(d)), length(groupUniqArray(2)(dt)), length(groupUniqArray(d)), length(groupUniqArray(dt)) FROM grop_uniq_array_date GROUP BY id ORDER BY id;
SELECT length(groupUniqArray(10000)(d)), length(groupUniqArray(10000)(dt)) FROM grop_uniq_array_date GROUP BY id ORDER BY id;
DROP STREAM IF EXISTS grop_uniq_array_date;
