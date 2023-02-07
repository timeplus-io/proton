SELECT date_diff('h', to_datetime('2018-01-01 22:00:00'), to_datetime('2018-01-02 23:00:00'));
SELECT  to_datetime('2018-01-01 22:00:00') + INTERVAL 4 as h HOUR
