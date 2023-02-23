-- all tests should be equal to zero as timediff is same as date_diff('second', ... )
SELECT date_diff('second', to_date('1927-01-01'), to_date('1927-01-02')) - time_diff(to_date('1927-01-01'), to_date('1927-01-02')) <= 2;
SELECT date_diff('second', to_date('1927-01-01'), to_datetime64('1927-01-02 00:00:00', 3)) - time_diff(to_date('1927-01-01'), to_datetime64('1927-01-02 00:00:00', 3)) <= 2;
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 3), to_date('1927-01-02')) - time_diff(to_datetime64('1927-01-01 00:00:00', 3), to_date('1927-01-02')) <= 2;
SELECT date_diff('second', to_date('2015-08-18'), to_datetime('2015-08-19 00:00:00')) - time_diff(to_date('2015-08-18'), to_datetime('2015-08-19 00:00:00')) <= 2;
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00'), to_date('2015-08-19')) - time_diff(to_datetime('2015-08-18 00:00:00'), to_date('2015-08-19')) <= 2;
SELECT date_diff('second', to_date('2015-08-18'), to_date('2015-08-19')) - time_diff(to_date('2015-08-18'), to_date('2015-08-19')) <= 2;
SELECT date_diff('second', to_date('2015-08-18'), to_date('2015-08-19')) - time_diff(to_date('2015-08-18'), to_date('2015-08-19')) <= 2;

-- utc_timestamp equals to now('UTC')
SELECT date_diff('s', utc_timestamp(), now('UTC')) <= 2;
SELECT time_diff(utc_timestamp(), now('UTC')) <= 2;
