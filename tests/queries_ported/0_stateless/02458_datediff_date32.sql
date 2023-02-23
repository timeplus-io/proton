-- { echo }

-- Date32 vs Date32
SELECT date_diff('second', to_date('1927-01-01', 'UTC'), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('minute', to_date('1927-01-01', 'UTC'), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('hour', to_date('1927-01-01', 'UTC'), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('day', to_date('1927-01-01', 'UTC'), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('week', to_date('1927-01-01', 'UTC'), to_date('1927-01-08', 'UTC'), 'UTC');
SELECT date_diff('month', to_date('1927-01-01', 'UTC'), to_date('1927-02-01', 'UTC'), 'UTC');
SELECT date_diff('quarter', to_date('1927-01-01', 'UTC'), to_date('1927-04-01', 'UTC'), 'UTC');
SELECT date_diff('year', to_date('1927-01-01', 'UTC'), to_date('1928-01-01', 'UTC'), 'UTC');

-- With DateTime64
-- Date32 vs DateTime64
SELECT date_diff('second', to_date('1927-01-01', 'UTC'), to_datetime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT date_diff('minute', to_date('1927-01-01', 'UTC'), to_datetime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT date_diff('hour', to_date('1927-01-01', 'UTC'), to_datetime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT date_diff('day', to_date('1927-01-01', 'UTC'), to_datetime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT date_diff('week', to_date('1927-01-01', 'UTC'), to_datetime64('1927-01-08 00:00:00', 3, 'UTC'), 'UTC');
SELECT date_diff('month', to_date('1927-01-01', 'UTC'), to_datetime64('1927-02-01 00:00:00', 3, 'UTC'), 'UTC');
SELECT date_diff('quarter', to_date('1927-01-01', 'UTC'), to_datetime64('1927-04-01 00:00:00', 3, 'UTC'), 'UTC');
SELECT date_diff('year', to_date('1927-01-01', 'UTC'), to_datetime64('1928-01-01 00:00:00', 3, 'UTC'), 'UTC');

-- DateTime64 vs Date32
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('minute', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('hour', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('day', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('week', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), to_date('1927-01-08', 'UTC'), 'UTC');
SELECT date_diff('month', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), to_date('1927-02-01', 'UTC'), 'UTC');
SELECT date_diff('quarter', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), to_date('1927-04-01', 'UTC'), 'UTC');
SELECT date_diff('year', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), to_date('1928-01-01', 'UTC'), 'UTC');

-- With DateTime
-- Date32 vs DateTime
SELECT date_diff('second', to_date('2015-08-18', 'UTC'), to_datetime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('minute', to_date('2015-08-18', 'UTC'), to_datetime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('hour', to_date('2015-08-18', 'UTC'), to_datetime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), to_datetime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('week', to_date('2015-08-18', 'UTC'), to_datetime('2015-08-25 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('month', to_date('2015-08-18', 'UTC'), to_datetime('2015-09-18 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('quarter', to_date('2015-08-18', 'UTC'), to_datetime('2015-11-18 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('year', to_date('2015-08-18', 'UTC'), to_datetime('2016-08-18 00:00:00', 'UTC'), 'UTC');

-- DateTime vs Date32
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('minute', to_datetime('2015-08-18 00:00:00', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('hour', to_datetime('2015-08-18 00:00:00', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('day', to_datetime('2015-08-18 00:00:00', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('week', to_datetime('2015-08-18 00:00:00', 'UTC'), to_date('2015-08-25', 'UTC'), 'UTC');
SELECT date_diff('month', to_datetime('2015-08-18 00:00:00', 'UTC'), to_date('2015-09-18', 'UTC'), 'UTC');
SELECT date_diff('quarter', to_datetime('2015-08-18 00:00:00', 'UTC'), to_date('2015-11-18', 'UTC'), 'UTC');
SELECT date_diff('year', to_datetime('2015-08-18 00:00:00', 'UTC'), to_date('2016-08-18', 'UTC'), 'UTC');

-- With Date
-- Date32 vs Date
SELECT date_diff('second', to_date('2015-08-18', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('minute', to_date('2015-08-18', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('hour', to_date('2015-08-18', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('week', to_date('2015-08-18', 'UTC'), to_date('2015-08-25', 'UTC'), 'UTC');
SELECT date_diff('month', to_date('2015-08-18', 'UTC'), to_date('2015-09-18', 'UTC'), 'UTC');
SELECT date_diff('quarter', to_date('2015-08-18', 'UTC'), to_date('2015-11-18', 'UTC'), 'UTC');
SELECT date_diff('year', to_date('2015-08-18', 'UTC'), to_date('2016-08-18', 'UTC'), 'UTC');

-- Date vs Date32
SELECT date_diff('second', to_date('2015-08-18', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('minute', to_date('2015-08-18', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('hour', to_date('2015-08-18', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('week', to_date('2015-08-18', 'UTC'), to_date('2015-08-25', 'UTC'), 'UTC');
SELECT date_diff('month', to_date('2015-08-18', 'UTC'), to_date('2015-09-18', 'UTC'), 'UTC');
SELECT date_diff('quarter', to_date('2015-08-18', 'UTC'), to_date('2015-11-18', 'UTC'), 'UTC');
SELECT date_diff('year', to_date('2015-08-18', 'UTC'), to_date('2016-08-18', 'UTC'), 'UTC');

-- Const vs non-const columns
SELECT date_diff('day', to_date('1927-01-01', 'UTC'), materialize(to_date('1927-01-02', 'UTC')), 'UTC');
SELECT date_diff('day', to_date('1927-01-01', 'UTC'), materialize(to_datetime64('1927-01-02 00:00:00', 3, 'UTC')), 'UTC');
SELECT date_diff('day', to_datetime64('1927-01-01 00:00:00', 3, 'UTC'), materialize(to_date('1927-01-02', 'UTC')), 'UTC');
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), materialize(to_datetime('2015-08-19 00:00:00', 'UTC')), 'UTC');
SELECT date_diff('day', to_datetime('2015-08-18 00:00:00', 'UTC'), materialize(to_date('2015-08-19', 'UTC')), 'UTC');
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), materialize(to_date('2015-08-19', 'UTC')), 'UTC');
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), materialize(to_date('2015-08-19', 'UTC')), 'UTC');

-- Non-const vs const columns
SELECT date_diff('day', materialize(to_date('1927-01-01', 'UTC')), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('day', materialize(to_date('1927-01-01', 'UTC')), to_datetime64('1927-01-02 00:00:00', 3, 'UTC'), 'UTC');
SELECT date_diff('day', materialize(to_datetime64('1927-01-01 00:00:00', 3, 'UTC')), to_date('1927-01-02', 'UTC'), 'UTC');
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), to_datetime('2015-08-19 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('day', materialize(to_datetime('2015-08-18 00:00:00', 'UTC')), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), to_date('2015-08-19', 'UTC'), 'UTC');
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), to_date('2015-08-19', 'UTC'), 'UTC');

-- Non-const vs non-const columns
SELECT date_diff('day', materialize(to_date('1927-01-01', 'UTC')), materialize(to_date('1927-01-02', 'UTC')), 'UTC');
SELECT date_diff('day', materialize(to_date('1927-01-01', 'UTC')), materialize(to_datetime64('1927-01-02 00:00:00', 3, 'UTC')), 'UTC');
SELECT date_diff('day', materialize(to_datetime64('1927-01-01 00:00:00', 3, 'UTC')), materialize(to_date('1927-01-02', 'UTC')), 'UTC');
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), materialize(to_datetime('2015-08-19 00:00:00', 'UTC')), 'UTC');
SELECT date_diff('day', materialize(to_datetime('2015-08-18 00:00:00', 'UTC')), materialize(to_date('2015-08-19', 'UTC')), 'UTC');
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), materialize(to_date('2015-08-19', 'UTC')), 'UTC');
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), materialize(to_date('2015-08-19', 'UTC')), 'UTC');
