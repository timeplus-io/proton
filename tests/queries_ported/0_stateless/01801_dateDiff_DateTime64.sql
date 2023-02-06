-- { echo }

-- DateTime64 vs DateTime64 same scale
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 00:00:10', 0, 'UTC'));
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 00:10:00', 0, 'UTC'));
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 01:00:00', 0, 'UTC'));
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 01:10:10', 0, 'UTC'));

SELECT date_diff('minute', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 00:10:00', 0, 'UTC'));
SELECT date_diff('minute', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 10:00:00', 0, 'UTC'));

SELECT date_diff('hour', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 10:00:00', 0, 'UTC'));

SELECT date_diff('day', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-02 00:00:00', 0, 'UTC'));
SELECT date_diff('month', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-02-01 00:00:00', 0, 'UTC'));
SELECT date_diff('year', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1928-01-01 00:00:00', 0, 'UTC'));

-- DateTime64 vs DateTime64 different scale
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 00:00:10', 3, 'UTC'));
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 00:10:00', 3, 'UTC'));
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 01:00:00', 3, 'UTC'));
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 01:10:10', 3, 'UTC'));

SELECT date_diff('minute', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 00:10:00', 3, 'UTC'));
SELECT date_diff('minute', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 10:00:00', 3, 'UTC'));

SELECT date_diff('hour', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 10:00:00', 3, 'UTC'));

SELECT date_diff('day', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-02 00:00:00', 3, 'UTC'));
SELECT date_diff('month', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-02-01 00:00:00', 3, 'UTC'));
SELECT date_diff('year', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1928-01-01 00:00:00', 3, 'UTC'));

-- With DateTime
-- DateTime64 vs DateTime
SELECT date_diff('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:00:00', 'UTC'));
SELECT date_diff('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:00:10', 'UTC'));
SELECT date_diff('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:10:00', 'UTC'));
SELECT date_diff('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 01:00:00', 'UTC'));
SELECT date_diff('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 01:10:10', 'UTC'));

-- DateTime vs DateTime64
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 00:00:00', 3, 'UTC'));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 00:00:10', 3, 'UTC'));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 00:10:00', 3, 'UTC'));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 01:00:00', 3, 'UTC'));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 01:10:10', 3, 'UTC'));

-- With Date
-- DateTime64 vs Date
SELECT date_diff('day', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_date('2015-08-19', 'UTC'));

-- Date vs DateTime64
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), to_datetime64('2015-08-19 00:00:00', 3, 'UTC'));

-- Same thing but const vs non-const columns
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), materialize(to_datetime64('1927-01-01 00:00:10', 0, 'UTC')));
SELECT date_diff('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), materialize(to_datetime64('1927-01-01 00:00:10', 3, 'UTC')));
SELECT date_diff('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), materialize(to_datetime('2015-08-18 00:00:10', 'UTC')));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), materialize(to_datetime64('2015-08-18 00:00:10', 3, 'UTC')));
SELECT date_diff('day', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), materialize(to_date('2015-08-19', 'UTC')));
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), materialize(to_datetime64('2015-08-19 00:00:00', 3, 'UTC')));

-- Same thing but non-const vs const columns
SELECT date_diff('second', materialize(to_datetime64('1927-01-01 00:00:00', 0, 'UTC')), to_datetime64('1927-01-01 00:00:10', 0, 'UTC'));
SELECT date_diff('second', materialize(to_datetime64('1927-01-01 00:00:00', 6, 'UTC')), to_datetime64('1927-01-01 00:00:10', 3, 'UTC'));
SELECT date_diff('second', materialize(to_datetime64('2015-08-18 00:00:00', 0, 'UTC')), to_datetime('2015-08-18 00:00:10', 'UTC'));
SELECT date_diff('second', materialize(to_datetime('2015-08-18 00:00:00', 'UTC')), to_datetime64('2015-08-18 00:00:10', 3, 'UTC'));
SELECT date_diff('day', materialize(to_datetime64('2015-08-18 00:00:00', 0, 'UTC')), to_date('2015-08-19', 'UTC'));
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), to_datetime64('2015-08-19 00:00:00', 3, 'UTC'));

-- Same thing but non-const vs non-const columns
SELECT date_diff('second', materialize(to_datetime64('1927-01-01 00:00:00', 0, 'UTC')), materialize(to_datetime64('1927-01-01 00:00:10', 0, 'UTC')));
SELECT date_diff('second', materialize(to_datetime64('1927-01-01 00:00:00', 6, 'UTC')), materialize(to_datetime64('1927-01-01 00:00:10', 3, 'UTC')));
SELECT date_diff('second', materialize(to_datetime64('2015-08-18 00:00:00', 0, 'UTC')), materialize(to_datetime('2015-08-18 00:00:10', 'UTC')));
SELECT date_diff('second', materialize(to_datetime('2015-08-18 00:00:00', 'UTC')), materialize(to_datetime64('2015-08-18 00:00:10', 3, 'UTC')));
SELECT date_diff('day', materialize(to_datetime64('2015-08-18 00:00:00', 0, 'UTC')), materialize(to_date('2015-08-19', 'UTC')));
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), materialize(to_datetime64('2015-08-19 00:00:00', 3, 'UTC')));
