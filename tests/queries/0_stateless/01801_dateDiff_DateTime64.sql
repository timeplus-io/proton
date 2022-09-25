-- { echo }

-- DateTime64 vs DateTime64 same scale
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 00:00:10', 0, 'UTC'));
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 00:10:00', 0, 'UTC'));
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 01:00:00', 0, 'UTC'));
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 01:10:10', 0, 'UTC'));

SELECT date_diff('minute', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 00:10:00', 0, 'UTC'));
SELECT date_diff('minute', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 10:00:00', 0, 'UTC'));

SELECT date_diff('hour', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-01 10:00:00', 0, 'UTC'));

SELECT date_diff('day', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-01-02 00:00:00', 0, 'UTC'));
SELECT date_diff('month', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1927-02-01 00:00:00', 0, 'UTC'));
SELECT date_diff('year', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), toDateTime64('1928-01-01 00:00:00', 0, 'UTC'));

-- DateTime64 vs DateTime64 different scale
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 00:00:10', 3, 'UTC'));
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 00:10:00', 3, 'UTC'));
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 01:00:00', 3, 'UTC'));
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 01:10:10', 3, 'UTC'));

SELECT date_diff('minute', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 00:10:00', 3, 'UTC'));
SELECT date_diff('minute', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 10:00:00', 3, 'UTC'));

SELECT date_diff('hour', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-01 10:00:00', 3, 'UTC'));

SELECT date_diff('day', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-01-02 00:00:00', 3, 'UTC'));
SELECT date_diff('month', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1927-02-01 00:00:00', 3, 'UTC'));
SELECT date_diff('year', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), toDateTime64('1928-01-01 00:00:00', 3, 'UTC'));

-- With datetime
-- DateTime64 vs datetime
SELECT date_diff('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:00:00', 'UTC'));
SELECT date_diff('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:00:10', 'UTC'));
SELECT date_diff('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:10:00', 'UTC'));
SELECT date_diff('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 01:00:00', 'UTC'));
SELECT date_diff('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 01:10:10', 'UTC'));

-- datetime vs DateTime64
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 00:00:00', 3, 'UTC'));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 00:00:10', 3, 'UTC'));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 00:10:00', 3, 'UTC'));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 01:00:00', 3, 'UTC'));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), toDateTime64('2015-08-18 01:10:10', 3, 'UTC'));

-- With date
-- DateTime64 vs date
SELECT date_diff('day', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), to_date('2015-08-19', 'UTC'));

-- date vs DateTime64
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), toDateTime64('2015-08-19 00:00:00', 3, 'UTC'));

-- Same thing but const vs non-const columns
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 0, 'UTC'), materialize(toDateTime64('1927-01-01 00:00:10', 0, 'UTC')));
SELECT date_diff('second', toDateTime64('1927-01-01 00:00:00', 6, 'UTC'), materialize(toDateTime64('1927-01-01 00:00:10', 3, 'UTC')));
SELECT date_diff('second', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), materialize(to_datetime('2015-08-18 00:00:10', 'UTC')));
SELECT date_diff('second', to_datetime('2015-08-18 00:00:00', 'UTC'), materialize(toDateTime64('2015-08-18 00:00:10', 3, 'UTC')));
SELECT date_diff('day', toDateTime64('2015-08-18 00:00:00', 0, 'UTC'), materialize(to_date('2015-08-19', 'UTC')));
SELECT date_diff('day', to_date('2015-08-18', 'UTC'), materialize(toDateTime64('2015-08-19 00:00:00', 3, 'UTC')));

-- Same thing but non-const vs const columns
SELECT date_diff('second', materialize(toDateTime64('1927-01-01 00:00:00', 0, 'UTC')), toDateTime64('1927-01-01 00:00:10', 0, 'UTC'));
SELECT date_diff('second', materialize(toDateTime64('1927-01-01 00:00:00', 6, 'UTC')), toDateTime64('1927-01-01 00:00:10', 3, 'UTC'));
SELECT date_diff('second', materialize(toDateTime64('2015-08-18 00:00:00', 0, 'UTC')), to_datetime('2015-08-18 00:00:10', 'UTC'));
SELECT date_diff('second', materialize(to_datetime('2015-08-18 00:00:00', 'UTC')), toDateTime64('2015-08-18 00:00:10', 3, 'UTC'));
SELECT date_diff('day', materialize(toDateTime64('2015-08-18 00:00:00', 0, 'UTC')), to_date('2015-08-19', 'UTC'));
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), toDateTime64('2015-08-19 00:00:00', 3, 'UTC'));

-- Same thing but non-const vs non-const columns
SELECT date_diff('second', materialize(toDateTime64('1927-01-01 00:00:00', 0, 'UTC')), materialize(toDateTime64('1927-01-01 00:00:10', 0, 'UTC')));
SELECT date_diff('second', materialize(toDateTime64('1927-01-01 00:00:00', 6, 'UTC')), materialize(toDateTime64('1927-01-01 00:00:10', 3, 'UTC')));
SELECT date_diff('second', materialize(toDateTime64('2015-08-18 00:00:00', 0, 'UTC')), materialize(to_datetime('2015-08-18 00:00:10', 'UTC')));
SELECT date_diff('second', materialize(to_datetime('2015-08-18 00:00:00', 'UTC')), materialize(toDateTime64('2015-08-18 00:00:10', 3, 'UTC')));
SELECT date_diff('day', materialize(toDateTime64('2015-08-18 00:00:00', 0, 'UTC')), materialize(to_date('2015-08-19', 'UTC')));
SELECT date_diff('day', materialize(to_date('2015-08-18', 'UTC')), materialize(toDateTime64('2015-08-19 00:00:00', 3, 'UTC')));
