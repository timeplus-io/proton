-- { echo }

-- DateTime64 vs DateTime64 same scale
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 00:00:10', 0, 'UTC'));
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 00:10:00', 0, 'UTC'));
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 01:00:00', 0, 'UTC'));
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 01:10:10', 0, 'UTC'));

SELECT age('minute', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 00:10:00', 0, 'UTC'));
SELECT age('minute', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 10:00:00', 0, 'UTC'));

SELECT age('hour', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-01 10:00:00', 0, 'UTC'));

SELECT age('day', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-01-02 00:00:00', 0, 'UTC'));
SELECT age('month', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1927-02-01 00:00:00', 0, 'UTC'));
SELECT age('year', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), to_datetime64('1928-01-01 00:00:00', 0, 'UTC'));

-- DateTime64 vs DateTime64 different scale
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 00:00:10', 3, 'UTC'));
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 00:10:00', 3, 'UTC'));
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 01:00:00', 3, 'UTC'));
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 01:10:10', 3, 'UTC'));

SELECT age('minute', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 00:10:00', 3, 'UTC'));
SELECT age('minute', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 10:00:00', 3, 'UTC'));

SELECT age('hour', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-01 10:00:00', 3, 'UTC'));

SELECT age('day', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-01-02 00:00:00', 3, 'UTC'));
SELECT age('month', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1927-02-01 00:00:00', 3, 'UTC'));
SELECT age('year', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), to_datetime64('1928-01-01 00:00:00', 3, 'UTC'));

-- With DateTime
-- DateTime64 vs DateTime
SELECT age('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:00:00', 'UTC'));
SELECT age('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:00:10', 'UTC'));
SELECT age('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 00:10:00', 'UTC'));
SELECT age('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 01:00:00', 'UTC'));
SELECT age('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_datetime('2015-08-18 01:10:10', 'UTC'));

-- DateTime vs DateTime64
SELECT age('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 00:00:00', 3, 'UTC'));
SELECT age('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 00:00:10', 3, 'UTC'));
SELECT age('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 00:10:00', 3, 'UTC'));
SELECT age('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 01:00:00', 3, 'UTC'));
SELECT age('second', to_datetime('2015-08-18 00:00:00', 'UTC'), to_datetime64('2015-08-18 01:10:10', 3, 'UTC'));

-- With Date
-- DateTime64 vs Date
SELECT age('day', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), to_date('2015-08-19', 'UTC'));

-- Date vs DateTime64
SELECT age('day', to_date('2015-08-18', 'UTC'), to_datetime64('2015-08-19 00:00:00', 3, 'UTC'));

-- Same thing but const vs non-const columns
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 0, 'UTC'), materialize(to_datetime64('1927-01-01 00:00:10', 0, 'UTC')));
SELECT age('second', to_datetime64('1927-01-01 00:00:00', 6, 'UTC'), materialize(to_datetime64('1927-01-01 00:00:10', 3, 'UTC')));
SELECT age('second', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), materialize(to_datetime('2015-08-18 00:00:10', 'UTC')));
SELECT age('second', to_datetime('2015-08-18 00:00:00', 'UTC'), materialize(to_datetime64('2015-08-18 00:00:10', 3, 'UTC')));
SELECT age('day', to_datetime64('2015-08-18 00:00:00', 0, 'UTC'), materialize(to_date('2015-08-19', 'UTC')));
SELECT age('day', to_date('2015-08-18', 'UTC'), materialize(to_datetime64('2015-08-19 00:00:00', 3, 'UTC')));

-- Same thing but non-const vs const columns
SELECT age('second', materialize(to_datetime64('1927-01-01 00:00:00', 0, 'UTC')), to_datetime64('1927-01-01 00:00:10', 0, 'UTC'));
SELECT age('second', materialize(to_datetime64('1927-01-01 00:00:00', 6, 'UTC')), to_datetime64('1927-01-01 00:00:10', 3, 'UTC'));
SELECT age('second', materialize(to_datetime64('2015-08-18 00:00:00', 0, 'UTC')), to_datetime('2015-08-18 00:00:10', 'UTC'));
SELECT age('second', materialize(to_datetime('2015-08-18 00:00:00', 'UTC')), to_datetime64('2015-08-18 00:00:10', 3, 'UTC'));
SELECT age('day', materialize(to_datetime64('2015-08-18 00:00:00', 0, 'UTC')), to_date('2015-08-19', 'UTC'));
SELECT age('day', materialize(to_date('2015-08-18', 'UTC')), to_datetime64('2015-08-19 00:00:00', 3, 'UTC'));

-- Same thing but non-const vs non-const columns
SELECT age('second', materialize(to_datetime64('1927-01-01 00:00:00', 0, 'UTC')), materialize(to_datetime64('1927-01-01 00:00:10', 0, 'UTC')));
SELECT age('second', materialize(to_datetime64('1927-01-01 00:00:00', 6, 'UTC')), materialize(to_datetime64('1927-01-01 00:00:10', 3, 'UTC')));
SELECT age('second', materialize(to_datetime64('2015-08-18 00:00:00', 0, 'UTC')), materialize(to_datetime('2015-08-18 00:00:10', 'UTC')));
SELECT age('second', materialize(to_datetime('2015-08-18 00:00:00', 'UTC')), materialize(to_datetime64('2015-08-18 00:00:10', 3, 'UTC')));
SELECT age('day', materialize(to_datetime64('2015-08-18 00:00:00', 0, 'UTC')), materialize(to_date('2015-08-19', 'UTC')));
SELECT age('day', materialize(to_date('2015-08-18', 'UTC')), materialize(to_datetime64('2015-08-19 00:00:00', 3, 'UTC')));
