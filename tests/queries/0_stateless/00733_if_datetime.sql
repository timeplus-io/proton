SELECT (number % 2 <> 0) ? to_datetime('2000-01-01 00:00:00') : to_datetime('2001-02-03 04:05:06') FROM numbers(2);
SELECT (number % 2 <> 0) ? to_datetime('2000-01-01 00:00:00') : materialize(to_datetime('2001-02-03 04:05:06')) FROM numbers(2);
SELECT (number % 2 <> 0) ? materialize(to_datetime('2000-01-01 00:00:00')) : to_datetime('2001-02-03 04:05:06') FROM numbers(2);
SELECT (number % 2 <> 0) ? materialize(to_datetime('2000-01-01 00:00:00')) : materialize(to_datetime('2001-02-03 04:05:06')) FROM numbers(2);

SELECT (number % 2 <> 0) ? to_date('2000-01-01') : to_date('2001-02-03') FROM numbers(2);
SELECT (number % 2 <> 0) ? to_date('2000-01-01') : materialize(to_date('2001-02-03')) FROM numbers(2);
SELECT (number % 2 <> 0) ? materialize(to_date('2000-01-01')) : to_date('2001-02-03') FROM numbers(2);
SELECT (number % 2 <> 0) ? materialize(to_date('2000-01-01')) : materialize(to_date('2001-02-03')) FROM numbers(2);
