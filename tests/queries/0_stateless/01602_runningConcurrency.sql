--
SELECT 'Invocation with date columns';

DROP STREAM IF EXISTS runningConcurrency_test;
create stream runningConcurrency_test(begin date, end date) ;

INSERT INTO runningConcurrency_test VALUES ('2020-12-01', '2020-12-10'), ('2020-12-02', '2020-12-10'), ('2020-12-03', '2020-12-12'), ('2020-12-10', '2020-12-12'), ('2020-12-13', '2020-12-20');
SELECT runningConcurrency(begin, end) FROM runningConcurrency_test;

DROP STREAM runningConcurrency_test;

--
SELECT 'Invocation with datetime';

DROP STREAM IF EXISTS runningConcurrency_test;
create stream runningConcurrency_test(begin datetime, end datetime) ;

INSERT INTO runningConcurrency_test VALUES ('2020-12-01 00:00:00', '2020-12-01 00:59:59'), ('2020-12-01 00:30:00', '2020-12-01 00:59:59'), ('2020-12-01 00:40:00', '2020-12-01 01:30:30'), ('2020-12-01 01:10:00', '2020-12-01 01:30:30'), ('2020-12-01 01:50:00', '2020-12-01 01:59:59');
SELECT runningConcurrency(begin, end) FROM runningConcurrency_test;

DROP STREAM runningConcurrency_test;

--
SELECT 'Invocation with DateTime64';

DROP STREAM IF EXISTS runningConcurrency_test;
create stream runningConcurrency_test(begin DateTime64(3), end DateTime64(3)) ;

INSERT INTO runningConcurrency_test VALUES ('2020-12-01 00:00:00.000', '2020-12-01 00:00:00.100'), ('2020-12-01 00:00:00.010', '2020-12-01 00:00:00.100'), ('2020-12-01 00:00:00.020', '2020-12-01 00:00:00.200'), ('2020-12-01 00:00:00.150', '2020-12-01 00:00:00.200'), ('2020-12-01 00:00:00.250', '2020-12-01 00:00:00.300');
SELECT runningConcurrency(begin, end) FROM runningConcurrency_test;

DROP STREAM runningConcurrency_test;

--
SELECT 'Erroneous cases';

-- Constant columns are currently not supported.
SELECT runningConcurrency(to_date(array_join([1, 2])), to_date('2000-01-01')); -- { serverError 44 }

-- Unsupported data types
SELECT runningConcurrency('strings are', 'not supported'); -- { serverError 43 }
SELECT runningConcurrency(NULL, NULL); -- { serverError 43 }
SELECT runningConcurrency(CAST(NULL, 'nullable(datetime)'), CAST(NULL, 'nullable(datetime)')); -- { serverError 43 }

-- Mismatching data types
SELECT runningConcurrency(to_date('2000-01-01'), to_datetime('2000-01-01 00:00:00')); -- { serverError 43 }

-- begin > end
SELECT runningConcurrency(to_date('2000-01-02'), to_date('2000-01-01')); -- { serverError 117 }

                                                       
