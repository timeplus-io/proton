--
SELECT 'Invocation with Date columns';

DROP STREAM IF EXISTS running_concurrency_test;
CREATE STREAM running_concurrency_test(begin Date, end Date) ENGINE = Memory;

INSERT INTO running_concurrency_test VALUES ('2020-12-01', '2020-12-10'), ('2020-12-02', '2020-12-10'), ('2020-12-03', '2020-12-12'), ('2020-12-10', '2020-12-12'), ('2020-12-13', '2020-12-20');
SELECT running_concurrency(begin, end) FROM running_concurrency_test;

DROP STREAM running_concurrency_test;

--
SELECT 'Invocation with DateTime';

DROP STREAM IF EXISTS running_concurrency_test;
CREATE STREAM running_concurrency_test(begin DateTime, end DateTime) ENGINE = Memory;

INSERT INTO running_concurrency_test VALUES ('2020-12-01 00:00:00', '2020-12-01 00:59:59'), ('2020-12-01 00:30:00', '2020-12-01 00:59:59'), ('2020-12-01 00:40:00', '2020-12-01 01:30:30'), ('2020-12-01 01:10:00', '2020-12-01 01:30:30'), ('2020-12-01 01:50:00', '2020-12-01 01:59:59');
SELECT running_concurrency(begin, end) FROM running_concurrency_test;

DROP STREAM running_concurrency_test;

--
SELECT 'Invocation with DateTime64';

DROP STREAM IF EXISTS running_concurrency_test;
CREATE STREAM running_concurrency_test(begin DateTime64(3), end DateTime64(3)) ENGINE = Memory;

INSERT INTO running_concurrency_test VALUES ('2020-12-01 00:00:00.000', '2020-12-01 00:00:00.100'), ('2020-12-01 00:00:00.010', '2020-12-01 00:00:00.100'), ('2020-12-01 00:00:00.020', '2020-12-01 00:00:00.200'), ('2020-12-01 00:00:00.150', '2020-12-01 00:00:00.200'), ('2020-12-01 00:00:00.250', '2020-12-01 00:00:00.300');
SELECT running_concurrency(begin, end) FROM running_concurrency_test;

DROP STREAM running_concurrency_test;

--
SELECT 'Erroneous cases';

-- Constant columns are currently not supported.
SELECT running_concurrency(to_date(array_join([1, 2])), to_date('2000-01-01')); -- { serverError 44 }

-- Unsupported data types
SELECT running_concurrency('strings are', 'not supported'); -- { serverError 43 }
SELECT running_concurrency(NULL, NULL); -- { serverError 43 }
SELECT running_concurrency(CAST(NULL, 'nullable(DateTime)'), CAST(NULL, 'nullable(DateTime)')); -- { serverError 43 }

-- Mismatching data types
SELECT running_concurrency(to_date('2000-01-01'), to_datetime('2000-01-01 00:00:00')); -- { serverError 43 }

-- begin > end
SELECT running_concurrency(to_date('2000-01-02'), to_date('2000-01-01')); -- { serverError 117 }

                                                       
