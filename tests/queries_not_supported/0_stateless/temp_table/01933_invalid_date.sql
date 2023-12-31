SELECT to_date('07-08-2019'); -- { serverError 6 }
SELECT to_date('2019-0708'); -- { serverError 38 }
SELECT to_date('201907-08'); -- { serverError 38 }
SELECT to_date('2019^7^8');

CREATE TEMPORARY STREAM test (d date);
INSERT INTO test VALUES ('2018-01-01');

SELECT * FROM test WHERE d >= '07-08-2019'; -- { serverError 53 }
SELECT * FROM test WHERE d >= '2019-07-08';
