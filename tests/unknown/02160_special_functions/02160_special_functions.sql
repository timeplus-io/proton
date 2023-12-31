SELECT CAST(1 AS uint8);
SELECT CAST([] AS array(uint8));
SELECT CAST(1, 'uint8');

SELECT SUBSTRING('Hello, world' FROM 8);
SELECT SUBSTRING('Hello, world' FROM 8 FOR 5);
SELECT SUBSTRING('Hello, world', 8);
SELECT SUBSTRING('Hello, world', 8, 5);

SELECT TRIM(LEADING 'abc' FROM 'abcdef');
SELECT TRIM(TRAILING 'def' FROM 'abcdef');
SELECT TRIM(BOTH 'af' FROM 'abcdef');
SELECT TRIM(' abcdef ');
SELECT LTRIM(' abcdef ');
SELECT RTRIM(' abcdef ');

SELECT EXTRACT(YEAR FROM DATE '2022-01-01');
SELECT EXTRACT('Hello, world', '^\w+');

SELECT POSITION('ll' IN 'Hello');
SELECT POSITION('Hello', 'll');

SELECT DATE_ADD(YEAR, 1, DATE '2022-01-01');
SELECT DATE_ADD(INTERVAL 1 YEAR, DATE '2022-01-01');
SELECT DATEADD(YEAR, 1, DATE '2022-01-01');
SELECT DATEADD(INTERVAL 1 YEAR, DATE '2022-01-01');
SELECT TIMESTAMP_ADD(YEAR, 1, DATE '2022-01-01');
SELECT TIMESTAMP_ADD(INTERVAL 1 YEAR, DATE '2022-01-01');
SELECT TIMESTAMPADD(YEAR, 1, DATE '2022-01-01');
SELECT TIMESTAMPADD(INTERVAL 1 YEAR, DATE '2022-01-01');

SELECT DATE_SUB(YEAR, 1, DATE '2022-01-01');
SELECT DATE_SUB(DATE '2022-01-01', INTERVAL 1 YEAR);
SELECT DATESUB(YEAR, 1, DATE '2022-01-01');
SELECT DATESUB(DATE '2022-01-01', INTERVAL 1 YEAR);
SELECT TIMESTAMP_SUB(YEAR, 1, DATE '2022-01-01');
SELECT TIMESTAMP_SUB(DATE '2022-01-01', INTERVAL 1 YEAR);
SELECT TIMESTAMPSUB(YEAR, 1, DATE '2022-01-01');
SELECT TIMESTAMPSUB(DATE '2022-01-01', INTERVAL 1 YEAR);

SELECT DATE_DIFF(YEAR, DATE '2021-01-01', DATE '2022-01-01');
SELECT DATEDIFF(YEAR, DATE '2021-01-01', DATE '2022-01-01');

SELECT EXISTS (SELECT 1);
