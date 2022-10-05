SELECT number IN (1, NULL, 3) FROM system.numbers LIMIT 5;
SELECT null_if(number, 2) IN (1, NULL, 3) FROM system.numbers LIMIT 5;
SELECT null_if(number, 2) IN (1, 2, 3) FROM system.numbers LIMIT 5;

SELECT number IN (SELECT number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT number IN (SELECT null_if(number, 2) FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT null_if(number, 4) IN (SELECT null_if(number, 2) FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;


SELECT to_string(number) IN ('1', NULL, '3') FROM system.numbers LIMIT 5;
SELECT null_if(to_string(number), '2') IN ('1', NULL, '3') FROM system.numbers LIMIT 5;
SELECT null_if(to_string(number), '2') IN ('1', '2', '3') FROM system.numbers LIMIT 5;

SELECT to_string(number) IN (SELECT to_string(number) FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT to_string(number) IN (SELECT null_if(to_string(number), '2') FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT null_if(to_string(number), '4') IN (SELECT null_if(to_string(number), '2') FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;


SELECT (number, -number) IN ((1, -1), (NULL, NULL), (3, -3)) FROM system.numbers LIMIT 5;
SELECT (null_if(number, 2), -number) IN ((1, -1), (NULL, NULL), (3, -3)) FROM system.numbers LIMIT 5;
SELECT (null_if(number, 2), -number) IN ((1, -1), (2, -2), (3, -3)) FROM system.numbers LIMIT 5;
SELECT (null_if(number, 2), -null_if(number, 2)) IN ((1, -1), (NULL, NULL), (3, -3)) FROM system.numbers LIMIT 5;
SELECT (null_if(number, 2), -null_if(number, 2)) IN ((1, -1), (2, -2), (3, -3)) FROM system.numbers LIMIT 5;

SELECT (number, -number) IN (SELECT number, -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT (number, -number) IN (SELECT null_if(number, 2), -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT (null_if(number, 4), -number) IN (SELECT null_if(number, 2), -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT (number, -null_if(number, 3)) IN (SELECT null_if(number, 2), -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
SELECT (null_if(number, 4), -null_if(number, 3)) IN (SELECT null_if(number, 2), -number FROM system.numbers LIMIT 1, 3) AS res FROM system.numbers LIMIT 5;
