SELECT array_join([0,0,0,0,0,0,0,0,0,0,0,1,2,2,3,4,12,NULL]) AS x ORDER BY x;
SELECT array_join([0,0,0,0,0,0,0,0,0,0,0,1,2,2,3,4,12,NULL]) AS x ORDER BY x DESC;

SET max_block_size = 1000;

SELECT null_if(number, number % 3 = 0 ? number : 0) AS x FROM (SELECT * FROM system.numbers LIMIT 10) ORDER BY x;
SELECT null_if(number, number % 3 = 0 ? number : 0) AS x FROM (SELECT * FROM system.numbers LIMIT 10) ORDER BY x DESC;

SET max_block_size = 5;

SELECT null_if(number, number % 3 = 0 ? number : 0) AS x FROM (SELECT * FROM system.numbers LIMIT 10) ORDER BY x;
SELECT null_if(number, number % 3 = 0 ? number : 0) AS x FROM (SELECT * FROM system.numbers LIMIT 10) ORDER BY x DESC;

SET max_block_size = 1000;

SELECT null_if(number, number % 3 = 0 ? number : 0) AS x, number AS y FROM (SELECT * FROM system.numbers LIMIT 10) ORDER BY x, y;
SELECT null_if(number, number % 3 = 0 ? number : 0) AS x, number AS y FROM (SELECT * FROM system.numbers LIMIT 10) ORDER BY x DESC, y;

SET max_block_size = 5;

SELECT null_if(number, number % 3 = 0 ? number : 0) AS x, number AS y FROM (SELECT * FROM system.numbers LIMIT 10) ORDER BY x, y;
SELECT null_if(number, number % 3 = 0 ? number : 0) AS x, number AS y FROM (SELECT * FROM system.numbers LIMIT 10) ORDER BY x DESC, y;

SELECT x FROM (SELECT to_nullable(number) AS x FROM system.numbers LIMIT 3) ORDER BY x DESC LIMIT 10
