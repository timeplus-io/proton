SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL 0 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL 1 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL 11 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL 12 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL 13 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL -1 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL -11 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL -12 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL -13 MONTH AS x;

SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL 0 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL 1 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL 11 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL 12 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL 13 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL -1 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL -11 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL -12 MONTH AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL -13 MONTH AS x;

SELECT to_date('2017-01-01') + INTERVAL 0 MONTH AS x;
SELECT to_date('2017-01-01') + INTERVAL 1 MONTH AS x;
SELECT to_date('2017-01-01') + INTERVAL 11 MONTH AS x;
SELECT to_date('2017-01-01') + INTERVAL 12 MONTH AS x;
SELECT to_date('2017-01-01') + INTERVAL 13 MONTH AS x;
SELECT to_date('2017-01-01') + INTERVAL -1 MONTH AS x;
SELECT to_date('2017-01-01') + INTERVAL -11 MONTH AS x;
SELECT to_date('2017-01-01') + INTERVAL -12 MONTH AS x;
SELECT to_date('2017-01-01') + INTERVAL -13 MONTH AS x;

SELECT to_date('2017-01-01') - INTERVAL 0 MONTH AS x;
SELECT to_date('2017-01-01') - INTERVAL 1 MONTH AS x;
SELECT to_date('2017-01-01') - INTERVAL 11 MONTH AS x;
SELECT to_date('2017-01-01') - INTERVAL 12 MONTH AS x;
SELECT to_date('2017-01-01') - INTERVAL 13 MONTH AS x;
SELECT to_date('2017-01-01') - INTERVAL -1 MONTH AS x;
SELECT to_date('2017-01-01') - INTERVAL -11 MONTH AS x;
SELECT to_date('2017-01-01') - INTERVAL -12 MONTH AS x;
SELECT to_date('2017-01-01') - INTERVAL -13 MONTH AS x;

SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL 0 YEAR AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL 1 YEAR AS x;
SELECT to_datetime('2017-01-01 00:00:00') + INTERVAL -1 YEAR AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL 0 YEAR AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL 1 YEAR AS x;
SELECT to_datetime('2017-01-01 00:00:00') - INTERVAL -1 YEAR AS x;

SELECT to_date('2017-01-01') + INTERVAL 0 YEAR AS x;
SELECT to_date('2017-01-01') + INTERVAL 1 YEAR AS x;
SELECT to_date('2017-01-01') + INTERVAL -1 YEAR AS x;
SELECT to_date('2017-01-01') - INTERVAL 0 YEAR AS x;
SELECT to_date('2017-01-01') - INTERVAL 1 YEAR AS x;
SELECT to_date('2017-01-01') - INTERVAL -1 YEAR AS x;


SELECT to_date('2017-01-01') + INTERVAL number - 15 MONTH AS x FROM system.numbers LIMIT 30;
SELECT to_date('2017-01-01') - INTERVAL number - 15 MONTH AS x FROM system.numbers LIMIT 30;

SELECT to_date('2017-01-01') + INTERVAL number - 15 YEAR AS x FROM system.numbers LIMIT 30;
SELECT to_date('2017-01-01') - INTERVAL number - 15 YEAR AS x FROM system.numbers LIMIT 30;
