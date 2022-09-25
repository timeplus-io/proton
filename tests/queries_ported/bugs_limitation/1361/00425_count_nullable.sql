SELECT count() FROM (SELECT number AS x FROM system.numbers LIMIT 10);

SELECT count(x) FROM (SELECT number AS x FROM system.numbers LIMIT 10);

SELECT count(x) FROM (SELECT CAST(number AS nullable(uint64)) AS x FROM system.numbers LIMIT 10);

SELECT count(x) FROM (SELECT null_if(number, 5) AS x FROM system.numbers LIMIT 10);

SELECT count(NULL);
