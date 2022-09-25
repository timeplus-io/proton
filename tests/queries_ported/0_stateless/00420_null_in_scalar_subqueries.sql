SELECT (SELECT 1 WHERE false);
SELECT (SELECT * FROM (SELECT * FROM system.numbers LIMIT 2) WHERE number = number + 1);
SELECT (SELECT NULL WHERE false);
SELECT (SELECT Null WHERE nuLL IS NOT NULL);
SELECT (SELECT Null WHERE true);
SELECT CAST(NULL as nullable(nothing));
SELECT (SELECT CAST(NULL as nullable(nothing)) WHERE false);
SELECT (SELECT 1 WHERE false) AS a, (SELECT 1 WHERE true) AS b FORMAT TSVWithNames;
