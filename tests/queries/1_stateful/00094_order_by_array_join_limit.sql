SELECT `ParsedParams.Key2` AS x
FROM test.hits
ARRAY JOIN ParsedParams AS PP
ORDER BY x ASC
LIMIT 2;

SELECT array_join(`ParsedParams.Key2`) AS x FROM test.hits ORDER BY x ASC LIMIT 2;
WITH array_join(`ParsedParams.Key2`) AS pp SELECT ParsedParams.Key2 AS x FROM test.hits ORDER BY x ASC LIMIT 2;
WITH array_join(`ParsedParams.Key2`) AS pp SELECT ParsedParams.Key2 AS x FROM test.hits WHERE NOT ignore(pp) ORDER BY x ASC LIMIT 2;
