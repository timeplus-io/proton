DROP STREAM IF EXISTS has_function;

create stream has_function(arr array(nullable(string))) ;
INSERT INTO has_function(arr) values ([null, 'str1', 'str2']),(['str1', 'str2']), ([]), ([]);

SELECT arr, has(`arr`, 'str1') FROM has_function;
SELECT has([null, 'str1', 'str2'], 'str1');

DROP STREAM has_function;
