SELECT 'aaaabb ' == trim(leading 'b ' FROM 'b aaaabb ') as x;
SELECT 'b aaaa' == trim(trailing 'b ' FROM 'b aaaabb ') as x;
SELECT 'aaaa' == trim(both 'b ' FROM 'b aaaabb ') as x;

SELECT '1' == replace_regexp_all(',,1,,', '^[,]*|[,]*$', '') as x;
SELECT '1' == replace_regexp_all(',,1', '^[,]*|[,]*$', '') as x;
SELECT '1' == replace_regexp_all('1,,', '^[,]*|[,]*$', '') as x;

SELECT '1,,' == replace_regexp_one(',,1,,', '^[,]*|[,]*$', '') as x;
SELECT '1' == replace_regexp_one(',,1', '^[,]*|[,]*$', '') as x;
SELECT '1,,' == replace_regexp_one('1,,', '^[,]*|[,]*$', '') as x;

SELECT '5935,5998,6014' == trim(BOTH ', ' FROM '5935,5998,6014, ') as x;
SELECT '5935,5998,6014' == replace_regexp_all('5935,5998,6014, ', concat('^[', regexp_quote_meta(', '), ']*|[', regexp_quote_meta(', '), ']*$'), '') AS x;

SELECT trim(BOTH '"' FROM '2') == '2'
