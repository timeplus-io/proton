SELECT 'aaaabb ' == trim(leading 'b ' FROM 'b aaaabb ') x;
SELECT 'b aaaa' == trim(trailing 'b ' FROM 'b aaaabb ') x;
SELECT 'aaaa' == trim(both 'b ' FROM 'b aaaabb ') x;

SELECT '1' ==  replace_regex(',,1,,', '^[,]*|[,]*$', '') x;
SELECT '1' ==  replace_regex(',,1', '^[,]*|[,]*$', '') x;
SELECT '1' ==  replace_regex('1,,', '^[,]*|[,]*$', '') x;

SELECT '1,,' ==  replace_regexp_one(',,1,,', '^[,]*|[,]*$', '') x;
SELECT '1' ==  replace_regexp_one(',,1', '^[,]*|[,]*$', '') x;
SELECT '1,,' ==  replace_regexp_one('1,,', '^[,]*|[,]*$', '') x;

SELECT '5935,5998,6014' == trim(BOTH ', ' FROM '5935,5998,6014, ') x;
SELECT '5935,5998,6014' ==  replace_regex('5935,5998,6014, ', concat('^[', regexpQuoteMeta(', '), ']*|[', regexpQuoteMeta(', '), ']*$'), '') AS x;
