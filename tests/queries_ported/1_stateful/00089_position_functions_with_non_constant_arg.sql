SET max_threads = 0; -- let's reset to automatic detection of the number of threads, otherwise test can be slow.

SELECT count() FROM test.hits WHERE position(URL, 'metrika') != position(URL, materialize('metrika'));
SELECT count() FROM test.hits WHERE position_case_insensitive(URL, 'metrika') != position_case_insensitive(URL, materialize('metrika'));
SELECT count() FROM test.hits WHERE position_utf8(Title, 'новости') != position_utf8(Title, materialize('новости'));
SELECT count() FROM test.hits WHERE position_case_insensitive_utf8(Title, 'новости') != position_case_insensitive_utf8(Title, materialize('новости'));

SELECT position(URL, split_by_string('/',URL)[3]) AS x FROM test.hits WHERE x = 0 AND URL NOT LIKE '%yandex.ru%' LIMIT 100;
SELECT URL FROM test.hits WHERE x > 10 ORDER BY position(URL, split_by_string('/',URL)[3]) AS x DESC, URL LIMIT 2;
SELECT DISTINCT URL, URLDomain, position('http://yandex.ru/', split_by_string('/',URL)[3]) AS x FROM test.hits WHERE x > 8 ORDER BY position('http://yandex.ru/', split_by_string('/',URL)[3]) DESC, URL LIMIT 3;
