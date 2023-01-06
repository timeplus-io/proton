SELECT  array_distinct([1, 2, 3]);
SELECT  array_distinct([1, 2, 3, 2, 2]);
SELECT  array_distinct([1, 2, NULL, 5, 2, NULL]);

SELECT  array_distinct(['1212', 'sef', '343r4']);
SELECT  array_distinct(['1212', 'sef', '343r4', '1212']);
SELECT  array_distinct(['1212', 'sef', '343r4', NULL, NULL, '232']);

DROP STREAM IF EXISTS arrayDistinct_test;
create stream arrayDistinct_test(arr_int array(uint8), arr_string array(string)) ENGINE=Memory;
INSERT INTO arrayDistinct_test values ([1, 2, 3], ['a', 'b', 'c']), ([21, 21, 21, 21], ['123', '123', '123']);

SELECT  array_distinct(arr_int) FROM arrayDistinct_test;
SELECT  array_distinct(arr_string) FROM arrayDistinct_test;

DROP STREAM arrayDistinct_test;

SELECT  array_distinct([['1212'], ['sef'], ['343r4'], ['1212']]);
SELECT  array_distinct([(1, 2), (1, 3), (1, 2), (1, 2), (1, 2), (1, 5)]);
SELECT length( array_distinct([NULL, NULL, NULL]));
