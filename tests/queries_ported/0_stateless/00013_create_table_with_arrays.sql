SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS arrays_test;
create stream arrays_test (s string, arr array(uint8));
INSERT INTO arrays_test (s, arr) VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
SELECT sleep(3);
SELECT * FROM arrays_test;
SELECT s, arr FROM arrays_test ARRAY JOIN arr;
SELECT s, arr, a FROM arrays_test ARRAY JOIN arr AS a;
SELECT s, arr, a, num FROM arrays_test ARRAY JOIN arr AS a, array_enumerate(arr) AS num;
SELECT s, arr, a, num, array_enumerate(arr) FROM arrays_test ARRAY JOIN arr AS a, array_enumerate(arr) AS num;
SELECT s, arr, a, mapped FROM arrays_test ARRAY JOIN arr AS a, array_map(x -> x + 1, arr) AS mapped;
SELECT s, arr, a, num, mapped FROM arrays_test ARRAY JOIN arr AS a, array_enumerate(arr) AS num, array_map(x -> x + 1, arr) AS mapped;
SELECT sum_array(arr), sum_array_if(arr, to_bool(s LIKE '%l%')), sum_array_if(arr, to_bool(s LIKE '%e%')) FROM arrays_test;
DROP STREAM arrays_test;
