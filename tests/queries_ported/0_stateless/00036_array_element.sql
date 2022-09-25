SET query_mode = 'table';
DROP STREAM IF EXISTS array_element_test;
create stream array_element_test (arr array(int32), id int32);
insert into array_element_test(arr, id) VALUES ([11,12,13], 2), ([11,12], 3), ([11,12,13], -1), ([11,12], -2), ([11,12], -3), ([11], 0);

SELECT sleep(3);
select arr[id] from array_element_test;

DROP STREAM IF EXISTS array_element_test;
create stream array_element_test (arr array(int32), id uint32);
insert into array_element_test(arr, id) VALUES ([11,12,13], 2), ([11,12], 3), ([11,12,13], 1), ([11,12], 4), ([11], 0);
SELECT sleep(3);
select arr[id] from array_element_test;

DROP STREAM IF EXISTS array_element_test;
create stream array_element_test (arr array(string), id int32);
insert into array_element_test(arr, id) VALUES (['Abc','Df','Q'], 2), (['Abc','DEFQ'], 3), (['ABC','Q','ERT'], -1), (['Ab','ber'], -2), (['AB','asd'], -3), (['A'], 0);
SELECT sleep(3);
select arr[id] from array_element_test;

DROP STREAM IF EXISTS array_element_test;
create stream array_element_test (arr array(string), id uint32);
insert into array_element_test(arr, id) VALUES (['Abc','Df','Q'], 2), (['Abc','DEFQ'], 3), (['ABC','Q','ERT'], 1), (['Ab','ber'], 4), (['A'], 0);
SELECT sleep(3);
select arr[id] from array_element_test;

DROP STREAM IF EXISTS array_element_test;
create stream array_element_test (id uint32);
insert into array_element_test(id) VALUES (2), (1), (4), (3), (0);
SELECT sleep(3);
select [1, 2, 3] as arr, arr[id] from array_element_test;

DROP STREAM IF EXISTS array_element_test;
create stream array_element_test (id int32);
insert into array_element_test(id) VALUES (-2), (1), (-4), (3), (2), (-1), (4), (-3), (0);
SELECT sleep(3);
select [1, 2, 3] as arr, arr[id] from array_element_test;

DROP STREAM array_element_test;
