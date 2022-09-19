SET query_mode = 'table';
DROP STREAM IF EXISTS big_array;
create stream big_array (x array(uint8));
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO big_array(x) SELECT group_array(number % 255) AS x FROM (SELECT * FROM system.numbers LIMIT 1000000);
SELECT sleep(5);
SELECT count() FROM big_array ARRAY JOIN x;
SELECT count() FROM big_array ARRAY JOIN x AS y;
SELECT count_if(has(x, 10)), sum(y) FROM big_array ARRAY JOIN x AS y;
SELECT count_if(has(x, 10)) FROM big_array ARRAY JOIN x AS y;
SELECT count_if(has(x, 10)), sum(y) FROM big_array ARRAY JOIN x AS y WHERE 1;
SELECT count_if(has(x, 10)) FROM big_array ARRAY JOIN x AS y WHERE 1;
SELECT count_if(has(x, 10)), sum(y) FROM big_array ARRAY JOIN x AS y WHERE has(x,15);

DROP STREAM big_array;
