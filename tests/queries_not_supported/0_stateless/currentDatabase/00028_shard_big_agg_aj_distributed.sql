-- Tags: distributed, not_supported, blocked_by_currentDatabase()

SET query_mode = 'table';
DROP STREAM IF EXISTS big_array;
create stream big_array (x array(uint8));
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO big_array(x) SELECT group_array(number % 255) AS x FROM (SELECT * FROM system.numbers LIMIT 1000000);
SELECT sleep(3);
SELECT sum(y) AS s FROM remote('127.0.0.{2,3}', currentDatabase(), big_array) ARRAY JOIN x AS y;
SELECT sum(s) FROM (SELECT y AS s FROM remote('127.0.0.{2,3}', currentDatabase(), big_array) ARRAY JOIN x AS y);
DROP STREAM big_array;
