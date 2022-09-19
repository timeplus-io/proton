-- Tags: distributed

SET max_memory_usage = 300000000;
SET max_bytes_before_external_sort = 20000000;
SET query_mode = 'table';
drop stream IF EXISTS numbers10m;
CREATE VIEW numbers10m AS SELECT number FROM system.numbers LIMIT 10000000;

SELECT number FROM remote('127.0.0.{2,3}', currentDatabase(), numbers10m) ORDER BY number * 1234567890123456789 LIMIT 19999980, 20;

drop stream numbers10m;
