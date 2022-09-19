-- Tags: distributed

DROP STREAM IF EXISTS numbers_memory;
create stream numbers_memory AS system.numbers ;
INSERT INTO numbers_memory SELECT number FROM system.numbers LIMIT 100;
SELECT DISTINCT number FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_memory) ORDER BY number LIMIT 10;
DROP STREAM numbers_memory;
