SET query_mode = 'table';
create stream IF NOT EXISTS empty_tiny_log(A uint8);

SELECT A FROM empty_tiny_log;

DROP STREAM empty_tiny_log;
