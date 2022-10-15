-- Tags: no-fasttest

DROP STREAM IF EXISTS test.avro;

SET max_threads = 1, max_block_size = 8192, min_insert_block_size_rows = 8192, min_insert_block_size_bytes = 1048576; -- lower memory usage

CREATE STREAM test.avro AS test.hits ENGINE = File(Avro);
INSERT INTO test.avro SELECT * FROM table(test.hits) LIMIT 10000;

SELECT sum(cityHash64(*)) FROM (SELECT * FROM table(test.hits) LIMIT 10000);
SELECT sum(cityHash64(*)) FROM test.avro;

DROP STREAM test.avro;
