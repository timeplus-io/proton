DROP STREAM IF EXISTS test_99027;
CREATE STREAM test_99027(number uint64);

INSERT INTO test_99027(number) SETTINGS idempotent_id='test_99027', max_block_size=10 SELECT * FROM numbers(10000);
SELECT sleep(3);
SELECT count() FROM table(test_99027);

INSERT INTO test_99027(number) SETTINGS idempotent_id='test_99027', max_block_size=10 SELECT * FROM numbers(10000);
SELECT sleep(3);
SELECT count() FROM table(test_99027);

INSERT INTO test_99027(number) SETTINGS idempotent_id='test_99027_max_insert_block_size', max_insert_block_size=10 SELECT * FROM numbers(10000);
SELECT sleep(3);
SELECT count() FROM table(test_99027);

INSERT INTO test_99027(number) SETTINGS idempotent_id='test_99027_max_insert_block_size', max_insert_block_size=10 SELECT * FROM numbers(10000);
SELECT sleep(3);
SELECT count() FROM table(test_99027);

DROP STREAM test_99027;
