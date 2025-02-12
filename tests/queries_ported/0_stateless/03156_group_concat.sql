SET query_mode = 'table';
DROP STREAM IF EXISTS test_group_concat;
CREATE STREAM test_group_concat
(
    id uint64,
    p_int nullable(int32),
    p_string string,
    p_array array(int32)
) ORDER BY id;

SET max_insert_threads = 1, max_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
INSERT INTO test_group_concat (id, p_int, p_string, p_array) VALUES (0, 95, 'abc', [1, 2, 3]), (1, NULL, 'a', [993, 986, 979, 972]), (2, 123, 'makson95', []);

SELECT sleep(3);
SELECT id, p_int, p_string, p_array FROM table(test_group_concat);

SELECT group_concat(p_int) FROM test_group_concat;
SELECT group_concat(p_string) FROM test_group_concat;
SELECT group_concat(p_array) FROM test_group_concat;

SELECT group_concat(p_int, ',') FROM test_group_concat;
SELECT group_concat(p_string, ',') FROM test_group_concat;
SELECT group_concat(p_array, ',', 2) FROM test_group_concat;

SELECT group_concat(p_int) FROM test_group_concat WHERE id = 1;

INSERT INTO test_group_concat (id, p_int, p_string, p_array) VALUES (0, 95, 'abc', [1, 2, 3]), (1, NULL, 'a', [993, 986, 979, 972]), (2, 123, 'makson95', []);
SELECT sleep(1);
INSERT INTO test_group_concat (id, p_int, p_string, p_array) VALUES (0, 95, 'abc', [1, 2, 3]), (1, NULL, 'a', [993, 986, 979, 972]), (2, 123, 'makson95', []);

SELECT sleep(3);
SELECT group_concat(p_int) FROM test_group_concat;
SELECT group_concat(p_string, ',') FROM test_group_concat;
SELECT group_concat(p_array) FROM test_group_concat;

SELECT group_concat(number, 123) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT group_concat(number, ',', '3') FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT group_concat(number, ',', 0) FROM numbers(10); -- { serverError BAD_GET }
SELECT group_concat(number, ',', -1) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT group_concat(number, ',', 3, 3) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT length(group_concat(number)) FROM numbers(100000);

DROP STREAM IF EXISTS test_group_concat;

CREATE STREAM test_group_concat
(
    id uint64,
    p_int int32
) ORDER BY id;

INSERT INTO test_group_concat (id, p_int) SELECT number, number FROM numbers(100000) SETTINGS min_insert_block_size_rows = 2000;

SELECT sleep(3);
SELECT length(group_concat(p_int)) FROM test_group_concat;

DROP STREAM IF EXISTS test_group_concat;
