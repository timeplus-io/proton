CREATE STREAM test_a (id uint32, company uint32, total uint64) ENGINE=SummingMergeTree() PARTITION BY company PRIMARY KEY (id) ORDER BY (id, company);
INSERT INTO test_a SELECT number%10 as id, number%2 as company, count() as total FROM numbers(100) GROUP BY id,company;
CREATE STREAM test_b (id uint32, company uint32, total uint64) ENGINE=SummingMergeTree() PARTITION BY company ORDER BY (id, company);
ALTER STREAM test_b REPLACE PARTITION '0' FROM test_a; -- {serverError BAD_ARGUMENTS}
