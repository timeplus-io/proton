DROP STREAM IF EXISTS test_grace_hash;

CREATE STREAM test_grace_hash (id uint32, value uint64) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_grace_hash SELECT number, number % 100 = 0 FROM numbers(100000);

SET join_algorithm = 'grace_hash';

SELECT count() FROM (
    SELECT f.id FROM test_grace_hash AS f
    LEFT JOIN test_grace_hash AS d
    ON f.id = d.id
    LIMIT 1000
);

DROP STREAM test_grace_hash;