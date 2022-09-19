DROP VIEW IF EXISTS test_view;
DROP STREAM IF EXISTS test_table;

create stream test_table
(
    f1 int32,
    f2 int32,
    pk int32
)
ENGINE = MergeTree()
ORDER BY f1
PARTITION BY pk;

CREATE VIEW test_view AS
SELECT f1, f2
FROM test_table
WHERE pk = 2;

INSERT INTO test_table (f1, f2, pk) VALUES (1,1,1), (1,1,2), (2,1,1), (2,1,2);

SELECT * FROM test_view ORDER BY f1, f2;

ALTER STREAM test_view DELETE WHERE pk = 2; --{serverError 48}

SELECT * FROM test_view ORDER BY f1, f2;

DROP VIEW IF EXISTS test_view;
DROP STREAM IF EXISTS test_table;
