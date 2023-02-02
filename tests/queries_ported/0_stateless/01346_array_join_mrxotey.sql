DROP STREAM IF EXISTS test;

CREATE STREAM test (
    a Date,
    b uint32,
    c uint64,
    p nested (
        at1 string,
        at2 string
    )
) ENGINE = MergeTree()
PARTITION BY a
ORDER BY b
SETTINGS index_granularity = 8192;

INSERT INTO test (a, b, c, p.at1, p.at2)
VALUES (now(), 1, 2, ['foo', 'bar'], ['baz', 'qux']);

SELECT b
FROM test
ARRAY JOIN p
WHERE
    b = 1
    AND c IN (
        SELECT c FROM test
    );

DROP STREAM test;
