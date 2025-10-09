set optimize_distinct_in_order=1;

DROP STREAM IF EXISTS test_string;

CREATE STREAM test_string
(
    `c1` string,
    `c2` string
)
ENGINE = MergeTree
ORDER BY c1;

INSERT INTO test_string(c1, c2) VALUES ('1',  ''), ('2', '');

SELECT DISTINCT c2, c1 FROM test_string;
