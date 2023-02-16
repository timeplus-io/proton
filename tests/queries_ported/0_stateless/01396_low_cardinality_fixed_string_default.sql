DROP STREAM IF EXISTS test;
CREATE STREAM test
(
  id   uint32,
  code low_cardinality(fixed_string(2)) DEFAULT '--'
) ENGINE = MergeTree() PARTITION BY id ORDER BY id;

INSERT INTO test FORMAT CSV 1,RU
INSERT INTO test FORMAT CSV 1,

SELECT * FROM test ORDER BY code;
OPTIMIZE TABLE test;
SELECT * FROM test ORDER BY code;

DROP STREAM test;
