DROP STREAM IF EXISTS test_02187;
create stream test_02187 (
    info string,
    id int32
)
ENGINE = ReplacingMergeTree(id)
ORDER BY id;

INSERT INTO TABLE test_02187 VALUES ('nothing', 1);
INSERT INTO TABLE test_02187 VALUES ('something', 1);

SELECT * FROM test_02187 FINAL;
SELECT * FROM test_02187 FINAL LIMIT 1;


