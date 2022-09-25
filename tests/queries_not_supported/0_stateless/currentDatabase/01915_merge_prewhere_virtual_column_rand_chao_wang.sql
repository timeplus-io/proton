DROP STREAM IF EXISTS abc;

create stream abc
(
    `f1` string,
    `f2` string
)
ENGINE = MergeTree()
ORDER BY f1;

-- In version 20.12 this query sometimes produces an exception "Cannot find column"
SELECT f2 FROM merge(currentDatabase(), '^abc$') PREWHERE _table = 'abc' AND f1 = 'a' AND rand() % 100 < 20;
SELECT f2 FROM merge(currentDatabase(), '^abc$') PREWHERE _table = 'abc' AND f1 = 'a';

DROP STREAM abc;
