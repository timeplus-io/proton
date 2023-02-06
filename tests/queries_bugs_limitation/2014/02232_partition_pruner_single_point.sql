DROP STREAM IF EXISTS lower_test;

CREATE STREAM lower_test (
    a int32,
    b string
) ENGINE=MergeTree
PARTITION BY b
ORDER BY a;

INSERT INTO lower_test (a,b) VALUES (1,'A'),(2,'B'),(3,'C');

SELECT a FROM lower_test WHERE lower(b) IN ('a','b') order by a;

DROP STREAM lower_test;
