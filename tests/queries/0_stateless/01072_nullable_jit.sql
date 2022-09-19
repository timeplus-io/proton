SET compile_expressions = 1;

DROP STREAM IF EXISTS foo;

create stream foo (
    id uint32,
    a float64,
    b float64,
    c float64,
    d float64
) Engine = MergeTree()
  PARTITION BY id
  ORDER BY id;

INSERT INTO foo VALUES (1, 0.5, 0.2, 0.3, 0.8);

SELECT divide(sum(a) + sum(b), nullIf(sum(c) + sum(d), 0)) FROM foo;
SELECT divide(sum(a) + sum(b), nullIf(sum(c) + sum(d), 0)) FROM foo;

DROP STREAM foo;
