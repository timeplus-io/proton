create stream t
(
    `d` date,
    `s` LowCardinality(FixedString(3)),
    `c` uint32
)
ENGINE = SummingMergeTree()
PARTITION BY d
ORDER BY (d, s);

INSERT INTO t (d, s, c) VALUES ('2020-01-01', 'ABC', 1);
INSERT INTO t (d, s, c) VALUES ('2020-01-01', 'ABC', 2);

OPTIMIZE STREAM t;
SELECT * FROM t;

DROP STREAM t;
