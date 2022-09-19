-- Tags: not_supported, blocked_by_SummingMergeTree
DROP STREAM IF EXISTS tab_00625;

create stream tab_00625
(
    date date,
    key uint32,
    testMap nested(
    k UInt16,
    v uint64)
)
ENGINE = SummingMergeTree(date, (date, key), 1);

INSERT INTO tab_00625 SELECT
    today(),
    number,
    [to_uint16(number)],
    [number]
FROM system.numbers
LIMIT 8190;

INSERT INTO tab_00625 SELECT
    today(),
    number + 8190,
    [to_uint16(number)],
    [number + 8190]
FROM system.numbers
LIMIT 10;

OPTIMIZE STREAM tab_00625;

DROP STREAM tab_00625;
