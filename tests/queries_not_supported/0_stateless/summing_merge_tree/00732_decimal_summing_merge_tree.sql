-- Tags: not_supported, blocked_by_SummingMergeTree
SET query_mode = 'table';
DROP STREAM IF EXISTS decimal_sum;
create stream decimal_sum
(
    date date,
    sum32 Decimal32(4),
    sum64 Decimal64(8),
    sum128 Decimal128(10)
) Engine = SummingMergeTree(date, (date), 8192);

INSERT INTO decimal_sum VALUES ('2001-01-01', 1, 1, -1);
INSERT INTO decimal_sum VALUES ('2001-01-01', 1, -1, -1);

OPTIMIZE STREAM decimal_sum;
SELECT * FROM decimal_sum;

INSERT INTO decimal_sum VALUES ('2001-01-01', -2, 1, 2);

OPTIMIZE STREAM decimal_sum;
SELECT * FROM decimal_sum;

INSERT INTO decimal_sum VALUES ('2001-01-01', 0, -1, 0);

OPTIMIZE STREAM decimal_sum;
SELECT * FROM decimal_sum;

drop stream decimal_sum;
