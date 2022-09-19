DROP STREAM IF EXISTS mt;
create stream mt (d date, x uint8) ENGINE = MergeTree(d, x, 8192);
INSERT INTO mt VALUES (52392, 1), (62677, 2);
DROP STREAM mt;
