DROP STREAM IF EXISTS cool_table;

CREATE STREAM IF NOT EXISTS cool_table
(
    id uint64,
    n nested(n uint64, lc1 low_cardinality(string))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO cool_table SELECT number, range(number), range(number) FROM numbers(10);

ALTER STREAM cool_table ADD COLUMN IF NOT EXISTS `n.lc2` array(low_cardinality(string));

SELECT n.lc1, n.lc2 FROM cool_table ORDER BY id;

DROP STREAM IF EXISTS cool_table;

CREATE STREAM IF NOT EXISTS cool_table
(
    id uint64,
    n nested(n uint64, lc1 array(low_cardinality(string)))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO cool_table SELECT number, range(number), array_map(x -> range(x % 4), range(number)) FROM numbers(10);

ALTER STREAM cool_table ADD COLUMN IF NOT EXISTS `n.lc2` array(array(low_cardinality(string)));

SELECT n.lc1, n.lc2 FROM cool_table ORDER BY id;

DROP STREAM IF EXISTS cool_table;

CREATE STREAM IF NOT EXISTS cool_table
(
    id uint64,
    n nested(n uint64, lc1 map(low_cardinality(string), uint64))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO cool_table SELECT number, range(number), array_map(x -> (array_map(y -> 'k' || to_string(y), range(x % 4)), range(x % 4))::map(low_cardinality(string), uint64), range(number)) FROM numbers(10);

ALTER STREAM cool_table ADD COLUMN IF NOT EXISTS `n.lc2` array(map(low_cardinality(string), uint64));

SELECT n.lc1, n.lc2 FROM cool_table ORDER BY id;

DROP STREAM IF EXISTS cool_table;
