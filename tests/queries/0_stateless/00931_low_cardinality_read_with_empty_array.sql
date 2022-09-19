DROP STREAM IF EXISTS lc_00931;

create stream lc_00931 (
    key uint64,
    value array(LowCardinality(string)))
ENGINE = MergeTree
ORDER BY key;

INSERT INTO lc_00931 SELECT number,
if (number < 10000 OR number > 100000,
    [to_string(number)],
    empty_array_string())
    FROM system.numbers LIMIT 200000;

SELECT * FROM lc_00931
WHERE (key < 100 OR key > 50000)
    AND NOT has(value, to_string(key))
    AND length(value) == 1
LIMIT 10
SETTINGS max_block_size = 8192,
         max_threads = 1;

DROP STREAM IF EXISTS lc_00931;
