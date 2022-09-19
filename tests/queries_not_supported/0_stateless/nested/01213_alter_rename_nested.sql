DROP STREAM IF EXISTS table_for_rename_nested;
create stream table_for_rename_nested
(
    date date,
    key uint64,
    n nested(x uint32, y string),
    value1 array(array(LowCardinality(string))) -- column with several files
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY key;

INSERT INTO table_for_rename_nested (date, key, n.x, n.y, value1) SELECT to_date('2019-10-01'), number, [number + 1, number + 2, number + 3], ['a', 'b', 'c'], [[to_string(number)]] FROM numbers(10);

SELECT n.x FROM table_for_rename_nested WHERE key = 7;
SELECT n.y FROM table_for_rename_nested WHERE key = 7;

SHOW create stream table_for_rename_nested;

ALTER STREAM table_for_rename_nested RENAME COLUMN n.x TO n.renamed_x;
ALTER STREAM table_for_rename_nested RENAME COLUMN n.y TO n.renamed_y;

SHOW create stream table_for_rename_nested;

SELECT key, n.renamed_x FROM table_for_rename_nested WHERE key = 7;
SELECT key, n.renamed_y FROM table_for_rename_nested WHERE key = 7;

ALTER STREAM table_for_rename_nested RENAME COLUMN n.renamed_x TO not_nested_x; --{serverError 36}

-- Currently not implemented
ALTER STREAM table_for_rename_nested RENAME COLUMN n TO renamed_n; --{serverError 48}

ALTER STREAM table_for_rename_nested RENAME COLUMN value1 TO renamed_value1;

SELECT renamed_value1 FROM table_for_rename_nested WHERE key = 7;

SHOW create stream table_for_rename_nested;

SELECT * FROM table_for_rename_nested WHERE key = 7 FORMAT TSVWithNames;

DROP STREAM IF EXISTS table_for_rename_nested;

