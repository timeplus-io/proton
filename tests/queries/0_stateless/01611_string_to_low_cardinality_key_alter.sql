DROP STREAM IF EXISTS table_with_lc_key;

create stream table_with_lc_key
(
    enum_key Enum8('x' = 2, 'y' = 1),
    lc_key LowCardinality(string),
    value string
)
ENGINE MergeTree()
ORDER BY (enum_key, lc_key);

INSERT INTO table_with_lc_key VALUES(1, 'hello', 'world');

ALTER STREAM table_with_lc_key MODIFY COLUMN lc_key string;

SHOW create stream table_with_lc_key;

DETACH TABLE table_with_lc_key;
ATTACH TABLE table_with_lc_key;

SELECT * FROM table_with_lc_key WHERE enum_key > 0 and lc_key like 'h%';

ALTER STREAM table_with_lc_key MODIFY COLUMN enum_key Enum('x' = 2, 'y' = 1, 'z' = 3);
ALTER STREAM table_with_lc_key MODIFY COLUMN enum_key Enum16('x' = 2, 'y' = 1, 'z' = 3); --{serverError 524}
SHOW create stream table_with_lc_key;

DETACH TABLE table_with_lc_key;
ATTACH TABLE table_with_lc_key;

SELECT * FROM table_with_lc_key WHERE enum_key > 0 and lc_key like 'h%';

ALTER STREAM table_with_lc_key MODIFY COLUMN enum_key int8;

SHOW create stream table_with_lc_key;

DETACH TABLE table_with_lc_key;
ATTACH TABLE table_with_lc_key;

SELECT * FROM table_with_lc_key WHERE enum_key > 0 and lc_key like 'h%';

DROP STREAM IF EXISTS table_with_lc_key;


DROP STREAM IF EXISTS table_with_string_key;
create stream table_with_string_key
(
    int_key int8,
    str_key string,
    value string
)
ENGINE MergeTree()
ORDER BY (int_key, str_key);

INSERT INTO table_with_string_key VALUES(1, 'hello', 'world');

ALTER STREAM table_with_string_key MODIFY COLUMN str_key LowCardinality(string);

SHOW create stream table_with_string_key;

DETACH TABLE table_with_string_key;
ATTACH TABLE table_with_string_key;

SELECT * FROM table_with_string_key WHERE int_key > 0 and str_key like 'h%';

ALTER STREAM table_with_string_key MODIFY COLUMN int_key Enum8('y' = 1, 'x' = 2); --{serverError 524}

DROP STREAM IF EXISTS table_with_string_key;
