DROP STREAM IF EXISTS t_desc_subcolumns;

create stream t_desc_subcolumns
(
    d date,
    n Nullable(string) COMMENT 'It is a nullable column',
    arr1 array(uint32) CODEC(ZSTD),
    arr2 array(array(string)) TTL d + INTERVAL 1 DAY,
    t tuple(s string, a array(tuple(a uint32, b uint32))) CODEC(ZSTD)
)
ENGINE = MergeTree ORDER BY d;

DESCRIBE TABLE t_desc_subcolumns FORMAT PrettyCompactNoEscapes;

DESCRIBE TABLE t_desc_subcolumns FORMAT PrettyCompactNoEscapes
SETTINGS describe_include_subcolumns = 1;

DROP STREAM t_desc_subcolumns;
