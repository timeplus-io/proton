SELECT
    neighbor(n, -2) AS int,
    neighbor(s, -2) AS str,
    neighbor(lcs, -2) AS lowCstr
FROM
(
    SELECT
        number % 5 AS n,
        to_string(n) AS s,
        CAST(s, 'low_cardinality(string)') AS lcs
    FROM numbers(10)
);

SET query_mode = 'table';
drop stream if exists neighbor_test;

create stream neighbor_test
(
    `rowNr` uint8,
    `val_string` string,
    `val_low` low_cardinality(string)
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY rowNr;

INSERT INTO neighbor_test VALUES (1, 'string 1', 'string 1'), (2, 'string 1', 'string 1'), (3, 'string 2', 'string 2');

SELECT
    rowNr,
    val_string,
    neighbor(val_string, -1) AS str_m1,
    neighbor(val_string, 1) AS str_p1,
    val_low,
    neighbor(val_low, -1) AS low_m1,
    neighbor(val_low, 1) AS low_p1
FROM
(
    SELECT *
    FROM neighbor_test
    ORDER BY val_string ASC
) format PrettyCompact;

drop stream if exists neighbor_test;
