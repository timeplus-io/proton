CREATE STREAM stream1 (column1 string) ENGINE=MergeTree() ORDER BY tuple();
CREATE STREAM stream2 (column1 string, column2 string, column3 string) ENGINE=MergeTree() ORDER BY tuple();
CREATE STREAM stream3 (column3 string) ENGINE=MergeTree() ORDER BY tuple();

SELECT
    *
FROM
(
    SELECT
        column1
    FROM stream1
    GROUP BY
        column1
) AS a
ANY LEFT JOIN
(
    SELECT
        *
    FROM stream2
) AS b ON (b.column1 = a.column1) AND (b.column2 = a.column2)
ANY LEFT JOIN
(
    SELECT
        *
    FROM stream3
) AS c ON c.column3 = b.column3; -- {serverError UNKNOWN_IDENTIFIER}
