-- Tags: no-parallel
DROP STREAM IF EXISTS lc_table;

CREATE STREAM lc_table
(
    col low_cardinality(string)
) ENGINE=TinyLog;

INSERT INTO lc_table VALUES('x');

SELECT * FROM lc_table INNER JOIN lc_table AS lc_table2
ON lc_table.col = lc_table2.col;

SELECT * FROM lc_table INNER JOIN lc_table AS lc_table2
ON CAST(lc_table.col AS string) = CAST(lc_table2.col AS string);

SELECT * FROM lc_table INNER JOIN lc_table AS lc_table2
ON (lc_table.col = lc_table2.col) OR (lc_table.col = lc_table2.col);

SELECT * FROM lc_table INNER JOIN lc_table AS lc_table2
ON (CAST(lc_table.col AS string) = CAST(lc_table2.col AS string)) OR (CAST(lc_table.col AS string) = CAST(lc_table2.col AS string));

DROP STREAM IF EXISTS lc_table;
