drop stream if exists tbl_repr;

CREATE STREAM tbl_repr(
ts DateTime,
x  string)
ENGINE=MergeTree ORDER BY ts;


SELECT *
FROM
(
    SELECT
        x,
        length(x)
    FROM tbl_repr
    WHERE ts > now()
    LIMIT 1
)
WHERE x != '';

drop stream if exists tbl_repr;
