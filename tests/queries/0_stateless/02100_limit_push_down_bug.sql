SET query_mode = 'table';
drop stream if exists tbl_repr;

create stream tbl_repr(
ts datetime,
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
