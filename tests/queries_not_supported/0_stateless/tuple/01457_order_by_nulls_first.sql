SET query_mode = 'table';
drop stream if exists order_by_nulls_first;

create stream  order_by_nulls_first
(diff Nullable(int16), traf uint64)
ENGINE = MergeTree ORDER BY tuple();

insert into order_by_nulls_first values (NULL,1),(NULL,0),(NULL,0),(NULL,0),(NULL,0),(NULL,0),(28,0),(0,0);

SELECT
    diff,
    traf
FROM order_by_nulls_first
order by diff desc NULLS FIRST, traf
limit 1, 4;

select '--- DESC NULLS FIRST, ASC';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff DESC NULLS FIRST,
    traf ASC;

select '--- DESC NULLS LAST, ASC';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff DESC NULLS LAST,
    traf ASC;

select '--- ASC NULLS FIRST, ASC';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff ASC NULLS FIRST,
    traf ASC;

select '--- ASC NULLS LAST, ASC';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff ASC NULLS LAST,
    traf ASC;

select '--- DESC NULLS FIRST, DESC';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff DESC NULLS FIRST,
    traf DESC;

select '--- DESC NULLS LAST, DESC';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff DESC NULLS LAST,
    traf DESC;

select '--- ASC NULLS FIRST, DESC';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff ASC NULLS FIRST,
    traf DESC;

select '--- ASC NULLS LAST, DESC';

SELECT
    diff,
    traf
FROM order_by_nulls_first
ORDER BY
    diff ASC NULLS LAST,
    traf DESC;

drop stream if exists order_by_nulls_first;