-- -------------------------------------------------------------------------------------------------
-- Query 14: Calculation (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Convert bid timestamp into types and find bids with specific price.
-- Illustrates duplicate expressions and usage of user-defined-functions.
-- -------------------------------------------------------------------------------------------------

CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';

CREATE stream discard_sink (
    auction int64,
    bidder int64,
    price  DECIMAL(23, 3),
    bidTimeType string,
    dateTime datetime64(3),
    extra string,
    c_counts int64
);

INSERT INTO discard_sink
SELECT 
    auction,
    bidder,
    0.908 * price as price,
    CASE
        WHEN HOUR(dateTime) >= 8 AND HOUR(dateTime) <= 18 THEN 'dayTime'
        WHEN HOUR(dateTime) <= 6 OR HOUR(dateTime) >= 20 THEN 'nightTime'
        ELSE 'otherTime'
    END AS bidTimeType,
    dateTime,
    extra,
    count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;