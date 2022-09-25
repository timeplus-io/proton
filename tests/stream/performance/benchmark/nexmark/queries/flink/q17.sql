-- -------------------------------------------------------------------------------------------------
-- Query 17: Auction Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids on an auction made a day and what is the price?
-- Illustrates an unbounded group aggregation.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  auction BIGINT,
  `day` VARCHAR,
  total_bids BIGINT,
  rank1_bids BIGINT,
  rank2_bids BIGINT,
  rank3_bids BIGINT,
  min_price BIGINT,
  max_price BIGINT,
  avg_price BIGINT,
  sum_price BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
     auction,
     DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     min(price) AS min_price,
     max(price) AS max_price,
     avg(price) AS avg_price,
     sum(price) AS sum_price
FROM bid
GROUP BY auction, DATE_FORMAT(dateTime, 'yyyy-MM-dd');