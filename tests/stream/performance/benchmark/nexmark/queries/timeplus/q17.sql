-- -------------------------------------------------------------------------------------------------
-- Query 17: Auction Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids on an auction made a day and what is the price?
-- Illustrates an unbounded group aggregation.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
  auction int64,
  `day` string,
  total_bids int64,
  rank1_bids int64,
  rank2_bids int64,
  rank3_bids int64,
  min_price int64,
  max_price int64,
  avg_price int64,
  sum_price int64
);


SELECT
  auction, to_date(dateTime) AS day, count(*) AS total_bids, count_if(price < 10000) AS rank1_bids, count_if((price >= 10000) AND (price < 1000000)) AS rank2_bids, count_if(price >= 1000000) AS rank3_bids, min(price) AS min_price, max(price) AS max_price, avg(price) AS avg_price, sum(price) AS sum_price
FROM
 bid
GROUP BY
  auction, day


-- -------------------------------------------------------------------------------------------------
-- Proton could explain following statements, but 2 todos:
1. explain count() filter (where price < 10000) to count_if(price < 10000) but not count_if
2. to support count_if(*, price<10000), so far only support count_if(price < 10000)
-- -------------------------------------------------------------------------------------------------

INSERT INTO discard_sink
SELECT
     auction,
     to_date(dateTime) as `day`,
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     min(price) AS min_price,
     max(price) AS max_price,
     avg(price) AS avg_price,
     sum(price) AS sum_price
FROM bid
GROUP BY auction, day;