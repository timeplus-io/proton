-- -------------------------------------------------------------------------------------------------
-- Query 15: Bidding Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price?
-- Illustrates multiple distinct aggregations with filters.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  `day` VARCHAR,
  total_bids BIGINT,
  rank1_bids BIGINT,
  rank2_bids BIGINT,
  rank3_bids BIGINT,
  total_bidders BIGINT,
  rank1_bidders BIGINT,
  rank2_bidders BIGINT,
  rank3_bidders BIGINT,
  total_auctions BIGINT,
  rank1_auctions BIGINT,
  rank2_auctions BIGINT,
  rank3_auctions BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
     DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     count(distinct bidder) AS total_bidders,
     count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
     count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
     count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
     count(distinct auction) AS total_auctions,
     count(distinct auction) filter (where price < 10000) AS rank1_auctions,
     count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
     count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
FROM bid
GROUP BY DATE_FORMAT(dateTime, 'yyyy-MM-dd');