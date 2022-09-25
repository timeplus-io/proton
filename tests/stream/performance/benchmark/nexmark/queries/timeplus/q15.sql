-- -------------------------------------------------------------------------------------------------
-- Query 15: Bidding Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price?
-- Illustrates multiple distinct aggregations with filters.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
  `day` string,
  total_bids int64,
  rank1_bids int64,
  rank2_bids int64,
  rank3_bids int64,
  total_bidders int64,
  rank1_bidders int64,
  rank2_bidders int64,
  rank3_bidders int64,
  total_auctions int64,
  rank1_auctions int64,
  rank2_auctions int64,
  rank3_auctions int64
);


INSERT INTO discard_sink
SELECT
  to_date(dateTime) AS day, count() AS total_bids, count_if(price < 10000) AS rank1_bids, count_if((price >= 10000) AND (price < 1000000)) AS rank2_bids, count_if(price >= 1000000) AS rank3_bids, count_distinct(bidder) AS total_bidders, count_distinct_if(bidder, price < 10000) AS rank1_bidders, count_distinct_if(bidder, (price >= 10000) AND (price < 1000000)) AS rank2_bidders, count_distinct_if(bidder, price >= 1000000) AS rank3_bidders, count_distinct(auction) AS total_auctions, count_distinct_if(auction, price < 10000) AS rank1_auctions, count_distinct_if(auction, (price >= 10000) AND (price < 1000000)) AS rank2_auctions, count_distinct_if(auction, price >= 1000000) AS rank3_auctions
FROM
 bid
GROUP BY
  day


-- -------------------------------------------------------------------------------------------------
-- Proton could explain following statements, but 2 todos:
1. explain count() filter (where price < 10000) to count_if(price < 10000) but not count_if
2. to support count_if(*, price<10000), so far only support count_if(price < 10000)
-- -------------------------------------------------------------------------------------------------


INSERT INTO discard_sink
SELECT
     to_date(dateTime) as `day`,
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
GROUP BY day;
