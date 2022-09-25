-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------



blocked by https://github.com/timeplus-io/proton/issues/1066


DROP VIEW IF EXISTS auction_v;
DROP VIEW IF EXISTS bid_v;
DROP VIEW IF EXISTS person_v;
CREATE VIEW auction_v AS select to_int64(auction_a:id) as id, to_int64(auction_a:initialBid) as initialBid, auction_a:itemName as itemName, to_int64(auction_a:reserve) as reserve, to_int64(auction_a:seller) as seller, to_int64(auction_a:category) as category, to_datetime64(auction_a:dateTime, 3) as dateTime, auction_a:description as description, to_datetime64(auction_a:expires, 3) as expires from (select json_extract_string(coalesce(auction, '')) as auction_a from nexmark where event_type=1 settings seek_to='earliest');
CREATE VIEW bid_v AS select to_int64(bid_a:auction) as auction, to_int64(bid_a:bidder) as bidder, bid_a:channel as channel, to_int64(bid_a:price) as price, bid_a:url as url, to_datetime64(bid_a:dateTime, 3) as dateTime, bid_a:extra as extra from (select json_extract_string(coalesce(bid, '')) as bid_a from nexmark where event_type = 2 settings seek_to='earliest');
CREATE VIEW person_v as select to_int64(person_a:id) as id, person_a:name as name, person_a:state as state, person_a:city as city, person_a:creditCard as creditCard, to_datetime64(person_a:dateTime,3) as dateTime, person_a:emailAddress as emailAddress, person_a:extra as extra from (select json_extract_string(coalesce(person, '')) as person_a from nexmark where event_type = 0 settings seek_to = 'earliest') 


CREATE stream discard_sink (
  auction  int64,
  num  int64
);

INSERT INTO discard_sink
SELECT AuctionBids.auction, AuctionBids.num
 FROM (
   SELECT
     auction,
     count(*) AS num, window_start, window_end
   FROM hop(bid_v, dateTime, 2s, 10s)
   GROUP BY
     auction,
     window_start, window_end
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.window_start,
     CountBids.window_end
   FROM (
     SELECT count(*) AS num, window_start, window_end FROM hop(bid_v, dateTime, 2s, 10s) GROUP BY auction, window_start, window_end) AS CountBids
   GROUP BY CountBids.window_start, CountBids.window_end
 ) AS MaxBids
 ON AuctionBids.window_start = MaxBids.window_start AND
    AuctionBids.window_end = MaxBids.window_end 
 WHERE
    AuctionBids.num >= MaxBids.maxn;    