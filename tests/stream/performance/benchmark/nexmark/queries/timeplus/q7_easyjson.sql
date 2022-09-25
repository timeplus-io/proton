-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
-- Deliberately implemented using a side input to illustrate fanout.
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- -------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS auction_v;
DROP VIEW IF EXISTS bid_v;
DROP VIEW IF EXISTS person_v;
CREATE VIEW auction_v AS select to_int64(auction_a:id) as id, to_int64(auction_a:initialBid) as initialBid, auction_a:itemName as itemName, to_int64(auction_a:reserve) as reserve, to_int64(auction_a:seller) as seller, to_int64(auction_a:category) as category, to_datetime64(auction_a:dateTime, 3) as dateTime, auction_a:description as description, to_datetime64(auction_a:expires, 3) as expires from (select json_extract_string(coalesce(auction, '')) as auction_a from nexmark where event_type=1 settings seek_to='earliest');
CREATE VIEW bid_v AS select to_int64(bid_a:auction) as auction, to_int64(bid_a:bidder) as bidder, bid_a:channel as channel, to_int64(bid_a:price) as price, bid_a:url as url, to_datetime64(bid_a:dateTime, 3) as dateTime, bid_a:extra as extra from (select json_extract_string(coalesce(bid, '')) as bid_a from nexmark where event_type = 2 settings seek_to='earliest');
CREATE VIEW person_v as select to_int64(person_a:id) as id, person_a:name as name, person_a:state as state, person_a:city as city, person_a:creditCard as creditCard, to_datetime64(person_a:dateTime,3) as dateTime, person_a:emailAddress as emailAddress, person_a:extra as extra from (select json_extract_string(coalesce(person, '')) as person_a from nexmark where event_type = 0 settings seek_to = 'earliest') 


CREATE stream discard_sink (
  auction  int64,
  bidder  int64,
  price  int64,
  dateTime  datetime64(3),
  extra  string
);

INSERT INTO discard_sink
SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
from bid_v as B
JOIN (
  SELECT MAX(price) AS maxprice, window_start, window_end from tumble(bid_v, dateTime, 10s)
  GROUP BY window_start, window_end
) as B1
ON B.price = B1.maxprice
WHERE B.dateTime BETWEEN B1.window_start AND B1.window_end settings seek_to='earliest'