-- -------------------------------------------------------------------------------------------------
-- Query 8: Monitor New Users
-- -------------------------------------------------------------------------------------------------
-- Select people who have entered the system and created auctions in the last period.
-- Illustrates a simple join.
--
-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.
-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).
-- -------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS auction_v;
DROP VIEW IF EXISTS bid_v;
DROP VIEW IF EXISTS person_v;
CREATE VIEW auction_v AS select to_int64(auction_a:id) as id, to_int64(auction_a:initialBid) as initialBid, auction_a:itemName as itemName, to_int64(auction_a:reserve) as reserve, to_int64(auction_a:seller) as seller, to_int64(auction_a:category) as category, to_datetime64(auction_a:dateTime, 3) as dateTime, auction_a:description as description, to_datetime64(auction_a:expires, 3) as expires from (select json_extract_string(coalesce(auction, '')) as auction_a from nexmark where event_type=1 settings seek_to='earliest');
CREATE VIEW bid_v AS select to_int64(bid_a:auction) as auction, to_int64(bid_a:bidder) as bidder, bid_a:channel as channel, to_int64(bid_a:price) as price, bid_a:url as url, to_datetime64(bid_a:dateTime, 3) as dateTime, bid_a:extra as extra from (select json_extract_string(coalesce(bid, '')) as bid_a from nexmark where event_type = 2 settings seek_to='earliest');
CREATE VIEW person_v as select to_int64(person_a:id) as id, person_a:name as name, person_a:state as state, person_a:city as city, person_a:creditCard as creditCard, to_datetime64(person_a:dateTime,3) as dateTime, person_a:emailAddress as emailAddress, person_a:extra as extra from (select json_extract_string(coalesce(person, '')) as person_a from nexmark where event_type = 0 settings seek_to = 'earliest') 


CREATE stream discard_sink (
  id  int64,
  name  string,
  stime  datetime64(3)
);

INSERT INTO discard_sink
SELECT P.id, P.name, P.window_start
FROM (
  SELECT id, name, window_start, window_end
  FROM tumble(person_v, dateTime, 10s)
  GROUP BY id, name, window_start, window_end
) P
JOIN (
  SELECT seller, window_start, window_end
  FROM tumble(auction_v, dateTime, 10s)
  GROUP BY seller, window_start, window_end
) A
ON P.id = A.seller AND P.window_start = A.window_start AND P.window_end = A.window_end settings seek_to='earliest';