-- -------------------------------------------------------------------------------------------------
-- Query 9: Winning Bids (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find the winning bid for each auction.
-- -------------------------------------------------------------------------------------------------

-- TODO: streaming join doesn't support rowtime attribute in input, this should be fixed by FLINK-18651.
--  As a workaround, we re-create a new view without rowtime attribute for now.
DROP VIEW IF EXISTS auction;
DROP VIEW IF EXISTS auction_v;
DROP VIEW IF EXISTS bid_v;
DROP VIEW IF EXISTS person_v;
CREATE VIEW auction_v AS select to_int64(auction_a:id) as id, to_int64(auction_a:initialBid) as initialBid, auction_a:itemName as itemName, to_int64(auction_a:reserve) as reserve, to_int64(auction_a:seller) as seller, to_int64(auction_a:category) as category, to_datetime64(auction_a:dateTime, 3) as dateTime, auction_a:description as description, to_datetime64(auction_a:expires, 3) as expires from (select json_extract_string(coalesce(auction, '')) as auction_a from nexmark where event_type=1 settings seek_to='earliest');
CREATE VIEW bid_v AS select to_int64(bid_a:auction) as auction, to_int64(bid_a:bidder) as bidder, bid_a:channel as channel, to_int64(bid_a:price) as price, bid_a:url as url, to_datetime64(bid_a:dateTime, 3) as dateTime, bid_a:extra as extra from (select json_extract_string(coalesce(bid, '')) as bid_a from nexmark where event_type = 2 settings seek_to='earliest');
CREATE VIEW person_v as select to_int64(person_a:id) as id, person_a:name as name, person_a:state as state, person_a:city as city, person_a:creditCard as creditCard, to_datetime64(person_a:dateTime,3) as dateTime, person_a:emailAddress as emailAddress, person_a:extra as extra from (select json_extract_string(coalesce(person, '')) as person_a from nexmark where event_type = 0 settings seek_to = 'earliest') 


CREATE TABLE discard_sink (
  id  BIGINT,
  itemName  VARCHAR,
  description  VARCHAR,
  initialBid  BIGINT,
  reserve  BIGINT,
  dateTime  TIMESTAMP(3),
  expires  TIMESTAMP(3),
  seller  BIGINT,
  category  BIGINT,
  extra  VARCHAR,
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  bid_dateTime  TIMESTAMP(3),
  bid_extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra,
    auction, bidder, price, bid_dateTime, bid_extra
FROM (
   SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra,
     ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
   FROM auction A, bid B
   WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
)
WHERE rownum <= 1;