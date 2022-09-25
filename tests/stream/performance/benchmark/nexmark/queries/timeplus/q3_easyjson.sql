-- -------------------------------------------------------------------------------------------------
-- Query 3: Local Item Suggestion
-- -------------------------------------------------------------------------------------------------
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?
-- Illustrates an incremental join (using per-key state and timer) and filter.
-- -------------------------------------------------------------------------------------------------

DROP VIEW IF EXISTS auction_v;
DROP VIEW IF EXISTS bid_v;
DROP VIEW IF EXISTS person_v;
CREATE VIEW auction_v AS select to_int64(auction_a:id) as id, to_int64(auction_a:initialBid) as initialBid, auction_a:itemName as itemName, to_int64(auction_a:reserve) as reserve, to_int64(auction_a:seller) as seller, to_int64(auction_a:category) as category, to_datetime64(auction_a:dateTime, 3) as dateTime, auction_a:description as description, to_datetime64(auction_a:expires, 3) as expires from (select json_extract_string(coalesce(auction, '')) as auction_a from nexmark where event_type=1 settings seek_to='earliest');
CREATE VIEW bid_v AS select to_int64(bid_a:auction) as auction, to_int64(bid_a:bidder) as bidder, bid_a:channel as channel, to_int64(bid_a:price) as price, bid_a:url as url, to_datetime64(bid_a:dateTime, 3) as dateTime, bid_a:extra as extra from (select json_extract_string(coalesce(bid, '')) as bid_a from nexmark where event_type = 2 settings seek_to='earliest');
CREATE VIEW person_v as select to_int64(person_a:id) as id, person_a:name as name, person_a:state as state, person_a:city as city, person_a:creditCard as creditCard, to_datetime64(person_a:dateTime,3) as dateTime, person_a:emailAddress as emailAddress, person_a:extra as extra from (select json_extract_string(coalesce(person, '')) as person_a from nexmark where event_type = 0 settings seek_to = 'earliest') 




CREATE TABLE discard_sink (
  name  string,
  city  string,
  state  string,
  id  int64
);

INSERT INTO discard_sink (name, city, state, id)
SELECT
    P.name, P.city, P.state, A.id
FROM
    auction_v AS A INNER JOIN person_v AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');



