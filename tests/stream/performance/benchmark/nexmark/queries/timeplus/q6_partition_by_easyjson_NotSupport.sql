-- -------------------------------------------------------------------------------------------------
-- Query 6: Average Selling Price by Seller
-- -------------------------------------------------------------------------------------------------
-- What is the average selling price per seller for their last 10 closed auctions.
-- Shares the same ‘winning bids’ core as for Query4, and illustrates a specialized combiner.
-- -------------------------------------------------------------------------------------------------


DROP VIEW IF EXISTS auction_v;
DROP VIEW IF EXISTS bid_v;
DROP VIEW IF EXISTS person_v;
CREATE VIEW auction_v AS select to_int64(auction_a:id) as id, to_int64(auction_a:initialBid) as initialBid, auction_a:itemName as itemName, to_int64(auction_a:reserve) as reserve, to_int64(auction_a:seller) as seller, to_int64(auction_a:category) as category, to_datetime64(auction_a:dateTime, 3) as dateTime, auction_a:description as description, to_datetime64(auction_a:expires, 3) as expires from (select json_extract_string(coalesce(auction, '')) as auction_a from nexmark where event_type=1 settings seek_to='earliest');
CREATE VIEW bid_v AS select to_int64(bid_a:auction) as auction, to_int64(bid_a:bidder) as bidder, bid_a:channel as channel, to_int64(bid_a:price) as price, bid_a:url as url, to_datetime64(bid_a:dateTime, 3) as dateTime, bid_a:extra as extra from (select json_extract_string(coalesce(bid, '')) as bid_a from nexmark where event_type = 2 settings seek_to='earliest');
CREATE VIEW person_v as select to_int64(person_a:id) as id, person_a:name as name, person_a:state as state, person_a:city as city, person_a:creditCard as creditCard, to_datetime64(person_a:dateTime,3) as dateTime, person_a:emailAddress as emailAddress, person_a:extra as extra from (select json_extract_string(coalesce(person, '')) as person_a from nexmark where event_type = 0 settings seek_to = 'earliest') 


CREATE stream discard_sink (
  seller string,
  avg_price  BIGINT
);

-- TODO: this query is not supported yet in Flink SQL, because the OVER WINDOW operator doesn't
--  support to consume retractions.
INSERT INTO discard_sink
SELECT
    Q.seller,
    AVG(Q.final) OVER
        (PARTITION BY Q.seller ORDER BY Q.dateTime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM (
    SELECT MAX(B.price) AS final, A.seller, B.dateTime
    FROM auction_v as A join bid_v as B
    on A.id = B.auction where B.dateTime between A.dateTime and A.expires
    GROUP BY A.id, A.seller, B.dateTime settings seek_to='earliest'
) AS Q;