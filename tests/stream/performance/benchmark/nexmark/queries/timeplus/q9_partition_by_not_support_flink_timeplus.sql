-- -------------------------------------------------------------------------------------------------
-- Query 9: Winning Bids (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find the winning bid for each auction.
-- -------------------------------------------------------------------------------------------------

-- TODO: streaming join doesn't support rowtime attribute in input, this should be fixed by FLINK-18651.
--  As a workaround, we re-create a new view without rowtime attribute for now.


CREATE stream discard_sink (
  id  int64,
  itemName  string,
  description  string,
  initialBid  int64,
  reserve  int64,
  dateTime  datetime64(3),
  expires  datetime64(3),
  seller  int64,
  category  int64,
  extra  string,
  auction  int64,
  bidder  int64,
  price  int64,
  bid_dateTime  datetime64(3),
  bid_extra  string
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

