-- -------------------------------------------------------------------------------------------------
-- Query 9: Winning Bids (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find the winning bid for each auction.
-- -------------------------------------------------------------------------------------------------

-- TODO: streaming join doesn't support rowtime attribute in input, this should be fixed by FLINK-18651.
--  As a workaround, we re-create a new view without rowtime attribute for now.
DROP VIEW IF EXISTS auction;
DROP VIEW IF EXISTS bid;
CREATE VIEW auction AS SELECT auction.* FROM ${NEXMARK_TABLE} WHERE event_type = 1;
CREATE VIEW bid AS SELECT bid.* FROM ${NEXMARK_TABLE} WHERE event_type = 2;

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