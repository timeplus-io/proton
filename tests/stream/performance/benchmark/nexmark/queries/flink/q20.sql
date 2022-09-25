-- -------------------------------------------------------------------------------------------------
-- Query 20: Expand bid with auction (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Get bids with the corresponding auction information where category is 10.
-- Illustrates a filter join.
-- -------------------------------------------------------------------------------------------------

-- TODO: streaming join doesn't support rowtime attribute in input, this should be fixed by FLINK-18651.
--  As a workaround, we re-create a new view without rowtime attribute for now.
DROP VIEW IF EXISTS auction;
DROP VIEW IF EXISTS bid;
CREATE VIEW auction AS SELECT auction.* FROM ${NEXMARK_TABLE} WHERE event_type = 1;
CREATE VIEW bid AS SELECT bid.* FROM ${NEXMARK_TABLE} WHERE event_type = 2;

CREATE TABLE discard_sink (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    bid_dateTime  TIMESTAMP(3),
    bid_extra  VARCHAR,

    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    auction_dateTime  TIMESTAMP(3),
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    auction_extra  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    auction, bidder, price, channel, url, B.dateTime, B.extra,
    itemName, description, initialBid, reserve, A.dateTime, expires, seller, category, A.extra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10;