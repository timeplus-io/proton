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

CREATE stream discard_sink (
    auction  int64,
    bidder  int64,
    price  int64,
    channel  string,
    url  string,
    bid_dateTime  datetime64(3),
    bid_extra  string,

    itemName  string,
    description  string,
    initialBid  int64,
    reserve  int64,
    auction_dateTime  datetime64(3),
    expires  datetime64(3),
    seller  int64,
    category  int64,
    auction_extra  string
);

INSERT INTO discard_sink
SELECT
    auction, bidder, price, channel, url, B.dateTime, B.extra,
    itemName, description, initialBid, reserve, A.dateTime, expires, seller, category, A.extra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10;
