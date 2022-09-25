-- -------------------------------------------------------------------------------------------------
-- Query 19: Auction TOP-10 Price (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's the top price 10 bids of an auction?
-- Illustrates a TOP-N query.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    dateTime  TIMESTAMP(3),
    extra  VARCHAR,
    rank_number  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT * FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
WHERE rank_number <= 10;