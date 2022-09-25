-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  auction  BIGINT,
  bidder  BIGINT,
  price  DECIMAL(23, 3),
  dateTime  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    dateTime,
    extra
FROM bid;