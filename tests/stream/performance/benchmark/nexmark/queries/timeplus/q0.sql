-- -------------------------------------------------------------------------------------------------
-- Query 0: Pass through (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- This measures the monitoring overhead of the Flink SQL implementation including the source generator.
-- Using `bid` events here, as they are most numerous with default configuration.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
  auction  int64,
  bidder  int64,
  price  int64,
  dateTime  datetime64(3),
  extra  string
);

INSERT INTO discard_sink (auction, bidder, price, dateTime, extra)
SELECT auction, bidder, price, dateTime, extra FROM bid;