-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
  auction  int64,
  bidder  int64,
  price  decimal(23, 3),
  dateTime  decimal64(3),
  extra  string
);

INSERT INTO discard_sink (auction,bidder,price,dateTime, extra)
SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    dateTime,
    extra
FROM bid;