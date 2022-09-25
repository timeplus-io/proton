-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
-- Deliberately implemented using a side input to illustrate fanout.
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
  auction  int64,
  bidder  int64,
  price  int64,
  dateTime  datetime64(3),
  extra  string
);

INSERT INTO discard_sink
SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
from bid as B
JOIN (
  SELECT MAX(price) AS maxprice, window_start, window_end from tumble(bid, dateTime, 10s)
  GROUP BY window_start, window_end
) as B1
ON B.price = B1.maxprice
WHERE B.dateTime BETWEEN B1.window_start AND B1.window_end settings seek_to='earliest'
