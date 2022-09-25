-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  auction  BIGINT,
  num  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT AuctionBids.auction, AuctionBids.num
 FROM (
   SELECT
     B1.auction,
     count(*) AS num,
     HOP_START(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
     HOP_END(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
   FROM bid B1
   GROUP BY
     B1.auction,
     HOP(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.starttime,
     CountBids.endtime
   FROM (
     SELECT
       count(*) AS num,
       HOP_START(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
       HOP_END(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
     FROM bid B2
     GROUP BY
       B2.auction,
       HOP(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
     ) AS CountBids
   GROUP BY CountBids.starttime, CountBids.endtime
 ) AS MaxBids
 ON AuctionBids.starttime = MaxBids.starttime AND
    AuctionBids.endtime = MaxBids.endtime AND
    AuctionBids.num >= MaxBids.maxn;