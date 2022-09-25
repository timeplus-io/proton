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

blocked by https://github.com/timeplus-io/proton/issues/1066 and https://github.com/timeplus-io/proton/issues/1090

CREATE stream discard_sink (
  auction  int64,
  num  int64
);

INSERT INTO discard_sink
SELECT AuctionBids.auction, AuctionBids.num
 FROM (
   SELECT
     auction,
     count(*) AS num, window_start, window_end
   FROM hop(bid, dateTime, 2s, 10s)
   GROUP BY
     auction,
     window_start, window_end settings seek_to='earliest'
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.window_start,
     CountBids.window_end
   FROM (
     SELECT count(*) AS num, window_start, window_end FROM hop(bid, dateTime, 2s, 10s) GROUP BY auction, window_start, window_end settings seek_to='earliest') AS CountBids
   GROUP BY CountBids.window_start, CountBids.window_end
 ) AS MaxBids
 ON AuctionBids.window_start = MaxBids.window_start AND
    AuctionBids.window_end = MaxBids.window_end
 WHERE
    AuctionBids.num >= MaxBids.maxn;



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