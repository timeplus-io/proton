-- -------------------------------------------------------------------------------------------------
-- Query 12: Processing Time Windows (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids does a user make within a fixed processing time limit?
-- Illustrates working in processing time window.
--
-- Group bids by the same user into processing time windows of 10 seconds.
-- Emit the count of bids per window.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  bidder int64,
  bid_count int64,
  starttime datetime64(3),
  endtime datetime64(3)
);

INSERT INTO discard_sink
SELECT
    bidder,
    count(*) as bid_count,
    window_start,
    window_end
FROM tumble(bid, 10s)
GROUP BY bidder, window_start, window_end;


INSERT INTO discard_sink
SELECT
    B.bidder,
    count(*) as bid_count,
    TUMBLE_START(B.p_time, INTERVAL '10' SECOND) as starttime,
    TUMBLE_END(B.p_time, INTERVAL '10' SECOND) as endtime
FROM (SELECT *, PROCTIME() as p_time FROM bid) B
GROUP BY B.bidder, TUMBLE(B.p_time, INTERVAL '10' SECOND);