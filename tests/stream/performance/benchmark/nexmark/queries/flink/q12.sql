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
  bidder BIGINT,
  bid_count BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    B.bidder,
    count(*) as bid_count,
    TUMBLE_START(B.p_time, INTERVAL '10' SECOND) as starttime,
    TUMBLE_END(B.p_time, INTERVAL '10' SECOND) as endtime
FROM (SELECT *, PROCTIME() as p_time FROM bid) B
GROUP BY B.bidder, TUMBLE(B.p_time, INTERVAL '10' SECOND);