-- -------------------------------------------------------------------------------------------------
-- Query 11: User Sessions (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids did a user make in each session they were active? Illustrates session windows.
--
-- Group bids by the same user into sessions with max session gap.
-- Emit the number of bids per session.
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
    SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);