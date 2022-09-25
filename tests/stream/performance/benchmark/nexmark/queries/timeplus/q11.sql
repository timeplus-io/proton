-- -------------------------------------------------------------------------------------------------
-- Query 11: User Sessions (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids did a user make in each session they were active? Illustrates session windows.
--
-- Group bids by the same user into sessions with max session gap.
-- Emit the number of bids per session.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
  bidder int64,
  bid_count int64,
  starttime datetime64(3),
  endtime datetime64(3)
);

INSERT INTO discard_sink
SELECT
    B.bidder,
    count(*) as bid_count,
    SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);