-- -------------------------------------------------------------------------------------------------
-- Query 8: Monitor New Users
-- -------------------------------------------------------------------------------------------------
-- Select people who have entered the system and created auctions in the last period.
-- Illustrates a simple join.
--
-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.
-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
  id  BIGINT,
  name  VARCHAR,
  stime  TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT P.id, P.name, P.starttime
FROM (
  SELECT P.id, P.name,
         TUMBLE_START(P.dateTime, INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(P.dateTime, INTERVAL '10' SECOND) AS endtime
  FROM person P
  GROUP BY P.id, P.name, TUMBLE(P.dateTime, INTERVAL '10' SECOND)
) P
JOIN (
  SELECT A.seller,
         TUMBLE_START(A.dateTime, INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(A.dateTime, INTERVAL '10' SECOND) AS endtime
  FROM auction A
  GROUP BY A.seller, TUMBLE(A.dateTime, INTERVAL '10' SECOND)
) A
ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;