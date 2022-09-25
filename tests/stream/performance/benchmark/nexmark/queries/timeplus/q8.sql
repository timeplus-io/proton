-- -------------------------------------------------------------------------------------------------
-- Query 8: Monitor New Users
-- -------------------------------------------------------------------------------------------------
-- Select people who have entered the system and created auctions in the last period.
-- Illustrates a simple join.
--
-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.
-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
  id  int64,
  name  string,
  stime  datetime64(3)
);

INSERT INTO discard_sink
SELECT P.id, P.name, P.window_start
FROM (
  SELECT id, name, window_start, window_end
  FROM tumble(person, dateTime, 10s)
  GROUP BY id, name, window_start, window_end
) P
JOIN (
  SELECT seller, window_start, window_end
  FROM tumble(auction, dateTime, 10s)
  GROUP BY seller, window_start, window_end
) A
ON P.id = A.seller AND P.window_start = A.window_start AND P.window_end = A.window_end settings seek_to='earliest';

-- -------------------------------------------------------------------------------------------------
-- Extended playing ground: get the new uers and what auctions they created
-- -------------------------------------------------------------------------------------------------
SELECT P.id, P.name, A.id, A.itemName, A.category, P.window_start, P.window_end
FROM (
  SELECT id, name, window_start, window_end
  FROM tumble(person, dateTime, 10s)
  GROUP BY id, name, window_start, window_end
) P
JOIN (
  SELECT seller, id, itemName, category, window_start, window_end
  FROM tumble(auction, dateTime, 10s)
  GROUP BY seller, id, itemName, category, window_start, window_end
) A
ON P.id = A.seller AND P.window_start = A.window_start AND P.window_end = A.window_end settings seek_to = 'earliest'