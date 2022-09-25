-- -------------------------------------------------------------------------------------------------
-- Query 13: Bounded Side Input Join (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Joins a stream to a bounded side input, modeling basic stream enrichment.
-- -------------------------------------------------------------------------------------------------

-- TODO: use the new "filesystem" connector once FLINK-17397 is done
CREATE stream side_input (
  key int64,
  `value` string
);

CREATE stream discard_sink (
  auction  int64,
  bidder  int64,
  price  int64,
  dateTime  datetime64(3),
  `value`  string
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    B.auction,
    B.bidder,
    B.price,
    B.dateTime,
    S.`value`
FROM (SELECT * FROM bid) B
JOIN side_input AS S
ON mod(B.auction, 10000) = S.key;