-- -------------------------------------------------------------------------------------------------
-- Query 13: Bounded Side Input Join (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Joins a stream to a bounded side input, modeling basic stream enrichment.
-- -------------------------------------------------------------------------------------------------

-- TODO: use the new "filesystem" connector once FLINK-17397 is done
CREATE TABLE side_input (
  key BIGINT,
  `value` VARCHAR
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'file://${FLINK_HOME}/data/side_input.txt',
  'format.type' = 'csv'
);

CREATE TABLE discard_sink (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  dateTime  TIMESTAMP(3),
  `value`  VARCHAR
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
FROM (SELECT *, PROCTIME() as p_time FROM bid) B
JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
ON mod(B.auction, 10000) = S.key;