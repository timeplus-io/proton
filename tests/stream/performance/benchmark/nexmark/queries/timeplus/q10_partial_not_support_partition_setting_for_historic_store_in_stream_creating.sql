-- -------------------------------------------------------------------------------------------------
-- Query 10: Log to File System (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Log all events to file system. Illustrates windows streaming data into partitioned file system.
--
-- Every minute, save all events from the last period into partitioned log files.
-- -------------------------------------------------------------------------------------------------

CREATE stream fs_sink (
  auction  int64,
  bidder  int64,
  price  int64,
  dateTime  datetime64(3),
  extra  string,
  dt string,
  hm string
) PARTITIONED BY (dt, hm) WITH (
  'connector' = 'filesystem',
  'path' = 'file://${NEXMARK_DIR}/data/output/${SUBMIT_TIME}/bid/',
  'format' = 'csv',
  'sink.partition-commit.trigger' = 'partition-time',
  'sink.partition-commit.delay' = '1 min',
  'sink.partition-commit.policy.kind' = 'success-file',
  'partition.time-extractor.timestamp-pattern' = '$dt $hm:00',
  'sink.rolling-policy.rollover-interval' = '1min',
  'sink.rolling-policy.check-interval' = '1min'
);

INSERT INTO fs_sink
SELECT auction, bidder, price, dateTime, extra, DATE_FORMAT(dateTime, 'yyyy-MM-dd'), DATE_FORMAT(dateTime, 'HH:mm')
FROM bid;