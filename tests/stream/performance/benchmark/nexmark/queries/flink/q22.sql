-- -------------------------------------------------------------------------------------------------
-- Query 22: Get URL Directories (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What is the directory structure of the URL?
-- Illustrates a SPLIT_INDEX SQL.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
      auction  BIGINT,
      bidder  BIGINT,
      price  BIGINT,
      channel  VARCHAR,
      dir1  VARCHAR,
      dir2  VARCHAR,
      dir3  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    auction, bidder, price, channel,
    SPLIT_INDEX(url, '/', 3) as dir1,
    SPLIT_INDEX(url, '/', 4) as dir2,
    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid;
