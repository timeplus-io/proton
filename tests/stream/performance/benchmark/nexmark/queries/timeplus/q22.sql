-- -------------------------------------------------------------------------------------------------
-- Query 22: Get URL Directories (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What is the directory structure of the URL?
-- Illustrates a SPLIT_INDEX SQL.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
      auction  int64,
      bidder  int64,
      price  int64,
      channel  string,
      dir1  string,
      dir2  string,
      dir3  string
);


INSERT INTO discard_sink
SELECT
    auction, bidder, price, channel,
    split_by_string(url, '/') as pars,  as dir1,
    SPLIT_INDEX(url, '/', 4) as dir2,
    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid;



