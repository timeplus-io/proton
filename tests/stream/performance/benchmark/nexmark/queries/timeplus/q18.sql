-- -------------------------------------------------------------------------------------------------
-- Query 18: Find last bid (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's a's last bid for bidder to auction?
-- Illustrates a Deduplicate query.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
    auction  int64,
    bidder  int64,
    price  int64,
    channel  string,
    url  string,
    dateTime  datetime64(3),
    extra  string
);

INSERT INTO discard_sink
SELECT auction, bidder, price, channel, url, dateTime, extra
 FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY dateTime DESC) AS rank_number
       FROM bid)
 WHERE rank_number <= 1;