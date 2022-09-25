-- -------------------------------------------------------------------------------------------------
-- Query 21: Add channel id (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Add a channel_id column to the bid table.
-- Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL.
-- -------------------------------------------------------------------------------------------------

CREATE stream discard_sink (
    auction  int64,
    bidder  int64,
    price  int64,
    channel  string,
    channel_id  string
);

INSERT INTO discard_sink
SELECT
    auction, bidder, price, channel,
    CASE
        WHEN lower(channel) = 'apple' THEN '0'
        WHEN lower(channel) = 'google' THEN '1'
        WHEN lower(channel) = 'facebook' THEN '2'
        WHEN lower(channel) = 'baidu' THEN '3'
        ELSE extract(url, '.*channel_id=([^&]*)')
        END
    AS channel_id FROM bid
    where extract(url, '.*channel_id=([^&]*)') is not null or
          lower(channel) in ('apple', 'google', 'facebook', 'baidu');