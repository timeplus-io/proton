    -- -------------------------------------------------------------------------------------------------
    -- Query 21: Add channel id (Not in original suite)
    -- -------------------------------------------------------------------------------------------------
    -- Add a channel_id column to the bid table.
    -- Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL.
    -- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    channel_id  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO discard_sink
SELECT
    auction, bidder, price, channel,
    CASE
        WHEN lower(channel) = 'apple' THEN '0'
        WHEN lower(channel) = 'google' THEN '1'
        WHEN lower(channel) = 'facebook' THEN '2'
        WHEN lower(channel) = 'baidu' THEN '3'
        ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)
        END
    AS channel_id FROM bid
    where REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) is not null or
          lower(channel) in ('apple', 'google', 'facebook', 'baidu');