--  ddl creare stream
CREATE STREAM IF NOT EXISTS tickers (
    best_ask float64,
    best_ask_size float64,
    best_bid float64,
    best_bid_size float64,
    high_24h float64,
    last_size float64,
    low_24h float64,
    open_24h float64,
    price float64,
    product_id string,
    sequence int,
    side string,
    time datetime,
    trade_id int,
    type string,
    volume_24h float64,
    volume_30d float64
)

-- OHLC query
SELECT
  window_start, product_id, earliest(price) AS open, max(price) AS high, min(price) AS low, latest(price) AS close, latest(volume_24h) as acc_vol
FROM
  tumble(tickers, 60s)
WHERE
  product_id != ''
GROUP BY
  window_start, product_id
SETTINGS
  seek_to = 'earliest'

-- OHLCV query
WITH ohlc AS
  (
    SELECT
      window_start, product_id, earliest(price) AS open, max(price) AS high, min(price) AS low, latest(price) AS close, latest(volume_24h) AS acc_vol
    FROM
      tumble(tickers, 300s)
    WHERE
      product_id != ''
    GROUP BY
      window_start, product_id
    SETTINGS
      seek_to = 'earliest'
  )
SELECT
  window_start, product_id, open, high, low, close, acc_vol, acc_vol - lag(acc_vol) AS vol
FROM
  ohlc
PARTITION BY
  product_id
