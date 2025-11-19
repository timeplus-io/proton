-- High-frequency/throughput ingestion benchmark.

CREATE STREAM perf_btc_quotes
(
  `ts_bucket`     datetime64,
  `instrument_id` string,
  `venue_id`      string,
  `price`         float64,
  `size`          float64
)
ORDER BY (instrument_id, ts_bucket, venue_id);

INSERT INTO perf_btc_quotes (ts_bucket, instrument_id, venue_id, price, size)
SELECT
  to_datetime64('2024-01-01 00:00:00', 3) + to_int64(number % 86400)                         AS ts_bucket,
  ['BTC-USD', 'ETH-USD', 'NVDA'][number % 3]                                                 AS instrument_id,
  ['binance', 'coinbase', 'okx', 'bybit', 'kraken'][number % 5]                              AS venue_id,
  (30000 + ((number % 1000) / 10.0)) + ((number % 5) * 5.0)                                  AS price,
  0.01 + ((number % 1000) / 10000.0)                                                         AS size
FROM numbers_mt(100000000);

