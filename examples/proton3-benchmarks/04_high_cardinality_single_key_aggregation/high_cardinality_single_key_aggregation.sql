-- High-cardinality analytics (single-key aggregations) micro-benchmark.

SELECT
  dim.symbol,
  t2.instrument_key AS has_trades_flag
FROM
(
  -- Tiny dimension: map numeric key → symbol
  SELECT
    1         AS instrument_key,
    'BTC-USD' AS symbol
) AS dim
LEFT JOIN
(
  -- Huge fact side: 10B synthetic rows collapsed into a single key
  -- (micro-benchmark for the single-key GROUP BY reduce path)
  SELECT
    1 AS instrument_key
  FROM numbers_mt(10000000000)
  GROUP BY instrument_key
) AS t2
USING (instrument_key);

