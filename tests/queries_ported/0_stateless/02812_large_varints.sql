-- 64-bit integers with MSB set (i.e. values > (1ULL<<63) - 1) could for historical/compat reasons not be serialized as var-ints (issue #51486).
-- These two queries internally produce such big values, run them to be sure no bad things happen.

-- SELECT top_k(65535, now() -2) FORMAT Null; (topk need revisit? clickhouse works well)
-- https://fiddle.clickhouse.com/760fc2b2-f1d3-42e2-8856-b2b835f97589

SELECT number FROM numbers(to_uint64(-1)) limit 10 Format Null;
