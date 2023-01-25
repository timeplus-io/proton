-- Tags: shard

SELECT count() FROM remote('127.0.0.1', system, one) WHERE array_exists((x) -> x = 1, [1, 2, 3])
