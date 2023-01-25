-- Tags: shard

SELECT (dummy AS x) - 1 FROM remote('127.0.0.1', system, one)
