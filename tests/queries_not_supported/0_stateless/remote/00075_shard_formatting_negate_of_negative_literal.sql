-- Tags: shard, bug, #1304

SELECT -(-1) FROM remote('127.0.0.{2,3}', system, one)
