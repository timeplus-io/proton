-- Tags: shard, bug, #1304

SELECT to_type_name(1.0) FROM remote('127.0.0.{2,3}', system, one)
