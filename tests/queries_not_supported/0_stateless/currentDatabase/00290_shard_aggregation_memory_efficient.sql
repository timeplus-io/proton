-- Tags: shard

DROP STREAM IF EXISTS numbers_10_00290;
SET max_block_size = 1000;
create stream numbers_10_00290   AS SELECT * FROM system.numbers LIMIT 10000;
SET distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 5000;

SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(to_string(number), array_string_concat(array_map(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;

DROP STREAM numbers_10_00290;
