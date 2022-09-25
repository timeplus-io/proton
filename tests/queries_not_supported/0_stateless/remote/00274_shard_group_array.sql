-- Tags: shard

SELECT length(group_array(number)), count() FROM (SELECT number FROM system.numbers_mt LIMIT 1000000);
SELECT group_array(dummy), count() FROM remote('127.0.0.{2,3}', system.one);

SELECT length(group_array(to_string(number))), count() FROM (SELECT number FROM system.numbers LIMIT 100000);
SELECT group_array(to_string(dummy)), count() FROM remote('127.0.0.{2,3}', system.one);
