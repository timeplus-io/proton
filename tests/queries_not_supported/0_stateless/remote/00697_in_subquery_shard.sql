-- Tags: shard

SELECT 0 IN 0;
SELECT 0 IN 1;
SELECT 0 IN (SELECT 0);
SELECT 0 IN (SELECT 1);

SELECT dummy IN (SELECT 0) FROM remote('127.0.0.1', system.one);
SELECT dummy IN (SELECT 1) FROM remote('127.0.0.1', system.one);

SELECT dummy IN (SELECT 0) FROM remote('127.0.0.{1,2}', system.one);
SELECT dummy IN (SELECT 1) FROM remote('127.0.0.{1,2}', system.one);

SELECT number IN (SELECT to_uint64(array_join([1, 8]))) FROM remote('127.0.0.{1,2}', numbers(10));

SELECT array_exists(x -> (x IN (SELECT 1)), [1]) FROM remote('127.0.0.{1,2}', system.one);
SELECT sum_if(number, array_exists(x -> (x IN (SELECT 1)), [1])) FROM remote('127.0.0.{1,2}', numbers(10));

SET prefer_localhost_replica = 0;

SELECT dummy IN (SELECT 0) FROM remote('127.0.0.{1,2}', system.one);
SELECT dummy IN (SELECT 1) FROM remote('127.0.0.{1,2}', system.one);

SELECT number IN (SELECT to_uint64(array_join([1, 8]))) FROM remote('127.0.0.{1,2}', numbers(10));

SELECT array_exists(x -> (x IN (SELECT 1)), [1]) FROM remote('127.0.0.{1,2}', system.one);
SELECT sum_if(number, array_exists(x -> (x IN (SELECT 1)), [1])) FROM remote('127.0.0.{1,2}', numbers(10));
