-- Tags: distributed, bug, #1304

SELECT NOT dummy FROM remote('127.0.0.{2,3}', system, one) WHERE NOT dummy
