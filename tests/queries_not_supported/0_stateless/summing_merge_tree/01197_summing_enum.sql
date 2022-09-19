-- Tags: not_supported, blocked_by_SummingMergeTree

DROP STREAM IF EXISTS summing;

create stream summing (k string, x uint64, e Enum('hello' = 1, 'world' = 2)) ENGINE = SummingMergeTree ORDER BY k;
INSERT INTO summing SELECT '', 1, e FROM generateRandom('e Enum(\'hello\' = 1, \'world\' = 2)', 1) LIMIT 1000;
INSERT INTO summing SELECT '', 1, e FROM generateRandom('e Enum(\'hello\' = 1, \'world\' = 2)', 1) LIMIT 1000;

OPTIMIZE STREAM summing;
SELECT k, x, e FROM summing;

DROP STREAM summing;