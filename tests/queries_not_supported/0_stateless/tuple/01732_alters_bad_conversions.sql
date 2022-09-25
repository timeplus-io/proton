DROP STREAM IF EXISTS bad_conversions;
DROP STREAM IF EXISTS bad_conversions_2;

create stream bad_conversions (a uint32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO bad_conversions VALUES (1);
ALTER STREAM bad_conversions MODIFY COLUMN a array(string); -- { serverError 53 }
SHOW create stream bad_conversions;
SELECT count() FROM system.mutations WHERE table = 'bad_conversions' AND database = currentDatabase();

create stream bad_conversions_2 (e Enum('foo' = 1, 'bar' = 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO bad_conversions_2 VALUES (1);
ALTER STREAM bad_conversions_2 MODIFY COLUMN e Enum('bar' = 1, 'foo' = 2); -- { serverError 70 }
SHOW create stream bad_conversions_2;
SELECT count() FROM system.mutations WHERE table = 'bad_conversions_2' AND database = currentDatabase();

DROP STREAM IF EXISTS bad_conversions;
DROP STREAM IF EXISTS bad_conversions_2;
