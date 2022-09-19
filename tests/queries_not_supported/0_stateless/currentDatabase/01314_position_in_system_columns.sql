DROP STREAM IF EXISTS test;
create stream test (x uint8, y string, z array(string)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1);

SELECT name, type, position FROM system.columns WHERE database = currentDatabase() AND table = 'test';
SELECT column, type, column_position FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test';

DROP STREAM test;
