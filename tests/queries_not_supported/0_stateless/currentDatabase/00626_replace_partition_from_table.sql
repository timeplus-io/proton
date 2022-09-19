-- Tags: no-parallel

DROP STREAM IF EXISTS src;
DROP STREAM IF EXISTS dst;

create stream src (p uint64, k string, d uint64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
create stream dst (p uint64, k string, d uint64) ENGINE = MergeTree PARTITION BY p ORDER BY k;

SELECT 'Initial';
INSERT INTO src VALUES (0, '0', 1);
INSERT INTO src VALUES (1, '0', 1);
INSERT INTO src VALUES (1, '1', 1);
INSERT INTO src VALUES (2, '0', 1);

INSERT INTO dst VALUES (0, '1', 2);
INSERT INTO dst VALUES (1, '1', 2), (1, '2', 2);
INSERT INTO dst VALUES (2, '1', 2);

SELECT count(), sum(d) FROM src;
SELECT count(), sum(d) FROM dst;


SELECT 'REPLACE simple';
ALTER STREAM dst REPLACE PARTITION 1 FROM src;
ALTER STREAM src DROP PARTITION 1;
SELECT count(), sum(d) FROM src;
SELECT count(), sum(d) FROM dst;


SELECT 'REPLACE empty';
ALTER STREAM src DROP PARTITION 1;
ALTER STREAM dst REPLACE PARTITION 1 FROM src;
SELECT count(), sum(d) FROM dst;


SELECT 'REPLACE recursive';
ALTER STREAM dst DROP PARTITION 1;
INSERT INTO dst VALUES (1, '1', 2), (1, '2', 2);

CREATE TEMPORARY table test_block_numbers (m uint64);
INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database=currentDatabase() AND  table='dst' AND active AND name LIKE '1_%';

ALTER STREAM dst REPLACE PARTITION 1 FROM dst;
SELECT count(), sum(d) FROM dst;

INSERT INTO test_block_numbers SELECT max(max_block_number) AS m FROM system.parts WHERE database=currentDatabase() AND  table='dst' AND active AND name LIKE '1_%';
SELECT (max(m) - min(m) > 1) AS new_block_is_generated FROM test_block_numbers;
DROP TEMPORARY TABLE test_block_numbers;


SELECT 'ATTACH FROM';
ALTER STREAM dst DROP PARTITION 1;
DROP STREAM src;

create stream src (p uint64, k string, d uint64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO src VALUES (1, '0', 1);
INSERT INTO src VALUES (1, '1', 1);

SYSTEM STOP MERGES dst;
INSERT INTO dst VALUES (1, '1', 2);
ALTER STREAM dst ATTACH PARTITION 1 FROM src;
SELECT count(), sum(d) FROM dst;


SELECT 'OPTIMIZE';
SELECT count(), sum(d), uniqExact(_part) FROM dst;
SYSTEM START MERGES dst;
SET optimize_throw_if_noop=1;
OPTIMIZE STREAM dst;
SELECT count(), sum(d), uniqExact(_part) FROM dst;


SELECT 'After restart';
DETACH TABLE dst;
ATTACH TABLE dst;
SELECT count(), sum(d) FROM dst;

SELECT 'DETACH+ATTACH PARTITION';
ALTER STREAM dst DETACH PARTITION 0;
ALTER STREAM dst DETACH PARTITION 1;
ALTER STREAM dst DETACH PARTITION 2;
ALTER STREAM dst ATTACH PARTITION 1;
SELECT count(), sum(d) FROM dst;

DROP STREAM IF EXISTS src;
DROP STREAM IF EXISTS dst;
