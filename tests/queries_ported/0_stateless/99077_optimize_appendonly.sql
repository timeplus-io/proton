drop stream if exists example;

CREATE STREAM example (
    primary_key int32,
    secondary_key int32,
    value uint32,
    partition_key uint32,
    materialized_value uint32,
    aliased_value uint32 ALIAS 2,
    PRIMARY KEY primary_key
)
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);

select 'original status';
INSERT INTO example (primary_key, secondary_key, value, partition_key) VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
select sleep(3) FORMAT Null;

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);

OPTIMIZE STREAM example FINAL DEDUPLICATE;
select sleep(3) FORMAT Null;
select 'after OPTIMIZE STREAM example FINAL DEDUPLICATE;';

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);



------------------------------------------------------


drop stream if exists example;

CREATE STREAM example (
    primary_key int32,
    secondary_key int32,
    value uint32,
    partition_key uint32,
    materialized_value uint32,
    aliased_value uint32 ALIAS 2,
    PRIMARY KEY primary_key
)
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);

select 'original status';
INSERT INTO example (primary_key, secondary_key, value, partition_key) VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
select sleep(3) FORMAT Null;

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);

OPTIMIZE STREAM example FINAL DEDUPLICATE BY *;
select sleep(3) FORMAT Null;
select 'after OPTIMIZE STREAM example FINAL DEDUPLICATE BY *;';

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);


------------------------------------------------------
drop stream if exists example;

CREATE STREAM example (
    primary_key int32,
    secondary_key int32,
    value uint32,
    partition_key uint32,
    materialized_value uint32,
    aliased_value uint32 ALIAS 2,
    PRIMARY KEY primary_key
)
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);

select 'original status';
INSERT INTO example (primary_key, secondary_key, value, partition_key) VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
select sleep(3) FORMAT Null;

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);

OPTIMIZE STREAM example FINAL DEDUPLICATE BY * EXCEPT value;
select sleep(3) FORMAT Null;
select 'after OPTIMIZE STREAM example FINAL DEDUPLICATE BY * EXCEPT value;';

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);



------------------------------------------------------
drop stream if exists example;

CREATE STREAM example (
    primary_key int32,
    secondary_key int32,
    value uint32,
    partition_key uint32,
    materialized_value uint32,
    aliased_value uint32 ALIAS 2,
    PRIMARY KEY primary_key
)
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);

select 'original status';
INSERT INTO example (primary_key, secondary_key, value, partition_key) VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
select sleep(3) FORMAT Null;

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);

OPTIMIZE STREAM example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
select sleep(3) FORMAT Null;
select 'after OPTIMIZE STREAM example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;';

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);



------------------------------------------------------
drop stream if exists example;

CREATE STREAM example (
    primary_key int32,
    secondary_key int32,
    value uint32,
    partition_key uint32,
    materialized_value uint32,
    aliased_value uint32 ALIAS 2,
    PRIMARY KEY primary_key
)
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);

select 'original status';
INSERT INTO example (primary_key, secondary_key, value, partition_key) VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
select sleep(3) FORMAT Null;

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);

OPTIMIZE STREAM example FINAL DEDUPLICATE BY COLUMNS('.*_key');
select sleep(3) FORMAT Null;
select 'after OPTIMIZE STREAM example FINAL DEDUPLICATE BY COLUMNS(.*_key)';

SELECT primary_key, secondary_key, value, partition_key FROM table(example) ORDER BY (primary_key, secondary_key, value, partition_key);





---------------------------

-- Setup: Create a table and insert data into two partitions
DROP STREAM IF EXISTS optimize_partition_test;

CREATE STREAM optimize_partition_test (
    id int,
    ts datetime,
    event_type string,
    partition_key uint16
)
PARTITION BY partition_key
ORDER BY (id, ts);

-- Insert duplicate data into partition 101
INSERT INTO optimize_partition_test(id, ts, event_type, partition_key) VALUES (1, '2023-01-01 10:00:00', 'login', 101);
INSERT INTO optimize_partition_test(id, ts, event_type, partition_key) VALUES (1, '2023-01-01 10:00:00', 'login', 101);

-- Insert duplicate data into partition 102
INSERT INTO optimize_partition_test(id, ts, event_type, partition_key) VALUES (2, '2023-01-02 12:00:00', 'logout', 102);
INSERT INTO optimize_partition_test(id, ts, event_type, partition_key) VALUES (2, '2023-01-02 12:00:00', 'logout', 102);
SELECT sleep(3) FORMAT Null; -- Allow parts to be written

-- Verify initial state: 4 rows total
SELECT '--- Initial State ---';
SELECT * except _tp_time FROM table(optimize_partition_test) ORDER BY partition_key, id;

-- Also check the system.parts table to see the initial parts
SELECT active, partition, name, rows FROM system.parts WHERE table = 'optimize_partition_test';

-- Action: Optimize ONLY partition 101
SELECT '--- Running OPTIMIZE on Partition 101 ---';
OPTIMIZE STREAM optimize_partition_test PARTITION 101 FINAL DEDUPLICATE;
SELECT sleep(3) FORMAT Null; -- Allow merge to complete

-- Verification: Check the final state
SELECT '--- Final State ---';
SELECT * except _tp_time FROM table(optimize_partition_test) ORDER BY partition_key, id;

-- Check the system.parts table again. Partition 101 should have one merged part.
-- Partition 102 should still have its original parts.
SELECT active, partition, name, rows FROM system.parts WHERE table = 'optimize_partition_test';

drop stream optimize_partition_test;
