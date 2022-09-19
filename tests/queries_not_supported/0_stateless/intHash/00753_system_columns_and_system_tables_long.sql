-- Tags: long

DROP STREAM IF EXISTS check_system_tables;

-- Check MergeTree declaration in new format
create stream check_system_tables
  (
    name1 uint8,
    name2 uint8,
    name3 uint8
  ) ENGINE = MergeTree()
    ORDER BY name1
    PARTITION BY name2
    SAMPLE BY name1
    SETTINGS min_bytes_for_wide_part = 0;

SELECT name, partition_key, sorting_key, primary_key, sampling_key, storage_policy, total_rows
FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

SELECT name, is_in_partition_key, is_in_sorting_key, is_in_primary_key, is_in_sampling_key
FROM system.columns WHERE table = 'check_system_tables' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

INSERT INTO check_system_tables VALUES (1, 1, 1);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();

DROP STREAM IF EXISTS check_system_tables;

-- Check VersionedCollapsingMergeTree
create stream check_system_tables
  (
    date date,
    value string,
    version uint64,
    sign int8
  ) ENGINE = VersionedCollapsingMergeTree(sign, version)
    PARTITION BY date
    ORDER BY date;

SELECT name, partition_key, sorting_key, primary_key, sampling_key
FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

SELECT name, is_in_partition_key, is_in_sorting_key, is_in_primary_key, is_in_sampling_key
FROM system.columns WHERE table = 'check_system_tables' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

DROP STREAM IF EXISTS check_system_tables;

-- Check MergeTree declaration in old format
create stream check_system_tables
  (
    Event date,
    UserId uint32,
    Counter uint32
  ) ENGINE = MergeTree(Event, intHash32(UserId), (Counter, Event, intHash32(UserId)), 8192);

SELECT name, partition_key, sorting_key, primary_key, sampling_key
FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

SELECT name, is_in_partition_key, is_in_sorting_key, is_in_primary_key, is_in_sampling_key
FROM system.columns WHERE table = 'check_system_tables' AND database = currentDatabase()
FORMAT PrettyCompactNoEscapes;

DROP STREAM IF EXISTS check_system_tables;

SELECT 'Check total_bytes/total_rows for TinyLog';
create stream check_system_tables (key uint8) ();
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables VALUES (1);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
DROP STREAM check_system_tables;

SELECT 'Check total_bytes/total_rows for Memory';
create stream check_system_tables (key UInt16) ();
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables VALUES (1);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
DROP STREAM check_system_tables;

SELECT 'Check total_bytes/total_rows for Buffer';
DROP STREAM IF EXISTS check_system_tables;
DROP STREAM IF EXISTS check_system_tables_null;
create stream check_system_tables_null (key UInt16) ENGINE = Null();
create stream check_system_tables (key UInt16) ENGINE = Buffer(
    currentDatabase(),
    check_system_tables_null,
    2,
    0,   100, /* min_time /max_time */
    100, 100, /* min_rows /max_rows */
    0,   1e6  /* min_bytes/max_bytes */
);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables SELECT * FROM numbers_mt(50);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();

SELECT 'Check lifetime_bytes/lifetime_rows for Buffer';
SELECT lifetime_bytes, lifetime_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
OPTIMIZE STREAM check_system_tables; -- flush
SELECT lifetime_bytes, lifetime_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables SELECT * FROM numbers_mt(50);
SELECT lifetime_bytes, lifetime_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
OPTIMIZE STREAM check_system_tables; -- flush
SELECT lifetime_bytes, lifetime_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables SELECT * FROM numbers_mt(101); -- direct block write (due to min_rows exceeded)
SELECT lifetime_bytes, lifetime_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
DROP STREAM check_system_tables;
DROP STREAM check_system_tables_null;

SELECT 'Check total_bytes/total_rows for Set';
create stream check_system_tables Engine=Set() AS SELECT * FROM numbers(50);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables SELECT number+50 FROM numbers(50);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
DROP STREAM check_system_tables;

SELECT 'Check total_bytes/total_rows for Join';
create stream check_system_tables Engine=Join(ANY, LEFT, number) AS SELECT * FROM numbers(50);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables SELECT number+50 FROM numbers(50);
SELECT total_bytes, total_rows FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
DROP STREAM check_system_tables;
