-- { echo }

DROP STREAM IF EXISTS in_memory;

CREATE STREAM in_memory (a uint32) ENGINE = MergeTree ORDER BY a SETTINGS min_rows_for_compact_part = 1000, min_bytes_for_wide_part = 10485760;
INSERT INTO in_memory VALUES (1);
INSERT INTO in_memory VALUES (2);
SELECT name, active, part_type FROM system.parts WHERE database = currentDatabase() AND stream = 'in_memory';
SELECT * FROM in_memory ORDER BY a;

-- no WAL remove since parts are still in use
DETACH STREAM in_memory;
ATTACH STREAM in_memory;
SELECT name, active, part_type FROM system.parts WHERE database = currentDatabase() AND stream = 'in_memory';
SELECT * FROM in_memory ORDER BY a;

-- WAL should be removed, since on disk part covers all parts in WAL
OPTIMIZE STREAM in_memory;
DETACH STREAM in_memory;
ATTACH STREAM in_memory;
SELECT name, active, part_type FROM system.parts WHERE database = currentDatabase() AND stream = 'in_memory';

-- check that the WAL will be reinitialized after remove
INSERT INTO in_memory VALUES (3);
DETACH STREAM in_memory;
ATTACH STREAM in_memory;
SELECT * FROM in_memory ORDER BY a;
