DROP STREAM IF EXISTS data_compact;
DROP STREAM IF EXISTS data_memory;
DROP STREAM IF EXISTS data_wide;

-- compact
DROP STREAM IF EXISTS data_compact;
CREATE STREAM data_compact
(
    `root.array` array(uint8)
)
ENGINE = MergeTree()
ORDER BY tuple_cast()
SETTINGS min_rows_for_compact_part=0, min_bytes_for_compact_part=0, min_rows_for_wide_part=100, min_bytes_for_wide_part=1e9;
INSERT INTO data_compact VALUES ([0]);
ALTER STREAM data_compact ADD COLUMN root.nested_array array(array(uint8));
SELECT table, part_type FROM system.parts WHERE table = 'data_compact' AND database = current_database();
SELECT root.nested_array FROM data_compact;

-- memory
DROP STREAM IF EXISTS data_memory;
CREATE STREAM data_memory
(
    `root.array` array(uint8)
)
ENGINE = MergeTree()
ORDER BY tuple_cast()
SETTINGS min_rows_for_compact_part=100, min_bytes_for_compact_part=1e9, min_rows_for_wide_part=100, min_bytes_for_wide_part=1e9, in_memory_parts_enable_wal=0;
INSERT INTO data_memory VALUES ([0]);
ALTER STREAM data_memory ADD COLUMN root.nested_array array(array(uint8));
SELECT table, part_type FROM system.parts WHERE table = 'data_memory' AND database = current_database();
SELECT root.nested_array FROM data_memory;

-- wide
DROP STREAM IF EXISTS data_wide;
CREATE STREAM data_wide
(
    `root.array` array(uint8)
)
ENGINE = MergeTree()
ORDER BY tuple_cast()
SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0, min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
INSERT INTO data_wide VALUES ([0]);
ALTER STREAM data_wide ADD COLUMN root.nested_array array(array(uint8));
SELECT table, part_type FROM system.parts WHERE table = 'data_wide' AND database = current_database();
SELECT root.nested_array FROM data_wide;
