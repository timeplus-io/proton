DROP STREAM IF EXISTS limited_merge_table;

SET max_threads = 1;
SET max_block_size = 1;
SET min_insert_block_size_rows = 1;

create stream limited_merge_table
(
    key uint64
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS max_parts_to_merge_at_once = 3;

SYSTEM STOP MERGES limited_merge_table;

INSERT INTO limited_merge_table SELECT number FROM numbers(100);

SYSTEM START MERGES limited_merge_table;

OPTIMIZE TABLE limited_merge_table FINAL;

SYSTEM FLUSH LOGS;

SELECT count() FROM limited_merge_table;

-- final optimize FINAL will merge all parts, but all previous merges must merge <= 3 parts.
-- During concurrent run only one final merge can happen, thats why we have this `if`.
SELECT if(length(top_k(2)(length(merged_from))) == 2, arrayMin(top_k(2)(length(merged_from))) <= 3, 1)
FROM system.part_log WHERE table = 'limited_merge_table' and database = currentDatabase() and event_type = 'MergeParts';

DROP STREAM IF EXISTS limited_merge_table;
