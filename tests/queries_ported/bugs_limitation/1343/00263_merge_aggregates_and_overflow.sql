SET query_mode='table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS numbers_10k_log;

SET max_block_size = 1000;

create stream numbers_10k_log   AS SELECT number FROM system.numbers LIMIT 0;

SET max_threads = 4;
SET max_rows_to_group_by = 3000, group_by_overflow_mode = 'any';

SELECT ignore(rand() AS k), ignore(max(to_string(number))) FROM numbers_10k_log GROUP BY k LIMIT 1;

DROP STREAM numbers_10k_log;
