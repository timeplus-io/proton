-- Part of 00961_check_table test, but with in-memory parts
SET check_query_single_value_result = 0;
DROP STREAM IF EXISTS mt_table;
create stream mt_table (d date, key uint64, data string) ENGINE = MergeTree() PARTITION BY toYYYYMM(d) ORDER BY key
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_compact_part = 1000;

CHECK TABLE mt_table;
INSERT INTO mt_table VALUES (to_date('2019-01-02'), 1, 'Hello'), (to_date('2019-01-02'), 2, 'World');
CHECK TABLE mt_table;
DROP STREAM mt_table;
