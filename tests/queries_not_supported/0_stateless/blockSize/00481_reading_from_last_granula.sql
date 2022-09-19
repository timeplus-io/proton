DROP STREAM IF EXISTS tab_00481;
create stream tab_00481 (date date, value uint64, s string, m FixedString(16)) ENGINE = MergeTree(date, (date, value), 8);
INSERT INTO tab_00481 SELECT today() as date, number as value, '' as s, to_fixed_string('', 16) as m from system.numbers limit 42;
SET preferred_max_column_in_block_size_bytes = 32;
SELECT blockSize(), * from tab_00481 format Null;
SELECT 0;

DROP STREAM tab_00481;
