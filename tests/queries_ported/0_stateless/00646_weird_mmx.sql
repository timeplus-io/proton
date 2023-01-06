SET asterisk_include_reserved_columns=false;
SET query_mode='table';
DROP STREAM IF EXISTS weird_mmx;

create stream weird_mmx (x array(uint64)) ;
-- this triggers overlapping matches in LZ4 decompression routine; 915 is the minimum number
-- see comment in LZ4_decompression_faster.cpp about usage of MMX registers
INSERT INTO weird_mmx(x) SELECT range(number % 10) FROM system.numbers LIMIT 915;
select sleep(3);
SELECT sum(length(*)) FROM weird_mmx;

DROP STREAM weird_mmx;
