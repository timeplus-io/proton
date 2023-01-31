SET query_mode='table';
SET asterisk_include_reserved_columns=false;
DROP STREAM IF EXISTS test_00861;
create stream test_00861 (key uint64, d32 Decimal32(2), d64 Decimal64(2), d128 Decimal128(2)) ;

INSERT INTO test_00861(key,d32,d64,d128) FORMAT CSV "1","1","1","1"
;
INSERT INTO test_00861(key,d32,d64,d128) FORMAT CSV "2","-1","-1","-1"
;
INSERT INTO test_00861(key,d32,d64,d128) FORMAT CSV "3","1.0","1.0","1.0"
;
INSERT INTO test_00861(key,d32,d64,d128) FORMAT CSV "4","-0.1","-0.1","-0.1"
;
INSERT INTO test_00861(key,d32,d64,d128) FORMAT CSV "5","0.010","0.010","0.010"
;
SELECT sleep(3);
SELECT * FROM test_00861 ORDER BY key;

DROP STREAM test_00861;
