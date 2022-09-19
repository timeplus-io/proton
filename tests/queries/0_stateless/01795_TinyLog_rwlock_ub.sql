DROP STREAM IF EXISTS underlying_01795;
create stream underlying_01795 (key uint64) Engine=TinyLog();
INSERT INTO FUNCTION remote('127.1', currentDatabase(), underlying_01795) SELECT to_uint64(number) FROM system.numbers LIMIT 1;
SELECT * FROM underlying_01795 FORMAT Null;
DROP STREAM underlying_01795;
