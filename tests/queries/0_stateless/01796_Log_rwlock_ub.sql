DROP STREAM IF EXISTS underlying_01796;
create stream underlying_01796 (key uint64) Engine=Log();
INSERT INTO FUNCTION remote('127.1', currentDatabase(), underlying_01796) SELECT to_uint64(number) FROM system.numbers LIMIT 1;
SELECT * FROM underlying_01796 FORMAT Null;
DROP STREAM underlying_01796;
