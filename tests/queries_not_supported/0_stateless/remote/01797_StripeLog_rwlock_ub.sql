DROP STREAM IF EXISTS underlying_01797;
create stream underlying_01797 (key uint64) Engine=StripeLog();
INSERT INTO FUNCTION remote('127.1', currentDatabase(), underlying_01797) SELECT to_uint64(number) FROM system.numbers LIMIT 1;
SELECT * FROM underlying_01797 FORMAT Null;
DROP STREAM underlying_01797;
