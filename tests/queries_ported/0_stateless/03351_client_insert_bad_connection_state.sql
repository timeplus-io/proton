INSERT INTO function null('x string') FROM INFILE '/dev/null'; -- { clientError BAD_ARGUMENTS }
-- previously next query throws "Unexpected packet Query received from client."
SELECT 'Ok';
-- previously next query hangs
SELECT 'Ok Ok';
