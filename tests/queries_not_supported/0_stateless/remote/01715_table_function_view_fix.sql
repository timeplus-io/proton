SELECT view(SELECT 1); -- { clientError 62 }

SELECT sum_if(dummy, dummy) FROM remote('127.0.0.{1,2}', numbers(2, 100), view(SELECT CAST(NULL, 'Nullable(uint8)') AS dummy FROM system.one)); -- { serverError 183 }
