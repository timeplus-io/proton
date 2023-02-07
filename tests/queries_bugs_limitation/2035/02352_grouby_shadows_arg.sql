-- { echoOn }
SELECT to_string(dummy) as dummy FROM remote('127.{1,1}', 'system.one') GROUP BY dummy;
SELECT to_string(dummy+1) as dummy FROM remote('127.{1,1}', 'system.one') GROUP BY dummy;
SELECT to_string((to_int8(dummy)+2) * (to_int8(dummy)+2)) as dummy FROM remote('127.{1,1}', system.one) GROUP BY dummy;
SELECT round(number % 3) AS number FROM remote('127.{1,1}', numbers(20)) GROUP BY number ORDER BY number ASC;
