DROP STREAM IF EXISTS table2;
DROP STREAM IF EXISTS table3;

create stream table2 AS numbers(5);
create stream table3 AS table2;

SHOW CREATE table2;
SHOW CREATE table3;

SELECT count(), sum(number) FROM table2;
SELECT count(), sum(number) FROM table3;

DROP STREAM table2;
DROP STREAM table3;
