DROP STREAM IF EXISTS stream2;
DROP STREAM IF EXISTS stream3;

CREATE STREAM stream2 AS numbers(5);
CREATE STREAM stream3 AS stream2;

SHOW CREATE stream2;
SHOW CREATE stream3;

SELECT count(), sum(number) FROM stream2;
SELECT count(), sum(number) FROM stream3;

DROP STREAM stream2;
DROP STREAM stream3;
