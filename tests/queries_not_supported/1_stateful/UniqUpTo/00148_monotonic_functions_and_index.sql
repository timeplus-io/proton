--SET max_rows_to_read = 60000;

SELECT count() FROM table(test.hits) WHERE -CounterID = -1731;
SELECT count() FROM table(test.hits) WHERE abs(-CounterID) = 1731;
SELECT count() FROM table(test.hits) WHERE -abs(CounterID) = -1731;
SELECT count() FROM table(test.hits) WHERE to_uint32(CounterID) = 1731;
SELECT count() FROM table(test.hits) WHERE to_int32(CounterID) = 1731;
SELECT count() FROM table(test.hits) WHERE to_float32(CounterID) = 1731;

SET max_rows_to_read = 0;

SELECT count() FROM table(test.hits) WHERE to_int16(CounterID) = 1731;
SELECT count() FROM table(test.hits) WHERE to_int8(CounterID) = to_int8(1731);

SELECT count() FROM table(test.hits) WHERE to_date(to_uint16(CounterID)) = to_date(1731);

SELECT uniq(CounterID), uniqUpTo(5)(to_int8(CounterID)), count() FROM table(test.hits) WHERE to_int8(CounterID + 1 - 1) = to_int8(1731);
SELECT uniq(CounterID), uniqUpTo(5)(to_int8(CounterID)), count() FROM table(test.hits) WHERE to_int8(CounterID) = to_int8(1731);

SELECT uniq(CounterID), uniqUpTo(5)(to_int16(CounterID)), count() FROM table(test.hits) WHERE to_int16(CounterID + 1 - 1) = 1731;
SELECT uniq(CounterID), uniqUpTo(5)(to_int16(CounterID)), count() FROM table(test.hits) WHERE to_int16(CounterID) = 1731;

--SET max_rows_to_read = 500000;

SELECT uniq(CounterID), count() FROM table(test.hits) WHERE to_string(CounterID) = '1731';

--SET max_rows_to_read = 2200000;

SELECT count() FROM table(test.hits) WHERE CounterID < 732797;
SELECT count() FROM table(test.hits) WHERE CounterID <= 732797;
SELECT count() FROM table(test.hits) WHERE CounterID < 732797 AND CounterID > 107931;
SELECT count() FROM table(test.hits) WHERE CounterID < 732797 AND CounterID >= 107931;
SELECT count() FROM table(test.hits) WHERE CounterID <= 732797 AND CounterID > 107931;
SELECT count() FROM table(test.hits) WHERE CounterID <= 732797 AND CounterID >= 107931;
SELECT count() FROM table(test.hits) WHERE -CounterID > -732797;
SELECT count() FROM table(test.hits) WHERE -CounterID >= -732797;
SELECT count() FROM table(test.hits) WHERE -CounterID > -732797 AND CounterID > 107931;
SELECT count() FROM table(test.hits) WHERE -CounterID > -732797 AND CounterID >= 107931;
SELECT count() FROM table(test.hits) WHERE -CounterID >= -732797 AND CounterID > 107931;
SELECT count() FROM table(test.hits) WHERE -CounterID >= -732797 AND CounterID >= 107931;
SELECT count() FROM table(test.hits) WHERE CounterID < 732797 AND -CounterID < -107931;
SELECT count() FROM table(test.hits) WHERE CounterID < 732797 AND -CounterID <= -107931;
SELECT count() FROM table(test.hits) WHERE CounterID <= 732797 AND -CounterID < -107931;
SELECT count() FROM table(test.hits) WHERE CounterID <= 732797 AND -CounterID <= -107931;

--SET max_rows_to_read = 0;

SELECT count() FROM table(test.hits) WHERE EventDate = '2014-03-20';
SELECT count() FROM table(test.hits) WHERE toDayOfMonth(EventDate) = 20;
SELECT count() FROM table(test.hits) WHERE toDayOfWeek(EventDate) = 4;
SELECT count() FROM table(test.hits) WHERE to_uint16(EventDate) = to_uint16(to_date('2014-03-20'));
SELECT count() FROM table(test.hits) WHERE to_int64(EventDate) = to_int64(to_date('2014-03-20'));
SELECT count() FROM table(test.hits) WHERE to_datetime(EventDate) = '2014-03-20 00:00:00';

--SET max_rows_to_read = 50000;

SELECT count() FROM table(test.hits) WHERE toMonth(EventDate) != 3;
SELECT count() FROM table(test.hits) WHERE toYear(EventDate) != 2014;
SELECT count() FROM table(test.hits) WHERE toDayOfMonth(EventDate) > 23 OR toDayOfMonth(EventDate) < 17;
