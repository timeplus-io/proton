SELECT bitmaskToList(0);
SELECT bitmaskToArray(0);
SELECT bitmaskToList(7);
SELECT bitmaskToArray(7);
SELECT bitmaskToList(-1);
SELECT bitmaskToArray(-1);
SELECT bitmaskToList(-128);
SELECT bitmaskToArray(-128);

SELECT bitmaskToList(to_int64(0));
SELECT bitmaskToArray(to_int64(0));
SELECT bitmaskToList(to_int64(7));
SELECT bitmaskToArray(to_int64(7));
SELECT bitmaskToList(to_int64(-1));
SELECT bitmaskToArray(to_int64(-1));
SELECT bitmaskToList(to_int64(-128));
SELECT bitmaskToArray(to_int64(-128));
