
SELECT hex(xxHash64('')) = upper('ef46db3751d8e999');
SELECT hex(xxHash32('')) = upper('02cc5d05');

SELECT hex(xxHash64('ABC')) = upper('e66ae7354fcfee98');
SELECT hex(xxHash32('ABC')) = upper('80712ed5');

SELECT hex(xxHash64('xxhash')) = upper('32dd38952c4bc720');

--

SELECT xxHash64(NULL) is NULL;
SELECT xxHash64()       = to_uint64(16324913028386710556);

SELECT xxHash64(0)      = to_uint64(16804241149081757544);
SELECT xxHash64(123456) = to_uint64(9049736899514479480);

select xxHash64(to_uint8(0))  = xxHash64('\0');
select xxHash64(to_uint16(0)) = xxHash64('\0\0');
select xxHash64(to_uint32(0)) = xxHash64('\0\0\0\0');
select xxHash64(to_uint64(0)) = xxHash64('\0\0\0\0\0\0\0\0');

SELECT xxHash64(CAST(3 AS uint8))        = to_uint64(2244420788148980662);
SELECT xxHash64(CAST(1.2684 AS Float32)) = to_uint64(6662491266811474554);
SELECT xxHash64(CAST(-154477 AS int64))  = to_uint64(1162348840373071858);

SELECT xxHash64('')    = to_uint64(17241709254077376921);
SELECT xxHash64('foo') = to_uint64(3728699739546630719);
SELECT xxHash64(CAST('foo' AS FixedString(3))) = xxHash64('foo');
SELECT xxHash64(CAST('bar' AS FixedString(3))) = to_uint64(5234164152756840025);
SELECT xxHash64(x) = to_uint64(9962287286179718960) FROM (SELECT CAST(1 AS Enum8('a' = 1, 'b' = 2)) as x);

SELECT xxHash64('\x01') = to_uint64(9962287286179718960);
SELECT xxHash64('\x02\0') = to_uint64(6482051057365497128);
SELECT xxHash64('\x03\0\0\0') = to_uint64(13361037350151369407);

SELECT xxHash64(1) = to_uint64(9962287286179718960);
SELECT xxHash64(to_uint16(2)) = to_uint64(6482051057365497128);
SELECT xxHash64(to_uint32(3)) = to_uint64(13361037350151369407);

SELECT xxHash64(1, 2, 3) = to_uint64(13728743482242651702);
SELECT xxHash64(1, 3, 2) = to_uint64(10226792638577471533);
SELECT xxHash64(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) = to_uint64(3521288460171939489);

--

SELECT xxHash32(NULL) is NULL;
SELECT xxHash32()       = to_uint32(4263699484);

SELECT xxHash32(0)      = to_uint32(3479547966);
SELECT xxHash32(123456) = to_uint32(1434661961);

select xxHash32(to_uint8(0))  = xxHash32('\0');
select xxHash32(to_uint16(0)) = xxHash32('\0\0');
select xxHash32(to_uint32(0)) = xxHash32('\0\0\0\0');

SELECT xxHash32(CAST(3 AS uint8))        = to_uint32(565077562);
SELECT xxHash32(CAST(1.2684 AS Float32)) = to_uint32(3120514536);
SELECT xxHash32(CAST(-154477 AS int32))  = to_uint32(3279223048);

SELECT xxHash32('')    = to_uint32(46947589);
SELECT xxHash32('foo') = to_uint32(3792637401);
SELECT xxHash32(CAST('foo' AS FixedString(3))) = xxHash32('foo');
SELECT xxHash32(CAST('bar' AS FixedString(3))) = to_uint32(1101146924);
SELECT xxHash32(x) = to_uint32(949155633) FROM (SELECT CAST(1 AS Enum8('a' = 1, 'b' = 2)) as x);

SELECT xxHash32('\x01') = to_uint32(949155633);
SELECT xxHash32('\x02\0') = to_uint32(332955956);
SELECT xxHash32('\x03\0\0\0') = to_uint32(2158931063);

SELECT xxHash32(1) = to_uint32(949155633);
SELECT xxHash32(to_uint16(2)) = to_uint32(332955956);
SELECT xxHash32(to_uint32(3)) = to_uint32(2158931063);

SELECT xxHash32(1, 2, 3) = to_uint32(441104368);
SELECT xxHash32(1, 3, 2) = to_uint32(912264289);
SELECT xxHash32(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) = to_uint32(1930126291);

