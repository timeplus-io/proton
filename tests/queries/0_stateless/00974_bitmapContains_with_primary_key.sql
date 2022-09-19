DROP STREAM IF EXISTS test;
create stream test (num uint64, str string) ENGINE = MergeTree ORDER BY num;
INSERT INTO test (num) VALUES (1), (2), (10), (15), (23);
SELECT count(*) FROM test WHERE bitmapContains(bitmapBuild([1, 5, 7, 9]), to_uint8(num));
SELECT count(*) FROM test WHERE bitmapContains(bitmapBuild([1, 5, 7, 9]), to_uint16(num));
SELECT count(*) FROM test WHERE bitmapContains(bitmapBuild([1, 5, 7, 9]), to_uint32(num));
SELECT count(*) FROM test WHERE bitmapContains(bitmapBuild([1, 5, 7, 9]), to_uint64(num));
DROP STREAM test;
