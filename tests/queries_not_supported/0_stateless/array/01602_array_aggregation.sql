SELECT 'array min ', (arrayMin(array(1,2,3,4,5,6)));
SELECT 'array max ', (arrayMax(array(1,2,3,4,5,6)));
SELECT 'array sum ', (arraySum(array(1,2,3,4,5,6)));
SELECT 'array avg ', (arrayAvg(array(1,2,3,4,5,6)));

DROP STREAM IF EXISTS test_aggregation;
CREATE STREAM test_aggregation (x array(int)) ENGINE=TinyLog;

INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array int min';
SELECT arrayMin(x) FROM test_aggregation;
SELECT 'Table array int max';
SELECT arrayMax(x) FROM test_aggregation;
SELECT 'Table array int sum';
SELECT arraySum(x) FROM test_aggregation;
SELECT 'Table array int avg';
SELECT arrayAvg(x) FROM test_aggregation;

DROP STREAM test_aggregation;

CREATE STREAM test_aggregation (x array(decimal64(8))) ENGINE=TinyLog;

INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array decimal min';
SELECT arrayMin(x) FROM test_aggregation;
SELECT 'Table array decimal max';
SELECT arrayMax(x) FROM test_aggregation;
SELECT 'Table array decimal sum';
SELECT arraySum(x) FROM test_aggregation;
SELECT 'Table array decimal avg';
SELECT arrayAvg(x) FROM test_aggregation;

DROP STREAM test_aggregation;

SELECT 'Types of aggregation result array min';
SELECT to_type_name(arrayMin([to_int8(0)])), to_type_name(arrayMin([to_int16(0)])), to_type_name(arrayMin([to_int32(0)])), to_type_name(arrayMin([to_int64(0)]));
SELECT to_type_name(arrayMin([to_uint8(0)])), to_type_name(arrayMin([to_uint16(0)])), to_type_name(arrayMin([to_uint32(0)])), to_type_name(arrayMin([to_uint64(0)]));
SELECT to_type_name(arrayMin([to_int128(0)])), to_type_name(arrayMin([to_int256(0)])), to_type_name(arrayMin([to_uint256(0)]));
SELECT to_type_name(arrayMin([to_float32(0)])), to_type_name(arrayMin([to_float64(0)]));
SELECT to_type_name(arrayMin([to_decimal32(0, 8)])), to_type_name(arrayMin([to_decimal64(0, 8)])), to_type_name(arrayMin([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array max';
SELECT to_type_name(arrayMax([to_int8(0)])), to_type_name(arrayMax([to_int16(0)])), to_type_name(arrayMax([to_int32(0)])), to_type_name(arrayMax([to_int64(0)]));
SELECT to_type_name(arrayMax([to_uint8(0)])), to_type_name(arrayMax([to_uint16(0)])), to_type_name(arrayMax([to_uint32(0)])), to_type_name(arrayMax([to_uint64(0)]));
SELECT to_type_name(arrayMax([to_int128(0)])), to_type_name(arrayMax([to_int256(0)])), to_type_name(arrayMax([to_uint256(0)]));
SELECT to_type_name(arrayMax([to_float32(0)])), to_type_name(arrayMax([to_float64(0)]));
SELECT to_type_name(arrayMax([to_decimal32(0, 8)])), to_type_name(arrayMax([to_decimal64(0, 8)])), to_type_name(arrayMax([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array summ';
SELECT to_type_name(arraySum([to_int8(0)])), to_type_name(arraySum([to_int16(0)])), to_type_name(arraySum([to_int32(0)])), to_type_name(arraySum([to_int64(0)]));
SELECT to_type_name(arraySum([to_uint8(0)])), to_type_name(arraySum([to_uint16(0)])), to_type_name(arraySum([to_uint32(0)])), to_type_name(arraySum([to_uint64(0)]));
SELECT to_type_name(arraySum([to_int128(0)])), to_type_name(arraySum([to_int256(0)])), to_type_name(arraySum([to_uint256(0)]));
SELECT to_type_name(arraySum([to_float32(0)])), to_type_name(arraySum([to_float64(0)]));
SELECT to_type_name(arraySum([to_decimal32(0, 8)])), to_type_name(arraySum([to_decimal64(0, 8)])), to_type_name(arraySum([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array avg';
SELECT to_type_name(arrayAvg([to_int8(0)])), to_type_name(arrayAvg([to_int16(0)])), to_type_name(arrayAvg([to_int32(0)])), to_type_name(arrayAvg([to_int64(0)]));
SELECT to_type_name(arrayAvg([to_uint8(0)])), to_type_name(arrayAvg([to_uint16(0)])), to_type_name(arrayAvg([to_uint32(0)])), to_type_name(arrayAvg([to_uint64(0)]));
SELECT to_type_name(arrayAvg([to_int128(0)])), to_type_name(arrayAvg([to_int256(0)])), to_type_name(arrayAvg([to_uint256(0)]));
SELECT to_type_name(arrayAvg([to_float32(0)])), to_type_name(arrayAvg([to_float64(0)]));
SELECT to_type_name(arrayAvg([to_decimal32(0, 8)])), to_type_name(arrayAvg([to_decimal64(0, 8)])), to_type_name(arrayAvg([to_decimal128(0, 8)]));
