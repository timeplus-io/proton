SELECT 'array min ', (arrayMin(array(1,2,3,4,5,6)));
SELECT 'array max ', (arrayMax(array(1,2,3,4,5,6)));
SELECT 'array sum ', (array_sum(array(1,2,3,4,5,6)));
SELECT 'array avg ', (arrayAvg(array(1,2,3,4,5,6)));

DROP STREAM IF EXISTS test_aggregation;
create stream test_aggregation (x array(int)) ;

INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array int min';
SELECT arrayMin(x) FROM test_aggregation;
SELECT 'Table array int max';
SELECT arrayMax(x) FROM test_aggregation;
SELECT 'Table array int sum';
SELECT array_sum(x) FROM test_aggregation;
SELECT 'Table array int avg';
SELECT arrayAvg(x) FROM test_aggregation;

DROP STREAM test_aggregation;

create stream test_aggregation (x array(Decimal64(8))) ;

INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array decimal min';
SELECT arrayMin(x) FROM test_aggregation;
SELECT 'Table array decimal max';
SELECT arrayMax(x) FROM test_aggregation;
SELECT 'Table array decimal sum';
SELECT array_sum(x) FROM test_aggregation;
SELECT 'Table array decimal avg';
SELECT arrayAvg(x) FROM test_aggregation;

DROP STREAM test_aggregation;

SELECT 'Types of aggregation result array min';
SELECT to_type_name(arrayMin([to_int8(0)])), to_type_name(arrayMin([to_int16(0)])), to_type_name(arrayMin([to_int32(0)])), to_type_name(arrayMin([to_int64(0)]));
SELECT to_type_name(arrayMin([to_uint8(0)])), to_type_name(arrayMin([to_uint16(0)])), to_type_name(arrayMin([to_uint32(0)])), to_type_name(arrayMin([to_uint64(0)]));
SELECT to_type_name(arrayMin([to_int128(0)])), to_type_name(arrayMin([toInt256(0)])), to_type_name(arrayMin([toUInt256(0)]));
SELECT to_type_name(arrayMin([to_float32(0)])), to_type_name(arrayMin([to_float64(0)]));
SELECT to_type_name(arrayMin([to_decimal32(0, 8)])), to_type_name(arrayMin([to_decimal64(0, 8)])), to_type_name(arrayMin([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array max';
SELECT to_type_name(arrayMax([to_int8(0)])), to_type_name(arrayMax([to_int16(0)])), to_type_name(arrayMax([to_int32(0)])), to_type_name(arrayMax([to_int64(0)]));
SELECT to_type_name(arrayMax([to_uint8(0)])), to_type_name(arrayMax([to_uint16(0)])), to_type_name(arrayMax([to_uint32(0)])), to_type_name(arrayMax([to_uint64(0)]));
SELECT to_type_name(arrayMax([to_int128(0)])), to_type_name(arrayMax([toInt256(0)])), to_type_name(arrayMax([toUInt256(0)]));
SELECT to_type_name(arrayMax([to_float32(0)])), to_type_name(arrayMax([to_float64(0)]));
SELECT to_type_name(arrayMax([to_decimal32(0, 8)])), to_type_name(arrayMax([to_decimal64(0, 8)])), to_type_name(arrayMax([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array summ';
SELECT to_type_name(array_sum([to_int8(0)])), to_type_name(array_sum([to_int16(0)])), to_type_name(array_sum([to_int32(0)])), to_type_name(array_sum([to_int64(0)]));
SELECT to_type_name(array_sum([to_uint8(0)])), to_type_name(array_sum([to_uint16(0)])), to_type_name(array_sum([to_uint32(0)])), to_type_name(array_sum([to_uint64(0)]));
SELECT to_type_name(array_sum([to_int128(0)])), to_type_name(array_sum([toInt256(0)])), to_type_name(array_sum([toUInt256(0)]));
SELECT to_type_name(array_sum([to_float32(0)])), to_type_name(array_sum([to_float64(0)]));
SELECT to_type_name(array_sum([to_decimal32(0, 8)])), to_type_name(array_sum([to_decimal64(0, 8)])), to_type_name(array_sum([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array avg';
SELECT to_type_name(arrayAvg([to_int8(0)])), to_type_name(arrayAvg([to_int16(0)])), to_type_name(arrayAvg([to_int32(0)])), to_type_name(arrayAvg([to_int64(0)]));
SELECT to_type_name(arrayAvg([to_uint8(0)])), to_type_name(arrayAvg([to_uint16(0)])), to_type_name(arrayAvg([to_uint32(0)])), to_type_name(arrayAvg([to_uint64(0)]));
SELECT to_type_name(arrayAvg([to_int128(0)])), to_type_name(arrayAvg([toInt256(0)])), to_type_name(arrayAvg([toUInt256(0)]));
SELECT to_type_name(arrayAvg([to_float32(0)])), to_type_name(arrayAvg([to_float64(0)]));
SELECT to_type_name(arrayAvg([to_decimal32(0, 8)])), to_type_name(arrayAvg([to_decimal64(0, 8)])), to_type_name(arrayAvg([to_decimal128(0, 8)]));
