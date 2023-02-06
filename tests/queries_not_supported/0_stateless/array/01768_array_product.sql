SELECT 'array product with constant column';

SELECT array_product([1,2,3,4,5,6]) as a, to_type_name(a);
SELECT array_product(array(1.0,2.0,3.0,4.0)) as a, to_type_name(a);
SELECT array_product(array(1,3.5)) as a, to_type_name(a);
SELECT array_product([to_decimal64(1,8), to_decimal64(2,8), to_decimal64(3,8)]) as a, to_type_name(a);

SELECT 'array product with non constant column';

DROP STREAM IF EXISTS test_aggregation;
CREATE STREAM test_aggregation (x array(int)) ENGINE=TinyLog;
INSERT INTO test_aggregation VALUES ([1,2,3,4]), ([]), ([1,2,3]);
SELECT array_product(x) FROM test_aggregation;
DROP STREAM test_aggregation;

CREATE STREAM test_aggregation (x array(decimal64(8))) ENGINE=TinyLog;
INSERT INTO test_aggregation VALUES ([1,2,3,4]), ([]), ([1,2,3]);
SELECT array_product(x) FROM test_aggregation;
DROP STREAM test_aggregation;

SELECT 'Types of aggregation result array product';
SELECT to_type_name(array_product([to_int8(0)])), to_type_name(array_product([to_int16(0)])), to_type_name(array_product([to_int32(0)])), to_type_name(array_product([to_int64(0)]));
SELECT to_type_name(array_product([to_uint8(0)])), to_type_name(array_product([to_uint16(0)])), to_type_name(array_product([to_uint32(0)])), to_type_name(array_product([to_uint64(0)]));
SELECT to_type_name(array_product([to_int128(0)])), to_type_name(array_product([to_int256(0)])), to_type_name(array_product([to_uint256(0)]));
SELECT to_type_name(array_product([to_float32(0)])), to_type_name(array_product([to_float64(0)]));
SELECT to_type_name(array_product([to_decimal32(0, 8)])), to_type_name(array_product([to_decimal64(0, 8)])), to_type_name(array_product([to_decimal128(0, 8)]));
