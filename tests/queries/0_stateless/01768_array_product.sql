SELECT 'array product with constant column';

SELECT arrayProduct([1,2,3,4,5,6]) as a, to_type_name(a);
SELECT arrayProduct(array(1.0,2.0,3.0,4.0)) as a, to_type_name(a);
SELECT arrayProduct(array(1,3.5)) as a, to_type_name(a);
SELECT arrayProduct([to_decimal64(1,8), to_decimal64(2,8), to_decimal64(3,8)]) as a, to_type_name(a);

SELECT 'array product with non constant column';

DROP STREAM IF EXISTS test_aggregation;
create stream test_aggregation (x array(int)) ;
INSERT INTO test_aggregation VALUES ([1,2,3,4]), ([]), ([1,2,3]);
SELECT arrayProduct(x) FROM test_aggregation;
DROP STREAM test_aggregation;

create stream test_aggregation (x array(Decimal64(8))) ;
INSERT INTO test_aggregation VALUES ([1,2,3,4]), ([]), ([1,2,3]);
SELECT arrayProduct(x) FROM test_aggregation;
DROP STREAM test_aggregation;

SELECT 'Types of aggregation result array product';
SELECT to_type_name(arrayProduct([to_int8(0)])), to_type_name(arrayProduct([to_int16(0)])), to_type_name(arrayProduct([to_int32(0)])), to_type_name(arrayProduct([to_int64(0)]));
SELECT to_type_name(arrayProduct([to_uint8(0)])), to_type_name(arrayProduct([to_uint16(0)])), to_type_name(arrayProduct([to_uint32(0)])), to_type_name(arrayProduct([to_uint64(0)]));
SELECT to_type_name(arrayProduct([to_int128(0)])), to_type_name(arrayProduct([toInt256(0)])), to_type_name(arrayProduct([toUInt256(0)]));
SELECT to_type_name(arrayProduct([to_float32(0)])), to_type_name(arrayProduct([to_float64(0)]));
SELECT to_type_name(arrayProduct([to_decimal32(0, 8)])), to_type_name(arrayProduct([to_decimal64(0, 8)])), to_type_name(arrayProduct([toDecimal128(0, 8)]));
