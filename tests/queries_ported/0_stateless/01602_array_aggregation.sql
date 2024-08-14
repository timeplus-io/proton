SELECT 'Array min ', (array_min(array_cast(1,2,3,4,5,6)));
SELECT 'Array max ', (array_max(array_cast(1,2,3,4,5,6)));
SELECT 'Array sum ', (array_sum(array_cast(1,2,3,4,5,6)));
SELECT 'Array avg ', (array_avg(array_cast(1,2,3,4,5,6)));

DROP STREAM IF EXISTS test_aggregation;
CREATE STREAM test_aggregation (x array(int)) ENGINE=Memory;

INSERT INTO test_aggregation(x) VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array int min';
SELECT array_min(x) FROM test_aggregation;
SELECT 'Table array int max';
SELECT array_max(x) FROM test_aggregation;
SELECT 'Table array int sum';
SELECT array_sum(x) FROM test_aggregation;
SELECT 'Table array int avg';
SELECT array_avg(x) FROM test_aggregation;

DROP STREAM test_aggregation;

CREATE STREAM test_aggregation (x array(decimal64(8))) ENGINE=Memory;

INSERT INTO test_aggregation(x) VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array decimal min';
SELECT array_min(x) FROM test_aggregation;
SELECT 'Table array decimal max';
SELECT array_max(x) FROM test_aggregation;
SELECT 'Table array decimal sum';
SELECT array_sum(x) FROM test_aggregation;
SELECT 'Table array decimal avg';
SELECT array_avg(x) FROM test_aggregation;

DROP STREAM test_aggregation;

WITH ['2023-04-05 00:25:23', '2023-04-05 00:25:24']::array(datetime) AS dt SELECT array_max(dt), array_min(dt), array_difference(dt);
WITH ['2023-04-05 00:25:23.123', '2023-04-05 00:25:24.124']::array(datetime64(3)) AS dt SELECT array_max(dt), array_min(dt), array_difference(dt);
WITH ['2023-04-05', '2023-04-06']::array(date) AS d SELECT array_max(d), array_min(d), array_difference(d);
WITH ['2023-04-05', '2023-04-06']::array(date32) AS d SELECT array_max(d), array_min(d), array_difference(d);

SELECT 'Types of aggregation result array min';
SELECT to_type_name(array_min([to_int8(0)])), to_type_name(array_min([to_int16(0)])), to_type_name(array_min([to_int32(0)])), to_type_name(array_min([to_int64(0)]));
SELECT to_type_name(array_min([to_uint8(0)])), to_type_name(array_min([to_uint16(0)])), to_type_name(array_min([to_uint32(0)])), to_type_name(array_min([to_uint64(0)]));
SELECT to_type_name(array_min([to_int128(0)])), to_type_name(array_min([to_int256(0)])), to_type_name(array_min([to_uint256(0)]));
SELECT to_type_name(array_min([to_float32(0)])), to_type_name(array_min([to_float64(0)]));
SELECT to_type_name(array_min([to_decimal32(0, 8)])), to_type_name(array_min([to_decimal64(0, 8)])), to_type_name(array_min([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array max';
SELECT to_type_name(array_max([to_int8(0)])), to_type_name(array_max([to_int16(0)])), to_type_name(array_max([to_int32(0)])), to_type_name(array_max([to_int64(0)]));
SELECT to_type_name(array_max([to_uint8(0)])), to_type_name(array_max([to_uint16(0)])), to_type_name(array_max([to_uint32(0)])), to_type_name(array_max([to_uint64(0)]));
SELECT to_type_name(array_max([to_int128(0)])), to_type_name(array_max([to_int256(0)])), to_type_name(array_max([to_uint256(0)]));
SELECT to_type_name(array_max([to_float32(0)])), to_type_name(array_max([to_float64(0)]));
SELECT to_type_name(array_max([to_decimal32(0, 8)])), to_type_name(array_max([to_decimal64(0, 8)])), to_type_name(array_max([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array summ';
SELECT to_type_name(array_sum([to_int8(0)])), to_type_name(array_sum([to_int16(0)])), to_type_name(array_sum([to_int32(0)])), to_type_name(array_sum([to_int64(0)]));
SELECT to_type_name(array_sum([to_uint8(0)])), to_type_name(array_sum([to_uint16(0)])), to_type_name(array_sum([to_uint32(0)])), to_type_name(array_sum([to_uint64(0)]));
SELECT to_type_name(array_sum([to_int128(0)])), to_type_name(array_sum([to_int256(0)])), to_type_name(array_sum([to_uint256(0)]));
SELECT to_type_name(array_sum([to_float32(0)])), to_type_name(array_sum([to_float64(0)]));
SELECT to_type_name(array_sum([to_decimal32(0, 8)])), to_type_name(array_sum([to_decimal64(0, 8)])), to_type_name(array_sum([to_decimal128(0, 8)]));
SELECT 'Types of aggregation result array avg';
SELECT to_type_name(array_avg([to_int8(0)])), to_type_name(array_avg([to_int16(0)])), to_type_name(array_avg([to_int32(0)])), to_type_name(array_avg([to_int64(0)]));
SELECT to_type_name(array_avg([to_uint8(0)])), to_type_name(array_avg([to_uint16(0)])), to_type_name(array_avg([to_uint32(0)])), to_type_name(array_avg([to_uint64(0)]));
SELECT to_type_name(array_avg([to_int128(0)])), to_type_name(array_avg([to_int256(0)])), to_type_name(array_avg([to_uint256(0)]));
SELECT to_type_name(array_avg([to_float32(0)])), to_type_name(array_avg([to_float64(0)]));
SELECT to_type_name(array_avg([to_decimal32(0, 8)])), to_type_name(array_avg([to_decimal64(0, 8)])), to_type_name(array_avg([to_decimal128(0, 8)]));
