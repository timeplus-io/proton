SELECT to_int32_or_zero('123a'), to_int32_or_zero('456');
SELECT to_int32_or_zero(array_join(['123a', '456']));

SELECT to_float64_or_zero('123.456a'), to_float64_or_zero('456.789');
SELECT to_float64_or_zero(array_join(['123.456a', '456.789']));
