-- Tags: no-fasttest

SET output_format_write_statistics = 0;
SET extremes = 1;

SET output_format_json_quote_64bit_integers = 1;
SELECT to_int64(0) as i0, to_uint64(0) as u0, to_int64(9223372036854775807) as ip, to_int64(-9223372036854775808) as in, to_uint64(18446744073709551615) as up, [to_int64(0)] as arr, (to_uint64(0), to_uint64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSON;
SELECT to_int64(0) as i0, to_uint64(0) as u0, to_int64(9223372036854775807) as ip, to_int64(-9223372036854775808) as in, to_uint64(18446744073709551615) as up, [to_int64(0)] as arr, (to_uint64(0), to_uint64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSONCompact;
SELECT to_int64(0) as i0, to_uint64(0) as u0, to_int64(9223372036854775807) as ip, to_int64(-9223372036854775808) as in, to_uint64(18446744073709551615) as up, [to_int64(0)] as arr, (to_uint64(0), to_uint64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSONEachRow;

SET output_format_json_quote_64bit_integers = 0;
SELECT to_int64(0) as i0, to_uint64(0) as u0, to_int64(9223372036854775807) as ip, to_int64(-9223372036854775808) as in, to_uint64(18446744073709551615) as up, [to_int64(0)] as arr, (to_uint64(0), to_uint64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSON;
SELECT to_int64(0) as i0, to_uint64(0) as u0, to_int64(9223372036854775807) as ip, to_int64(-9223372036854775808) as in, to_uint64(18446744073709551615) as up, [to_int64(0)] as arr, (to_uint64(0), to_uint64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSONCompact;
SELECT to_int64(0) as i0, to_uint64(0) as u0, to_int64(9223372036854775807) as ip, to_int64(-9223372036854775808) as in, to_uint64(18446744073709551615) as up, [to_int64(0)] as arr, (to_uint64(0), to_uint64(0)) as tuple GROUP BY i0, u0, ip, in, up, arr, tuple WITH TOTALS FORMAT JSONEachRow;
