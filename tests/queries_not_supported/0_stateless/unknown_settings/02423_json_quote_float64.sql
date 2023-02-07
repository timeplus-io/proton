select 42424.4242424242::float64 as x, [42.42::float64, 42.42::float64] as arr, tuple(42.42::float64) as tuple format JSONEachRow settings output_format_json_quote_64bit_floats=1;
select 42424.4242424242::float64 as x, [42.42::float64, 42.42::float64] as arr, tuple(42.42::float64) as tuple format JSONEachRow settings output_format_json_quote_64bit_floats=0;

