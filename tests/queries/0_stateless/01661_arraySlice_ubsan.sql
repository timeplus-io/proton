-- { echo }
-- tests with INT64_MIN (UBsan)
select array_slice([], -9223372036854775808);
