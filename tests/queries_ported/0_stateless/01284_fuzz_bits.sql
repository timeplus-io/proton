SELECT fuzz_bits(to_string('string'), 1); -- { serverError 43 }
SELECT fuzz_bits('string', -1.0); -- { serverError 69 }
SELECT fuzz_bits('', 0.3);
SELECT length(fuzz_bits(random_string(100), 0.5));
SELECT to_type_name(fuzz_bits(random_string(100), 0.5));
SELECT to_type_name(fuzz_bits(to_fixed_string('abacaba', 10), 0.9));

SELECT
  (
    0.29 * 8 * 10000 < sum
    AND sum < 0.31 * 8 * 10000
  ) AS res
FROM
  (
    SELECT
      array_sum(
        id -> bit_count(
          reinterpret_as_uint8(
            substring(
              fuzz_bits(
                array_string_concat(array_map(x -> to_string('\0'), range(10000))),
                0.3
              ),
              id + 1,
              1
            )
          )
        ),
        range(10000)
      ) as sum
  )
