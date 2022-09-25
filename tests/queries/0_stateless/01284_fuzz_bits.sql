SELECT fuzzBits(to_string('string'), 1); -- { serverError 43 }
SELECT fuzzBits('string', -1.0); -- { serverError 69 }
SELECT fuzzBits('', 0.3);
SELECT length(fuzzBits(randomString(100), 0.5));
SELECT to_type_name(fuzzBits(randomString(100), 0.5));
SELECT to_type_name(fuzzBits(to_fixed_string('abacaba', 10), 0.9));

SELECT
  (
    0.29 * 8 * 10000 < sum
    AND sum < 0.31 * 8 * 10000
  ) AS res
FROM
  (
    SELECT
      array_sum(
        id -> bitCount(
          reinterpret_as_uint8(
            substring(
              fuzzBits(
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
