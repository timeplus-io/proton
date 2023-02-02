-- { echo }
SELECT sum_with_overflow(a - 65537) FROM (SELECT cast(number AS decimal32(4)) as a FROM numbers(10));
