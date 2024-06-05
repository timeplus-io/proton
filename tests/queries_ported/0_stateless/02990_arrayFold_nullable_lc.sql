SET allow_suspicious_low_cardinality_types=1;

SELECT array_fold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], to_int64(3));
SELECT array_fold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], to_int64(to_nullable(3)));
SELECT array_fold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], materialize(to_int64(to_nullable(3))));

SELECT array_fold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4]::array(nullable(int64)), to_int64(to_nullable(3)));

SELECT array_fold((acc, x) -> (acc + (x * 2)), []::array(int64), to_int64(3));
SELECT array_fold((acc, x) -> (acc + (x * 2)), []::array(nullable(int64)), to_int64(to_nullable(3)));
SELECT array_fold((acc, x) -> (acc + (x * 2)), []::array(nullable(int64)), to_int64(NULL));

SELECT array_fold((acc, x) -> x, materialize(cast('[0, 1]', 'array(nullable(uint8))')), to_uint8(to_nullable(0)));
SELECT array_fold((acc, x) -> x, materialize(cast([NULL], 'array(nullable(uint8))')), to_uint8(to_nullable(0)));
SELECT array_fold((acc, x) -> acc + x, materialize(cast([NULL], 'array(nullable(uint8))')), to_uint64(to_nullable(0)));
SELECT array_fold((acc, x) -> acc + x, materialize(cast([1, 2, NULL], 'array(nullable(uint8))')), to_uint64(to_nullable(0)));

SELECT array_fold((acc, x) -> to_nullable(acc + (x * 2)), [1, 2, 3, 4], to_nullable(to_int64(3)));

-- SELECT array_fold((acc, x) -> to_low_cardinality(acc + (x * 2)), [1, 2, 3, 4], to_low_cardinality(to_int64(3)));

-- SELECT array_fold((acc, x) -> to_low_cardinality(acc + (x * 2)), [1, 2, 3, 4]::array(low_cardinality(int64)), to_int64(to_low_cardinality(3)));


-- SELECT array_fold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], NULL);
-- -- It's debatable which one of the following 2 queries should work, but considering the return type must match the
-- -- accumulator type it makes sense to be the second one
-- SELECT array_fold((acc, x) -> (acc + (x * 2))::low_cardinality(nullable(int64)), [1, 2, 3, 4], NULL::low_cardinality(nullable(int64)));
