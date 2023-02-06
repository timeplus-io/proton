SELECT 'array_first_index constant predicate';
SELECT array_first_index(x -> 1, empty_array_uint8());
SELECT array_first_index(x -> 0, empty_array_uint8());
SELECT array_first_index(x -> 1, [1, 2, 3]);
SELECT array_first_index(x -> 0, [1, 2, 3]);

SELECT 'array_first_index non constant predicate';
SELECT array_first_index(x -> x >= 2, empty_array_uint8());
SELECT array_first_index(x -> x >= 2, [1, 2, 3]);
SELECT array_first_index(x -> x >= 2, [1, 2, 3]);

SELECT 'array_last_index constant predicate';
SELECT array_last_index(x -> 1, empty_array_uint8());
SELECT array_last_index(x -> 0, empty_array_uint8());
SELECT array_last_index(x -> 1, [1, 2, 3]);
SELECT array_last_index(x -> 0, materialize([1, 2, 3]));

SELECT 'array_last_index non constant predicate';
SELECT array_last_index(x -> x >= 2, empty_array_uint8());
SELECT array_last_index(x -> x >= 2, [1, 2, 3]);
SELECT array_last_index(x -> x >= 2, materialize([1, 2, 3]));
