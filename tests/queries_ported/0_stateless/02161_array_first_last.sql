SELECT 'array_first constant predicate';
SELECT array_first(x -> 1, empty_array_uint8());
SELECT array_first(x -> 0, empty_array_uint8());
SELECT array_first(x -> 1, [1, 2, 3]);
SELECT array_first(x -> 0, [1, 2, 3]);

SELECT 'array_first non constant predicate';
SELECT array_first(x -> x >= 2, empty_array_uint8());
SELECT array_first(x -> x >= 2, [1, 2, 3]);
SELECT array_first(x -> x >= 2, materialize([1, 2, 3]));

SELECT 'array_last constant predicate';
SELECT array_last(x -> 1, empty_array_uint8());
SELECT array_last(x -> 0, empty_array_uint8());
SELECT array_last(x -> 1, [1, 2, 3]);
SELECT array_last(x -> 0, [1, 2, 3]);

SELECT 'array_last non constant predicate';
SELECT array_last(x -> x >= 2, empty_array_uint8());
SELECT array_last(x -> x >= 2, [1, 2, 3]);
SELECT array_last(x -> x >= 2, materialize([1, 2, 3]));
