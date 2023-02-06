SELECT 'arrayFirst constant predicate';
SELECT array_first_or_null(x -> 1, empty_array_uint8());
SELECT array_first_or_null(x -> 0, empty_array_uint8());
SELECT array_first_or_null(x -> 1, [1, 2, 3]);
SELECT array_first_or_null(x -> 0, [1, 2, 3]);

SELECT 'arrayFirst non constant predicate';
SELECT array_first_or_null(x -> x >= 2, empty_array_uint8());
SELECT array_first_or_null(x -> x >= 2, [1, 2, 3]);
SELECT array_first_or_null(x -> x >= 2, materialize([1, 2, 3]));

SELECT 'arrayFirst with Null';
SELECT array_first_or_null((x,f) -> f, [1,2,3,NULL], [0,1,0,0]);
SELECT array_first_or_null((x,f) -> f, [1,2,3,NULL], [0,0,0,1]);

SELECT 'arrayLast constant predicate';
SELECT array_last_or_null(x -> 1, empty_array_uint8());
SELECT array_last_or_null(x -> 0, empty_array_uint8());
SELECT array_last_or_null(x -> 1, [1, 2, 3]);
SELECT array_last_or_null(x -> 0, [1, 2, 3]);

SELECT 'arrayLast non constant predicate';
SELECT array_last_or_null(x -> x >= 2, empty_array_uint8());
SELECT array_last_or_null(x -> x >= 2, [1, 2, 3]);
SELECT array_last_or_null(x -> x >= 2, materialize([1, 2, 3]));

SELECT 'arrayLast with Null';
SELECT array_last_or_null((x,f) -> f, [1,2,3,NULL], [0,1,0,0]);
SELECT array_last_or_null((x,f) -> f, [1,2,3,NULL], [0,1,0,1]);