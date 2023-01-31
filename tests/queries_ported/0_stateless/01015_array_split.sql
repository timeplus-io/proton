SELECT array_split((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]);
SELECT array_reverse_split((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]);

SELECT array_split(x -> 0, [1, 2, 3, 4, 5]);
SELECT array_reverse_split(x -> 0, [1, 2, 3, 4, 5]);
SELECT array_split(x -> 1, [1, 2, 3, 4, 5]);
SELECT array_reverse_split(x -> 1, [1, 2, 3, 4, 5]);
SELECT array_split(x -> x % 2 = 1, [1, 2, 3, 4, 5]);
SELECT array_reverse_split(x -> x % 2 = 1, [1, 2, 3, 4, 5]);

SELECT array_split(x -> 0, []);
SELECT array_reverse_split(x -> 0, []);
SELECT array_split(x -> 1, []);
SELECT array_reverse_split(x -> 1, []);
SELECT array_split(x -> x, empty_array_uint8());
SELECT array_reverse_split(x -> x, empty_array_uint8());

SELECT array_split(x -> x % 2 = 1, [1]);
SELECT array_reverse_split(x -> x % 2 = 1, [1]);
SELECT array_split(x -> x % 2 = 1, [2]);
SELECT array_reverse_split(x -> x % 2 = 1, [2]);
