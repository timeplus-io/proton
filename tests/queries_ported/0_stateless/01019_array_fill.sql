SELECT array_fill(x -> 0, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT array_reverse_fill(x -> 0, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT array_fill(x -> 1, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT array_reverse_fill(x -> 1, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);

SELECT array_fill(x -> x < 10, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT array_reverse_fill(x -> x < 10, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT array_fill(x -> not is_null(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]);
SELECT array_reverse_fill(x -> not is_null(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]);
SELECT array_fill((x, y) -> y, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16], [0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0]);
SELECT array_reverse_fill((x, y) -> y, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16], [0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0]);
