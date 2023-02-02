SELECT array_fill(x -> (x < 10), []);
SELECT array_fill(x -> (x < 10), empty_array_uint8());
SELECT array_fill(x -> 1, []);
SELECT array_fill(x -> 0, []);
