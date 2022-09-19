SELECT arrayFill(x -> (x < 10), []);
SELECT arrayFill(x -> (x < 10), empty_array_uint8());
SELECT arrayFill(x -> 1, []);
SELECT arrayFill(x -> 0, []);
