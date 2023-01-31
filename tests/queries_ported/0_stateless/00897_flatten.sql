SELECT flatten(array_join([[[1, 2, 3], [4, 5]], [[6], [7, 8]]]));
SELECT array_flatten(array_join([[[[]], [[1], [], [2, 3]]], [[[4]]]]));
SELECT flatten(array_map(x -> array_map(x -> array_map(x -> range(x), range(x)), range(x)), range(number))) FROM numbers(6);
SELECT array_flatten([[[1, 2, 3], [4, 5]], [[6], [7, 8]]]);
SELECT flatten([[[]]]);
SELECT array_flatten([]);
