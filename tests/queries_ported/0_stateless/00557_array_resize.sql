select array_resize([1, 2, 3], 10);
select array_resize([1, 2, 3], -10);
select array_resize([1, Null, 3], 10);
select array_resize([1, Null, 3], -10);
select array_resize([1, 2, 3, 4, 5, 6], 3);
select array_resize([1, 2, 3, 4, 5, 6], -3);
select array_resize([1, 2, 3], 5, 42);
select array_resize([1, 2, 3], -5, 42);
select array_resize(['a', 'b', 'c'], 5);
select array_resize([[1, 2], [3, 4]], 4);
select array_resize([[1, 2], [3, 4]], -4);
select array_resize([[1, 2], [3, 4]], 4, [5, 6]);
select array_resize([[1, 2], [3, 4]], -4, [5, 6]);

-- different types of array elements and default value to fill
select array_resize([1, 2, 3], 5, 423.56);
