SELECT array_sort(array_intersect(['a', 'b', 'c'], ['a', 'a']));
SELECT array_sort(array_intersect([1, 1], [2, 2]));
SELECT array_sort(array_intersect([1, 1], [1, 2]));
SELECT array_sort(array_intersect([1, 1, 1], [3], [2, 2, 2]));
SELECT array_sort(array_intersect([1, 2], [1, 2], [2]));
SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [1]));
SELECT array_sort(array_intersect([]));
SELECT array_sort(array_intersect([1, 2, 3]));
SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [2, 2, 2]));
