SELECT
    [1, 2, 3, 1, 3] AS a, 
    index_of(array_reverse(array_slice(a, 1, -1)), 3) AS offset_from_right, 
    array_slice(a, multi_if(offset_from_right = 0, 1, (length(a) - offset_from_right) + 1));
