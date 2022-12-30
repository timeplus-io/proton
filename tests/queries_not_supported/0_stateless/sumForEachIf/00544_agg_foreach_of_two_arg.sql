SELECT sum_for_each(arr), sumForEachIf(arr, arr[1] = 1), sum_ifForEach(arr, array_map(x -> x != 5, arr)) FROM (SELECT array_join([[1, 2, 3], [4, 5, 6]]) AS arr);
