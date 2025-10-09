SET allow_experimental_analyzer = 1;

SELECT [2, 1, 3] AS arr, array_sort(arr), array_reverse_sort(arr), array_sort(x -> -x, arr);
SELECT materialize([2, 1, 3]) AS arr, array_sort(arr), array_reverse_sort(arr), array_sort(x -> -x, arr);

SELECT array_map(x -> to_string(x), [2, 1, 3]) AS arr, array_sort(arr), array_reverse_sort(arr), array_sort(x -> reverse(x), arr);
SELECT array_map(x -> to_string(x), materialize([2, 1, 3])) AS arr, array_sort(arr), array_reverse_sort(arr), array_sort(x -> reverse(x), arr);

SELECT array_map(x -> range(x), [2, 1, 3]) AS arr, array_sort(arr), array_reverse_sort(arr), array_sort(x -> -length(x), arr);
SELECT array_map(x -> range(x), materialize([2, 1, 3])) AS arr, array_sort(arr), array_reverse_sort(arr), array_sort(x -> -length(x), arr);

SELECT split_by_char('0', to_string(int_hash64(number))) AS arr, array_sort(arr) AS sorted, array_sort(x -> to_int64_or_zero(x), arr) AS sorted_nums FROM system.numbers LIMIT 10;

SELECT array_reverse_sort(number % 2 ? empty_array_uInt64() : range(number)) FROM system.numbers LIMIT 10;

SELECT array_sort((x, y) -> y, ['hello', 'world'], [2, 1]);

-- Using arrayResize to trim the unsorted bit of the array that is normally left in unspecified order
SELECT [9,4,8,10,5,2,3,7,1,6] AS arr, 4 AS lim, array_resize(array_partial_reverse_sort(lim, arr), lim), array_resize(array_prartial_sort(x -> -x, lim, arr), lim);
SELECT materialize([9,4,8,10,5,2,3,7,1,6]) AS arr, 4 AS lim, array_resize(array_partial_reverse_sort(lim, arr), lim), array_resize(array_prartial_sort(x -> -x, lim, arr), lim);

SELECT array_map(x -> to_string(x), [9,4,8,10,5,2,3,7,1,6]) AS arr, 4 AS lim, array_resize(array_partial_reverse_sort(lim, arr), lim), array_resize(array_prartial_sort(x -> reverse(x), lim, arr), lim);
SELECT array_map(x -> to_string(x), materialize([9,4,8,10,5,2,3,7,1,6])) AS arr, 4 AS lim, array_resize(array_partial_reverse_sort(lim, arr), lim), array_resize(array_prartial_sort(x -> reverse(x), lim, arr), lim);

SELECT array_map(x -> range(x), [4,1,2,3]) AS arr, 2 AS lim, array_resize(array_prartial_sort(lim, arr), lim), array_resize(array_partial_reverse_sort(lim, arr), lim), array_resize(array_prartial_sort(x -> -length(x), lim, arr), lim);
SELECT array_map(x -> range(x), materialize([4,1,2,3])) AS arr, 2 AS lim, array_resize(array_prartial_sort(lim, arr), lim), array_resize(array_partial_reverse_sort(lim, arr), lim), array_resize(array_prartial_sort(x -> -length(x), lim, arr), lim);

SELECT split_by_char('0', to_string(intHash64(number))) AS arr, 3 AS lim, array_resize(array_prartial_sort(lim, arr), lim) AS sorted, array_resize(array_prartial_sort(x -> to_int64_or_zero(x), lim, arr), lim) AS sorted_nums FROM system.numbers LIMIT 10;

SELECT res FROM (SELECT array_partial_reverse_sort(2, number % 2 ? empty_array_uInt64() : range(number)) AS arr, array_resize(arr, if(empty(arr), 0, 2)) AS res FROM system.numbers LIMIT 10);

SELECT array_resize(array_prartial_sort((x, y) -> y, 3, ['directly','ignore','wrongly','relocate','upright'], [4,2,1,3,5]), 3);

SELECT 2 as nelems, array_resize(array_prartial_sort(nelems, [NULL,9,4,8,10,5,2,3,7,1,6]), nelems);
SELECT 2 as nelems, array_resize(array_prartial_sort(nelems, [NULL,'9','4','8','10','5','2','3','7','1','6']), nelems);
SELECT 2 as nelems, array_resize(array_partial_reverse_sort(nelems, [NULL,9,4,8,10,5,2,3,7,1,6]), nelems);
SELECT 2 as nelems, array_resize(array_partial_reverse_sort(nelems, [NULL,'9','4','8','10','5','2','3','7','1','6']), nelems);


SELECT 4 as nelems, array_resize(array_prartial_sort(nelems, [1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]), nelems);
SELECT 4 as nelems, array_resize(array_prartial_sort((x) -> -x, nelems, [1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]), nelems);
SELECT 10 as nelems, array_resize(array_prartial_sort(nelems, [1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]), nelems);
SELECT 10 as nelems, array_resize(array_prartial_sort(nelems, [1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]), nelems);

SELECT 3 as nelems, [[1,2],[-10,-20],[10,20],[0,0],[-1.5,1]] as arr, array_resize(array_prartial_sort(nelems, arr), nelems), array_resize(array_partial_reverse_sort(nelems, arr), nelems), array_resize(array_prartial_sort((x) -> arraySum(x), nelems, arr), nelems);

SELECT 0 as nelems, [NULL,9,4,8,10,5,2,3,7,1,6] AS arr, array_prartial_sort(nelems, arr), array_partial_reverse_sort(nelems, arr), array_prartial_sort((x) -> -x, nelems, arr);
SELECT 10 as nelems, [NULL,9,4,8,10,5,2,3,7,1,6] AS arr, array_prartial_sort(nelems, arr), array_partial_reverse_sort(nelems, arr), array_prartial_sort((x) -> -x, nelems, arr);


SELECT array_prartial_sort([1,2,3]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT array_prartial_sort(2, [1,2,3], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT array_prartial_sort(2, [1,2,3], 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT array_prartial_sort(array_sort([1,2,3]), [1,2,3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT array_map(x -> range(x), [4, 1, 2, 3]) AS arr, 100 AS lim, array_resize(array_prartial_sort(array_prartial_sort(lim, arr), arr), lim), array_resize(array_partial_reverse_sort(lim, arr), lim), array_resize(array_prartial_sort(x -> (-length(x)), lim, arr), lim); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT array_partial_reverse_sort(array_sort((x, y) -> y, [NULL, NULL], [NULL, NULL]), arr), array_map(x -> to_string(x), [257, -9223372036854775807, 2, -2147483648, 2147483648, NULL, 65536, -2147483648, 2, 65535]) AS arr, NULL, 100 AS lim, 65536, array_resize(array_prartial_sort(x -> reverse(x), lim, arr), lim) GROUP BY [NULL, 1023, -2, NULL, 255, '0', NULL, 9223372036854775806] WITH ROLLUP; -- { serverError NO_COMMON_TYPE }