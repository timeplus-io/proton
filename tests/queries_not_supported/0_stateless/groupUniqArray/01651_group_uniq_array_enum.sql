SELECT arraySort(groupUniqArray(x)) FROM (SELECT CAST(array_join([1, 2, 3, 2, 3, 3]) AS Enum('Hello' = 1, 'World' = 2, 'Упячка' = 3)) AS x);
SELECT arraySort(group_array(x)) FROM (SELECT CAST(array_join([1, 2, 3, 2, 3, 3]) AS Enum('Hello' = 1, 'World' = 2, 'Упячка' = 3)) AS x);

SELECT
    arraySort(groupUniqArray(val)) AS uniq,
    to_type_name(uniq),
    arraySort(group_array(val)) AS arr,
    to_type_name(arr)
FROM
(
    SELECT CAST(number % 2, 'Enum(\'hello\' = 1, \'world\' = 0)') AS val
    FROM numbers(2)
);
