SELECT array_map(x -> concat(x, concat(array_join([1]), x, NULL), ''), [1]);
SELECT array_map(x -> array_join([1]), [1, 2]);

SELECT
        array_join(array_map(x -> reinterpret_as_uint8(substring(random_string(range(random_string(1048577), NULL), array_join(array_map(x -> reinterpret_as_uint8(substring(random_string(range(NULL), 65537), 255)), range(1))), substring(random_string(NULL), x + 7), '257'), 1025)), range(7))) AS byte,
        count() AS c
    FROM numbers(10)
    GROUP BY
        array_map(x -> reinterpret_as_uint8(substring(random_string(random_string(range(random_string(255), NULL)), NULL), NULL)), range(3)),
        random_string(range(random_string(1048577), NULL), NULL),
        byte
    ORDER BY byte ASC;
