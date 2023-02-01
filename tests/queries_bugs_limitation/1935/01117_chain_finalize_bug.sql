SELECT array_join(array_map(i -> (i + 1), range(2))) AS index, number
FROM numbers(2)
GROUP BY number
ORDER BY index, number;

SET max_bytes_before_external_group_by = 1;

SELECT array_join(array_map(i -> (i + 1), range(2))) AS index, number
FROM numbers(2)
GROUP BY number
ORDER BY index, number;

SET group_by_two_level_threshold = 2;

SELECT count() FROM
(
    SELECT
        array_join(array_map(i -> (i + 1), range(2))) AS index, 
        number
    FROM numbers_mt(100000)
    GROUP BY number
    ORDER BY index ASC
    SETTINGS max_block_size = 100000, max_threads = 2
);
