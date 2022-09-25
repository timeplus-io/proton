SELECT *
FROM
(
    WITH

        (
            SELECT group_array(a)
            FROM
            (
                SELECT 1 AS a
            )
        ) AS keys,

        (
            SELECT group_array(a)
            FROM
            (
                SELECT 2 AS a
            )
        ) AS values
    SELECT *
    FROM
    (
        SELECT 1 AS a
    )
    WHERE transform(a, keys, values, 0)
) AS wrap;
