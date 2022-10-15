SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;

SELECT
    loyalty, 
    count()
from table(test.hits) ANY LEFT JOIN 
(
    SELECT
        UserID, 
        sum(to_uint8(SearchEngineID = 2)) AS yandex, 
        sum(to_uint8(SearchEngineID = 3)) AS google, 
        to_int8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    from table(test.hits)
    WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
    GROUP BY UserID
    HAVING (yandex + google) > 10
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT
    loyalty, 
    count()
FROM
(
    SELECT UserID
    from table(test.hits)
) ANY LEFT JOIN 
(
    SELECT
        UserID, 
        sum(to_uint8(SearchEngineID = 2)) AS yandex, 
        sum(to_uint8(SearchEngineID = 3)) AS google, 
        to_int8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    from table(test.hits)
    WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
    GROUP BY UserID
    HAVING (yandex + google) > 10
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT
    loyalty, 
    count()
FROM
(
    SELECT
        loyalty, 
        UserID
    FROM
    (
        SELECT UserID
        from table(test.hits)
    ) ANY LEFT JOIN 
    (
        SELECT
            UserID, 
            sum(to_uint8(SearchEngineID = 2)) AS yandex, 
            sum(to_uint8(SearchEngineID = 3)) AS google, 
            to_int8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
        from table(test.hits)
        WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
        GROUP BY UserID
        HAVING (yandex + google) > 10
    ) USING UserID
)
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT
    loyalty, 
    count() AS c, 
    log(c + 1) * 1000
from table(test.hits) ANY INNER JOIN 
(
    SELECT
        UserID, 
        to_int8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    FROM
    (
        SELECT
            UserID, 
            sum(to_uint8(SearchEngineID = 2)) AS yandex, 
            sum(to_uint8(SearchEngineID = 3)) AS google
        from table(test.hits)
        WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
        GROUP BY UserID
        HAVING (yandex + google) > 10
    )
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;
