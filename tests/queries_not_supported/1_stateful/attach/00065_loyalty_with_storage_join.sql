USE test;

DROP STREAM IF EXISTS join;
CREATE STREAM join (UserID uint64, loyalty int8) ENGINE = Join(SEMI, LEFT, UserID);

INSERT INTO join
SELECT
    UserID,
    to_int8(if((sum(to_uint8(SearchEngineID = 2)) AS yandex) > (sum(to_uint8(SearchEngineID = 3)) AS google),
    yandex / (yandex + google), 
    -google / (yandex + google)) * 10) AS loyalty
FROM table(hits)
WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
GROUP BY UserID
HAVING (yandex + google) > 10;

SELECT
    loyalty,
    count()
FROM table(hits) SEMI LEFT JOIN join USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;

DETACH stream join;
ATTACH stream join;

SELECT
    loyalty,
    count()
FROM table(hits) SEMI LEFT JOIN join USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;

DROP STREAM join;
