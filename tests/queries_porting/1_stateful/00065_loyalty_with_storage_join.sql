USE test;

DROP STREAM IF EXISTS join;
CREATE TABLE join (UserID uint64, loyalty int8) ENGINE = Join(SEMI, LEFT, UserID);

INSERT INTO join
SELECT
    UserID,
    to_int8(if((sum(SearchEngineID = 2) AS yandex) > (sum(SearchEngineID = 3) AS google),
    yandex / (yandex + google), 
    -google / (yandex + google)) * 10) AS loyalty
FROM hits
WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
GROUP BY UserID
HAVING (yandex + google) > 10;

SELECT
    loyalty,
    count()
FROM hits SEMI LEFT JOIN join USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;

DETACH TABLE join;
ATTACH TABLE join;

SELECT
    loyalty,
    count()
FROM hits SEMI LEFT JOIN join USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;

DROP STREAM join;
