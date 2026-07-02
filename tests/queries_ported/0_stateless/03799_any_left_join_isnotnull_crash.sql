SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS AddedToCart;
DROP STREAM IF EXISTS Session;

CREATE STREAM Session
(
    id string,
    site enum8('STORE_A' = 1, 'STORE_B' = 2),
    device enum8('DESKTOP' = 1, 'MOBILE' = 2)
)
ENGINE = MergeTree
ORDER BY id;

CREATE STREAM AddedToCart
(
    sessionId string,
    `order` int32,
    top nullable(int32),
    screenHeight nullable(int32),
    screenWidth nullable(int32),
    isPromotion uint8,
    date datetime64(3)
)
ENGINE = MergeTree
ORDER BY (sessionId, date);

INSERT INTO Session (id, site, device) VALUES ('s1', 'STORE_A', 'DESKTOP');
INSERT INTO Session (id, site, device) VALUES ('s2', 'STORE_B', 'MOBILE');

INSERT INTO AddedToCart (sessionId, `order`, top, screenHeight, screenWidth, isPromotion, date)
VALUES ('s1', 1, 100, 400, 1024, 1, parse_datetime64_best_effort('2026-01-19T12:00:00.000Z', 3));
INSERT INTO AddedToCart (sessionId, `order`, top, screenHeight, screenWidth, isPromotion, date)
VALUES ('s2', 2, 100, 400, 1024, 1, parse_datetime64_best_effort('2026-01-19T12:00:01.000Z', 3));

SELECT
    s.site AS site,
    if((a.`order` IS NULL) OR (a.`order` <= 0) OR (a.`order` > 30), NULL, accurate_cast_or_null(a.`order`, 'int32')) AS page_level,
    count() AS count
FROM AddedToCart AS a
ANY LEFT JOIN Session AS s ON a.sessionId = s.id
WHERE (a.top IS NOT NULL)
  AND (a.screenHeight IS NOT NULL)
  AND (a.screenHeight > 0)
  AND (a.isPromotion = _cast(1, 'uint8'))
  AND (s.device = 'DESKTOP')
  AND is_not_null(s.site)
GROUP BY site, page_level
ORDER BY site ASC, page_level ASC
FORMAT JSONEachRow;

DROP STREAM AddedToCart;
DROP STREAM Session;
