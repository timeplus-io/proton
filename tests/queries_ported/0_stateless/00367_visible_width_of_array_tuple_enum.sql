SELECT
CAST(['hello'] AS array(enum8('hello' = 1))) AS x,
(1, CAST('hello' AS enum8('hello' = 1))) AS y
FORMAT PrettyCompactNoEscapes;
