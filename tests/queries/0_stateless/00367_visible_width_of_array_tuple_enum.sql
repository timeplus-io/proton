SELECT
CAST(['hello'] AS array(Enum8('hello' = 1))) AS x,
(1, CAST('hello' AS Enum8('hello' = 1))) AS y
FORMAT PrettyCompactNoEscapes;
